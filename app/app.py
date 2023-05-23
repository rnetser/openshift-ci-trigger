import json
import os
import re
from json import JSONDecodeError

import gitlab
import requests
import urllib3
import yaml
from flask import Flask, request
from git import Repo


urllib3.disable_warnings()

app = Flask("webhook_server")


# TODO: Fill all addons and jobs mapping
# TODO: Remove all but dbaas-operator once we have a successful trigger
PRODUCTS_AND_JOBS_MAPPING = {
    "dbaas-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "ocs-provider": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "acs-fleetshard": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "cert-manager-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "isv-managed-starburst-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "ocs-consumer": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
    "connectors-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-addon-v4.12-poc-tests",
}


class RepositoryNotFoundError(Exception):
    pass


def dict_to_str(_dict):
    dict_str = ""
    for key, value in _dict.items():
        dict_str += f"{key}: {value}\n\t\t"
    return dict_str


def data_from_config():
    config_file = os.environ.get("OPENSHIFT_CI_TRIGGER_CONFIG", "/config/config.yaml")
    with open(config_file) as fd:
        return yaml.safe_load(fd)


def repo_data_from_config(repository_name):
    config = data_from_config()
    data = config["repositories"].get(repository_name)
    if not data:
        raise RepositoryNotFoundError(
            f"Repository {repository_name} not found in config file"
        )

    return data


def get_api(url, token):
    gitlab_api = gitlab.Gitlab(url=url, private_token=token, ssl_verify=False)
    gitlab_api.auth()
    return gitlab_api


def process_hook(api, data, slack_webhook_url):
    object_attributes = data["object_attributes"]
    if object_attributes.get("action") == "merge":
        project = api.projects.get(data["project"]["id"])
        merge_request = project.mergerequests.get(object_attributes["iid"])
        for change in merge_request.changes().get("changes", []):
            changed_file = change.get("new_path")
            # TODO: Get addon version from changed_file and send it to slack
            matches = re.match(
                r"addons/(?P<addon>.*)/addonimagesets/(production|stage)/.*.yaml",
                changed_file,
            )
            if matches:
                addon = matches.group("addon")
                job = PRODUCTS_AND_JOBS_MAPPING.get(addon)
                if job:
                    trigger_openshift_ci_job(
                        job=job, addon=addon, slack_webhook_url=slack_webhook_url
                    )
                else:
                    app.logger.info(f"No job found for addon: {addon}")


def trigger_openshift_ci_job(job, addon, slack_webhook_url):
    app.logger.info(f"Triggering openshift-ci job: {job}")
    config_data = data_from_config()
    res = requests.post(
        url=f"{config_data['trigger_url']}/{job}",
        headers={"Authorization": f"Bearer {config_data['trigger_token']}"},
        data='{"job_execution_type": "1"}',
    )
    res_dict = json.loads(res.text)
    if (not res.ok) or res_dict["job_status"] != "TRIGGERED":
        app.logger.error(
            f"Failed to trigger openshift-ci job: {job} for addon {addon}, response: {res_dict}"
        )

    message = f"""
    ```
    openshift-ci: New addon {addon} was merged.
    triggering job {job}
    response:
        {dict_to_str(_dict=res_dict)}
    ```
    """
    send_slack_message(
        message=message,
        webhook_url=slack_webhook_url,
    )
    return res_dict


def send_slack_message(message, webhook_url):
    slack_data = {"text": message}
    app.logger.info(f"Sending message to slack: {message}")
    response = requests.post(
        webhook_url,
        data=json.dumps(slack_data),
        headers={"Content-Type": "application/json"},
    )
    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code} with the following message: {response.text}"
        )


@app.route("/process", methods=["POST"])
def process():
    try:
        hook_data = request.json
        event_type = hook_data["event_type"]
        repository_name = hook_data["repository"]["name"]
        app.logger.info(f"{repository_name}: Event type: {event_type}")
        repository_data = repo_data_from_config(repository_name=repository_name)
        slack_webhook_url = repository_data["slack_webhook_url"]
        api = get_api(
            url=repository_data["gitlab_url"], token=repository_data["gitlab_token"]
        )
        process_hook(api=api, data=hook_data, slack_webhook_url=slack_webhook_url)
        return "Process done"
    except Exception as ex:
        app.logger.error(f"Failed to process hook: {ex}")
        return "Process failed"


def get_new_iib():
    operators_data_file = "operators-latest-iib.json"
    with open(operators_data_file, "a+") as fd:
        try:
            data = json.load(fd)
        except JSONDecodeError:
            data = {}

    for operator_name in ["rhods"]:
        res = requests.get(
            "https://datagrepper.engineering.redhat.com/raw?"
            "topic=/topic/VirtualTopic.eng.ci.redhat-container-image.index.built&"
            f"contains={operator_name}",
            verify=False,
        )

        for raw_msg in res.json()["raw_messages"]:
            iib_data = raw_msg["msg"]["index"]
            ocp_version = iib_data["ocp_version"]
            operator_data = data.get(operator_name)
            iib_number = iib_data["index_image"].split("iib:")[-1]
            if operator_data:
                version_data = operator_data.get(ocp_version)
                if version_data == ocp_version:
                    if version_data["iib"] < iib_number:
                        version_data["iib"] = iib_number
                else:
                    operator_data[ocp_version] = {"iib": iib_number}

            else:
                data[operator_name] = {ocp_version: {"iib": iib_number}}

    with open(operators_data_file, "w") as fd:
        fd.write(json.dumps(data))

    config_data = data_from_config()
    token = config_data["github_token"]
    git_repo = Repo(".")
    if operators_data_file in git_repo.git.status():
        git_repo.git.add(operators_data_file)
        git_repo.git.commit("-m", f"Auto update {operators_data_file}", "--no-verify")
        git_repo.git.push(
            f"https://{token}@github.com/RedHatQE/openshift-ci-trigger.git",
            "origin",
            "main",
        )


def main():
    app.logger.info("Starting openshift-ci-trigger app")
    app.run(port=5000, host="0.0.0.0", use_reloader=False)


if __name__ == "__main__":
    get_new_iib()
    main()
