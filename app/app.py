import json
import os
import re
from json import JSONDecodeError
from multiprocessing import Process
from time import sleep

import gitlab
import requests
import urllib3
import yaml
from flask import Flask, request
from git import Repo


urllib3.disable_warnings()

app = Flask("webhook_server")


OPERATORS_DATA_FILE = "operators-latest-iib.json"
OPERATORS_AND_JOBS_MAPPING = {
    "rhods": {"v4.13": "periodic-ci-CSPI-QE-MSI-rhods-operator-v4.13-rhods-tests"}
}

# TODO: Fill all addons and jobs mapping
# TODO: Remove all but dbaas-operator once we have a successful trigger
PRODUCTS_AND_JOBS_MAPPING = {
    "dbaas-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "ocs-provider": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "acs-fleetshard": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "cert-manager-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "isv-managed-starburst-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "ocs-consumer": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
    "connectors-operator": "periodic-ci-CSPI-QE-MSI-rdbaas-operator-product-v4.12-poc-tests",
}


class RepositoryNotFoundError(Exception):
    pass


def read_data_file():
    with open(OPERATORS_DATA_FILE, "r") as fd:
        try:
            return json.load(fd)
        except JSONDecodeError:
            return {}


def get_operator_data_from_url(datagrepper_config_data, operator_name):
    app.logger.info(f"Getting IIB data for {operator_name}")
    res = requests.get(
        f"{datagrepper_config_data['datagrepper_query_url']}{operator_name}",
        verify=False,
    )
    return res.json()


def get_new_iib(operator_config_data):
    data = read_data_file()
    trigger_dict = {}
    for operator_name in ["rhods"]:
        trigger_dict[operator_name] = {}
        app.logger.info(f"Parsing new IIB data for {operator_name}")
        res = get_operator_data_from_url(
            datagrepper_config_data=operator_config_data,
            operator_name=operator_name,
        )
        for raw_msg in res["raw_messages"]:
            iib_data = raw_msg["msg"]["index"]
            ocp_version = iib_data["ocp_version"]
            iib_number = iib_data["index_image"].split("iib:")[-1]

            trigger_dict[operator_name][ocp_version] = False
            operator_data_from_file = data.get(operator_name)
            if operator_data_from_file:
                iib_by_ocp_version = operator_data_from_file.get(ocp_version)
                if iib_by_ocp_version.get("iib"):
                    if iib_by_ocp_version["iib"] < iib_number:
                        iib_by_ocp_version["iib"] = iib_number
                        trigger_dict[operator_name][ocp_version] = True
                    else:
                        continue
                else:
                    operator_data_from_file[ocp_version] = {"iib": iib_number}
                    trigger_dict[operator_name][ocp_version] = True

            else:
                data[operator_name] = {ocp_version: {"iib": iib_number}}
                trigger_dict[operator_name][ocp_version] = True

    with open(OPERATORS_DATA_FILE, "w") as fd:
        fd.write(json.dumps(data))

    return trigger_dict


def push_changes(git_config_data):
    token = git_config_data["github_token"]
    git_repo = Repo(".")
    app.logger.info(f"Check if {OPERATORS_DATA_FILE} was changed")
    if OPERATORS_DATA_FILE in git_repo.git.status():
        git_repo.git.add(OPERATORS_DATA_FILE)
        git_repo.git.commit("-m", f"Auto update {OPERATORS_DATA_FILE}", "--no-verify")
        app.logger.info(f"Push new changes for {OPERATORS_DATA_FILE}")
        git_repo.git.push(
            f"https://{token}@github.com/RedHatQE/openshift-ci-trigger.git"
        )


def data_from_config():
    config_file = os.environ.get("OPENSHIFT_CI_TRIGGER_CONFIG", "/config/config.yaml")
    with open(config_file) as fd:
        return yaml.safe_load(fd)


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


def trigger_openshift_ci_job(job, product, slack_webhook_url):
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
            f"Failed to trigger openshift-ci job: {job} for addon {product}, response: {res_dict}"
        )

    message = f"""
    ```
    openshift-ci: New product {product} was merged/updated.
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


def dict_to_str(_dict):
    dict_str = ""
    for key, value in _dict.items():
        dict_str += f"{key}: {value}\n\t\t"
    return dict_str


def run_in_process():
    proc = Process(target=run_iib_update)
    proc.start()


def run_iib_update():
    while True:
        try:
            config_data = data_from_config()
            slack_webhook_url = config_data["slack_webhook_url"]
            trigger_dict = get_new_iib(operator_config_data=config_data)
            push_changes(git_config_data=config_data)
            for _operator, _version in trigger_dict.items():
                for _ocp_version, _trigger in _version.items():
                    if _trigger:
                        job = OPERATORS_AND_JOBS_MAPPING[_operator][_version]
                        trigger_openshift_ci_job(
                            job=job,
                            product=_operator,
                            slack_webhook_url=slack_webhook_url,
                        )

            sleep(60 * 5)
        except Exception as ex:
            app.logger.error(ex)
            continue


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
            # TODO: Get product version from changed_file and send it to slack
            matches = re.match(
                r"addons/(?P<product>.*)/addonimagesets/(production|stage)/.*.yaml",
                changed_file,
            )
            if matches:
                addon = matches.group("product")
                job = PRODUCTS_AND_JOBS_MAPPING.get(addon)
                if job:
                    trigger_openshift_ci_job(
                        job=job, product=addon, slack_webhook_url=slack_webhook_url
                    )
                else:
                    app.logger.info(f"No job found for product: {addon}")


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


def main():
    # run_in_process()
    run_iib_update()
    app.logger.info("Starting openshift-ci-trigger app")
    app.run(port=5000, host="0.0.0.0", use_reloader=False)


if __name__ == "__main__":
    main()
