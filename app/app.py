import re

import gitlab
import urllib3
from flask import Flask, request
from iib_update import run_in_process
from utils import data_from_config, trigger_openshift_ci_job


urllib3.disable_warnings()

app = Flask("webhook_server")


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


def dict_to_str(_dict):
    dict_str = ""
    for key, value in _dict.items():
        dict_str += f"{key}: {value}\n\t\t"
    return dict_str


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
    run_in_process(logger=app.logger)
    app.logger.info("Starting openshift-ci-trigger app")
    app.run(port=5000, host="0.0.0.0", use_reloader=False)


if __name__ == "__main__":
    main()
