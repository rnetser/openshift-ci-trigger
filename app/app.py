import json
import os
import re
import shutil
from contextlib import contextmanager
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

app = Flask("openshift-ci-trigger")

LOCAL_REPO_PATH = "/tmp/openshift-ci-trigger"
OPERATORS_DATA_FILE_NAME = "operators-latest-iib.json"
OPERATORS_DATA_FILE = os.path.join(
    "/tmp/openshift-ci-trigger", OPERATORS_DATA_FILE_NAME
)


class RepositoryNotFoundError(Exception):
    pass


def operators_triggered_for_slack(job_dict):
    res = ""
    for vals in job_dict.values():
        for operator, data in vals.items():
            if data.get("triggered"):
                res += f"{operator}: {data.get('iib')}\n\t"

    return res


def extract_key_from_dict(key, _dict):
    if isinstance(_dict, dict):
        for _key, _val in _dict.items():
            if _key == key:
                yield _val
            if isinstance(_val, dict):
                for result in extract_key_from_dict(key, _val):
                    yield result
            elif isinstance(_val, list):
                for _item in _val:
                    for result in extract_key_from_dict(key, _item):
                        yield result


@contextmanager
def change_directory(directory, logger):
    logger.info(f"Changing directory to {directory}")
    old_cwd = os.getcwd()
    yield os.chdir(directory)
    logger.info(f"Changing back to directory {old_cwd}")
    os.chdir(old_cwd)


def read_data_file():
    with open(OPERATORS_DATA_FILE, "r") as fd:
        try:
            return json.load(fd)
        except JSONDecodeError:
            return {}


def get_operator_data_from_url(datagrepper_config_data, operator_name, ocp_version):
    app.logger.info(f"Getting IIB data for {operator_name}")
    res = requests.get(
        f"{datagrepper_config_data['datagrepper_query_url']}&contains={operator_name}",
        verify=False,
    )
    app.logger.info(f"Done getting IIB data for {operator_name}")
    json_res = res.json()
    for raw_msg in json_res["raw_messages"]:
        _index = raw_msg["msg"]["index"]
        if _index["ocp_version"] == ocp_version:
            yield _index


def get_new_iib(operator_config_data):
    new_trigger_data = False
    data_from_file = read_data_file()
    openshift_ci_jobs = operator_config_data.get("openshift_ci_jobs", {})

    for _ocp_version, _jobs_data in openshift_ci_jobs.items():
        for openshift_ci_job_name in [*_jobs_data]:
            job_data = openshift_ci_jobs[_ocp_version][openshift_ci_job_name]
            for _operator, _operator_name in job_data.items():
                data_from_file.setdefault(_ocp_version, {}).setdefault(
                    openshift_ci_job_name, {}
                ).setdefault(_operator_name, {})
                _operator_data = data_from_file[_ocp_version][openshift_ci_job_name][
                    _operator_name
                ]
                _operator_data["triggered"] = False
                app.logger.info(f"Parsing new IIB data for {_operator_name}")
                for iib_data in get_operator_data_from_url(
                    datagrepper_config_data=operator_config_data,
                    operator_name=_operator,
                    ocp_version=_ocp_version,
                ):
                    index_image = iib_data["index_image"]

                    iib_data_from_file = _operator_data.get("iib")
                    if iib_data_from_file:
                        iib_from_url = iib_data["index_image"].split("iib:")[-1]
                        iib_from_file = iib_data_from_file.split("iib:")[-1]
                        if iib_from_file < iib_from_url:
                            _operator_data["iib"] = index_image
                            _operator_data["triggered"] = True
                            new_trigger_data = True

                    else:
                        _operator_data["iib"] = index_image
                        _operator_data["triggered"] = True
                        new_trigger_data = True

        app.logger.info(f"Done parsing new IIB data for {_jobs_data}")

    if new_trigger_data:
        app.logger.info(f"New IIB data found: {data_from_file}")
        with open(OPERATORS_DATA_FILE, "w") as fd:
            fd.write(json.dumps(data_from_file))

    return data_from_file


def clone_repo(repo_url):
    shutil.rmtree(path=LOCAL_REPO_PATH, ignore_errors=True)
    Repo.clone_from(url=repo_url, to_path=LOCAL_REPO_PATH)


def push_changes(repo_url, slack_webhook_url):
    app.logger.info(f"Check if {OPERATORS_DATA_FILE} was changed")
    with change_directory(directory=LOCAL_REPO_PATH, logger=app.logger):
        try:
            _git_repo = Repo(LOCAL_REPO_PATH)
            _git_repo.git.config("user.email", f"{app.name}@local")
            _git_repo.git.config("user.name", app.name)
            os.system("pre-commit install")

            if OPERATORS_DATA_FILE_NAME in _git_repo.git.status():
                app.logger.info(
                    f"Found changes for {OPERATORS_DATA_FILE_NAME}, pushing new changes"
                )
                app.logger.info(f"Run pre-commit on {OPERATORS_DATA_FILE_NAME}")
                os.system(f"pre-commit run --files {OPERATORS_DATA_FILE_NAME}")
                app.logger.info(f"Adding {OPERATORS_DATA_FILE_NAME} to git")
                _git_repo.git.add(OPERATORS_DATA_FILE_NAME)
                app.logger.info(f"Committing changes for {OPERATORS_DATA_FILE_NAME}")
                _git_repo.git.commit("-m", f"'Auto update: {OPERATORS_DATA_FILE_NAME}'")
                app.logger.info(f"Push new changes for {OPERATORS_DATA_FILE}")
                _git_repo.git.push(repo_url)
                app.logger.info(f"New changes for {OPERATORS_DATA_FILE_NAME} pushed")
        except Exception as ex:
            err_msg = f"Failed to update {OPERATORS_DATA_FILE_NAME}. {ex}"
            app.logger.error(err_msg)
            send_slack_message(message=err_msg, webhook_url=slack_webhook_url)

    app.logger.info(f"Done check if {OPERATORS_DATA_FILE} was changed")


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


def trigger_openshift_ci_job(
    job,
    product,
    slack_webhook_url,
    _type,
    slack_errors_webhook_url,
    trigger_dict=None,
):
    app.logger.info(f"Triggering openshift-ci job for {product} [{_type}]: {job}")
    config_data = data_from_config()
    trigger_url = config_data["trigger_url"]
    job_dict = trigger_dict[[*trigger_dict][0]] if trigger_dict else None
    data = '{"job_execution_type": "1"}'
    res = requests.post(
        url=f"{trigger_url}/{job}",
        headers={"Authorization": f"Bearer {config_data['trigger_token']}"},
        data=data,
    )
    if not res.ok:
        msg = (
            f"Failed to trigger openshift-ci job: {job} for addon {product}, "
            f"code: {res.status_code}, reason: {res.reason}"
        )
        app.logger.error(msg)
        send_slack_message(
            message=msg,
            webhook_url=slack_errors_webhook_url,
        )
        return {}

    res_dict = json.loads(res.text)
    if res_dict["job_status"] != "TRIGGERED":
        msg = f"Failed to trigger openshift-ci job: {job} for addon {product}, response: {res_dict}"
        app.logger.error(msg)
        send_slack_message(
            message=msg,
            webhook_url=slack_errors_webhook_url,
        )
        return {}

    message = f"""
```
openshift-ci: New product {product} [{_type}] was merged/updated.
triggering job {job}
response:
    {dict_to_str(_dict=res_dict)}
```
Get the status of the job run:
```
curl -X GET -d '{data}' -H "Authorization: Bearer $OPENSHIFT_CI_TOKEN" {trigger_url}/{res_dict['id']}
```
"""
    if job_dict:
        message += f"""
Triggered using data:
    ```
    {operators_triggered_for_slack(job_dict=job_dict)}
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
    slack_errors_webhook_url = None
    while True:
        try:
            app.logger.info("Check for new operators IIB")
            config_data = data_from_config()
            slack_webhook_url = config_data["slack_webhook_url"]
            slack_errors_webhook_url = config_data["slack_errors_webhook_url"]
            token = config_data["github_token"]
            repo_url = f"https://{token}@github.com/RedHatQE/openshift-ci-trigger.git"
            clone_repo(repo_url=repo_url)
            trigger_dict = get_new_iib(operator_config_data=config_data)
            push_changes(repo_url=repo_url, slack_webhook_url=slack_errors_webhook_url)
            for _ocp_version, _job_data in trigger_dict.items():
                if any(extract_key_from_dict("triggered", _job_data)):
                    _job_name = [*_job_data][0]
                    trigger_openshift_ci_job(
                        job=_job_name,
                        product=", ".join(_job_data[_job_name].keys()),
                        slack_webhook_url=slack_webhook_url,
                        _type="operator",
                        slack_errors_webhook_url=slack_errors_webhook_url,
                        trigger_dict=trigger_dict,
                    )

        except Exception as ex:
            err_msg = f"Fail to run run_iib_update function. {ex}"
            app.logger.error(err_msg)
            send_slack_message(message=err_msg, webhook_url=slack_errors_webhook_url)

        finally:
            app.logger.info("Done check for new operators IIB, sleeping for 1 day")
            sleep(60 * 60 * 24)


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


def process_hook(api, data, slack_webhook_url, repository_data):
    config_data = data_from_config()
    slack_errors_webhook_url = config_data["slack_errors_webhook_url"]
    object_attributes = data["object_attributes"]
    if object_attributes.get("action") == "merge":
        project = api.projects.get(data["project"]["id"])
        merge_request = project.mergerequests.get(object_attributes["iid"])
        app.logger.info(
            f"{project.name}: New merge request [{merge_request.iid}] {merge_request.title}"
        )
        for change in merge_request.changes().get("changes", []):
            changed_file = change.get("new_path")
            # TODO: Get product version from changed_file and send it to slack
            matches = re.match(
                r"addons/(?P<product>.*)/addonimagesets/(production|stage)/.*.yaml",
                changed_file,
            )
            if matches:
                addon = matches.group("product")
                job = repository_data["products_jobs_mapping"].get(addon)
                if job:
                    trigger_openshift_ci_job(
                        job=job,
                        product=addon,
                        slack_webhook_url=slack_webhook_url,
                        _type="addon",
                        slack_errors_webhook_url=slack_errors_webhook_url,
                    )
                else:
                    app.logger.info(
                        f"{project.name}: No job found for product: {addon}"
                    )


@app.route("/process", methods=["POST"])
def process():
    slack_errors_webhook_url = None
    try:
        config_data = data_from_config()
        slack_errors_webhook_url = config_data["slack_errors_webhook_url"]
        hook_data = request.json
        event_type = hook_data["event_type"]
        repository_name = hook_data["repository"]["name"]
        app.logger.info(f"{repository_name}: Event type: {event_type}")
        repository_data = repo_data_from_config(repository_name=repository_name)
        slack_webhook_url = repository_data["slack_webhook_url"]
        api = get_api(
            url=repository_data["gitlab_url"], token=repository_data["gitlab_token"]
        )
        process_hook(
            api=api,
            data=hook_data,
            slack_webhook_url=slack_webhook_url,
            repository_data=repository_data,
        )
        return "Process done"
    except Exception as ex:
        err_msg = f"Failed to process hook: {ex}"
        app.logger.error(err_msg)
        send_slack_message(message=err_msg, webhook_url=slack_errors_webhook_url)
        return "Process failed"


def main():
    run_in_process()
    app.logger.info(f"Starting {app.name} app")
    app.run(port=5000, host="0.0.0.0", use_reloader=False)


if __name__ == "__main__":
    main()
