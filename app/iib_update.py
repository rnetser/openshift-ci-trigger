import json
from json import JSONDecodeError
from multiprocessing import Process
from time import sleep

import requests
from git import Repo
from utils import data_from_config, trigger_openshift_ci_job


OPERATORS_DATA_FILE = "operators-latest-iib.json"
OPERATORS_AND_JOBS_MAPPING = {
    "rhods": {"4.13": "periodic-ci-CSPI-QE-MSI-rhods-operator-v4.13-rhods-tests"}
}


def read_data_file():
    with open(OPERATORS_DATA_FILE, "a+") as fd:
        try:
            return json.load(fd)
        except JSONDecodeError:
            return {}


def get_operator_data_from_url(datagrepper_config_data, operator_name, logger):
    logger.info(f"Getting IIB data for {operator_name}")
    res = requests.get(
        f"{datagrepper_config_data['datagrepper_query_url']}{operator_name}",
        verify=False,
    )
    return res.json()


def get_new_iib(operator_config_data, logger):
    data = read_data_file()
    trigger_dict = {}
    for operator_name in ["rhods"]:
        trigger_dict[operator_name] = {}
        logger.info(f"Parsing new IIB data for {operator_name}")
        res = get_operator_data_from_url(
            datagrepper_config_data=operator_config_data,
            operator_name=operator_name,
            logger=logger,
        )
        for raw_msg in res["raw_messages"]:
            iib_data = raw_msg["msg"]["index"]
            ocp_version = iib_data["ocp_version"]
            operator_data = data.get(operator_name)
            iib_number = iib_data["index_image"].split("iib:")[-1]
            if operator_data:
                version_data = operator_data.get(ocp_version)
                if version_data == ocp_version:
                    if version_data["iib"] < iib_number:
                        version_data["iib"] = iib_number
                        trigger_dict[operator_name][ocp_version] = True
                else:
                    operator_data[ocp_version] = {"iib": iib_number}
                    trigger_dict[operator_name][ocp_version] = True

            else:
                data[operator_name] = {ocp_version: {"iib": iib_number}}
                trigger_dict[operator_name][ocp_version] = True

    with open(OPERATORS_DATA_FILE, "w") as fd:
        fd.write(json.dumps(data))

    return trigger_dict


def push_changes(git_config_data, logger):
    token = git_config_data["github_token"]
    git_repo = Repo(".")
    logger.info(f"Check if {OPERATORS_DATA_FILE} was changed")
    if OPERATORS_DATA_FILE in git_repo.git.status():
        git_repo.git.add(OPERATORS_DATA_FILE)
        git_repo.git.commit("-m", f"Auto update {OPERATORS_DATA_FILE}", "--no-verify")
        logger.info(f"Push new changes for {OPERATORS_DATA_FILE}")
        git_repo.git.push(
            f"https://{token}@github.com/RedHatQE/openshift-ci-trigger.git"
        )


def run_in_process(logger):
    proc = Process(target=main, args=(logger,))
    proc.start()


def main(logger):
    while True:
        config_data = data_from_config()
        slack_webhook_url = config_data["slack_webhook_url"]
        trigger_dict = get_new_iib(operator_config_data=config_data, logger=logger)
        push_changes(git_config_data=config_data, logger=logger)
        for _operaotr, _version in trigger_dict.items():
            if _version:
                job = OPERATORS_AND_JOBS_MAPPING[_operaotr][_version]
                trigger_openshift_ci_job(
                    job=job, product=_operaotr, slack_webhook_url=slack_webhook_url
                )

        sleep(60 * 5)
