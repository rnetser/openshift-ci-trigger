import json
from json import JSONDecodeError
from multiprocessing import Process
from time import sleep

import requests
from git import Repo
from utils import data_from_config


OPERATORS_DATA_FILE = "operators-latest-iib.json"


def read_data_file():
    with open(OPERATORS_DATA_FILE, "a+") as fd:
        try:
            return json.load(fd)
        except JSONDecodeError:
            return {}


def get_operator_data_from_url(datagrepper_config_data, operator_name):
    res = requests.get(
        f"{datagrepper_config_data['datagrepper_query_url']}{operator_name}",
        verify=False,
    )
    return res.json()


def get_new_iib(operator_config_data):
    data = read_data_file()
    for operator_name in ["rhods"]:
        res = get_operator_data_from_url(
            datagrepper_config_data=operator_config_data, operator_name=operator_name
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
                else:
                    operator_data[ocp_version] = {"iib": iib_number}

            else:
                data[operator_name] = {ocp_version: {"iib": iib_number}}

    with open(OPERATORS_DATA_FILE, "w") as fd:
        fd.write(json.dumps(data))


def push_changes(git_config_data):
    token = git_config_data["github_token"]
    git_repo = Repo(".")
    if OPERATORS_DATA_FILE in git_repo.git.status():
        git_repo.git.add(OPERATORS_DATA_FILE)
        git_repo.git.commit("-m", f"Auto update {OPERATORS_DATA_FILE}", "--no-verify")
        git_repo.git.push(
            f"https://{token}@github.com/RedHatQE/openshift-ci-trigger.git"
        )


def run_in_process():
    proc = Process(target=main)
    proc.start()


def main():
    while True:
        config_data = data_from_config()
        get_new_iib(operator_config_data=config_data)
        push_changes(git_config_data=config_data)
        sleep(60 * 5)


if __name__ == "__main__":
    main()
