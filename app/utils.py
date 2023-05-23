import json
import os

import requests
import yaml

from app.app import app, dict_to_str


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
