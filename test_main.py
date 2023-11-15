import os
import requests
from dotenv import load_dotenv


def trigger_databricks_job():
    load_dotenv()
    access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
    job_id = os.getenv("JOB_ID")
    server_h = os.getenv("SERVER_HOSTNAME")

    url = f"https://{server_h}/api/2.0/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    data = {"job_id": job_id}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        print("Job run successfully triggered")
    else:
        print(f"Error: {response.status_code}, {response.text}")


# Call the function to trigger the Databricks job
if __name__ == "__main__":
    trigger_databricks_job()
