import requests
from dotenv import load_dotenv
import os
import json
import base64


load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("PERSONAL_ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/extract_fs"
headers = {"Authorization": "Bearer %s" % access_token}
url = "https://" + server_h + "/api/2.0"


# Defining functions necessary to add dataset to dbfs:


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        "POST", url + path, data=json.dumps(data), verify=True, headers=headers
    )
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data["path"] = path
    return perform_query("/dbfs/mkdirs", headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {}
    _data["path"] = path
    _data["overwrite"] = overwrite
    return perform_query("/dbfs/create", headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data["handle"] = handle
    _data["data"] = data
    return perform_query("/dbfs/add-block", headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data["handle"] = handle
    return perform_query("/dbfs/close", headers=headers, data=_data)


def put_file(src_path, dbfs_path, overwrite, headers):
    handle = create(dbfs_path, overwrite, headers=headers)["handle"]
    print("Putting file: " + dbfs_path)
    with open(src_path, "rb") as local_file:
        while True:
            contents = local_file.read(2**20)
            if len(contents) == 0:
                break
            add_block(
                handle, base64.standard_b64decode(contents).decode(), headers=headers
            )
        close(handle, headers=headers)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)["handle"]
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(
                handle,
                base64.standard_b64encode(content[i : i + 2**20]).decode(),
                headers=headers,
            )
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
    url="https://raw.githubusercontent.com/nogibjj/aad64_PySpark/main/songs_normalize.csv",
    file_path=FILESTORE_PATH + "/spotify.csv",
    directory=FILESTORE_PATH,
):
    """Extract a url to a file path"""
    # Making the directory
    mkdirs(path=directory, headers=headers)
    # Adding the csv file
    put_file_from_url(url, file_path, overwrite=True, headers=headers)

    return file_path


if __name__ == "__main__":
    extract()
