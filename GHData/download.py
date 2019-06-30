import requests
import sys
import os

from azure.storage.common import CloudStorageAccount

account_name="jsoyteststorageaccount"
account_key="xgpW5An8/ZdM1pld1Jkm2hMHj2F/7UWzNmZ4lMLgZY449quN8DDR98hsYRDrR8PWIjE/NaE1OBcK8JIkTGvwpQ=="
container_name="jsoy-container-example"


mapping = [
    {"column": "Id", "path": "$.id"},
    {"column": "Type", "path": "$.type"},
    {"column": "Actor", "path": "$.actor"},
    {"column": "Repo", "path": "$.repo"},
    {"column": "Payload", "path": "$.payload"},
    {"column": "Public", "path": "$.public"},
    {"column": "CreatedAt", "path": "$.create_at"},
]


def download_locally(domain, path):
    if os.path.exists(path):
        print("{} exists on local fs, skipping...".format(path))
    else:
        uri = domain + path
        response = requests.get(uri, stream=True)

        try:
            # Throw an error for bad status codes
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print("{} not found, skipping.".format(uri))
            else:
                print("{} erred status {}".format(uri, e.response.status_code))
            
            return False

        with open(path, "wb") as handle:
            for block in response.iter_content(1024):
                handle.write(block)

    return True

def missing_file(path):
    storage_client = CloudStorageAccount(account_name=account_name, account_key=account_key)
    blob_service = storage_client.create_block_blob_service()

    return len(list(blob_service.list_blobs(container_name, path))) == 0


def upload_to_azure_storage(path):
    storage_client = CloudStorageAccount(account_name=account_name, account_key=account_key)
    blob_service = storage_client.create_block_blob_service()

    if len(list(blob_service.list_blobs(container_name, path))) > 0 and len(list(blob_service.list_blobs(container_name, path + ".gz"))) == 0:
        print("{} exists on azure storage, skipping...".format(path))
    else:
        blob_service.create_blob_from_path(container_name=container_name, blob_name=path, file_path=path)
        print("uploaded to storage {}".format(path))

def main():

    print("current folder is: {}".format(os.getcwd()))

    url_domain = "http://data.gharchive.org/"
    url_path = "{y}-{m:02d}-{d:02d}-{h}.json.gz"

    years = range(2016,2019)
    months = range(1,13)
    days = range(1, 32)
    hours = range(0, 24)

    for y in years:
        for m in months:
            for d in days:
                for h in hours:
                    p = url_path.format(y=y, m=m, d=d, h=h)
                    if missing_file(path=p):
                        if download_locally(domain=url_domain, path=p):
                            upload_to_azure_storage(p)
                    else:
                        print('{} found. SKIPPING...'.format(p))


if __name__ == "__main__":
    # main()
    for name in os.listdir():
        if name.endswith(".json"):
            upload_to_azure_storage(name)