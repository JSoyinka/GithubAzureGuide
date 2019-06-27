import requests
import sys
import os
from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, DataFormat, BlobDescriptor
from azure.storage.common import CloudStorageAccount

# kusto_url = <>
# app_id = <>
# app_key = <>
# auth_id = <>

storage_account_name="jsoyteststorageaccount"
storage_account_key="xgpW5An8/ZdM1pld1Jkm2hMHj2F/7UWzNmZ4lMLgZY449quN8DDR98hsYRDrR8PWIjE/NaE1OBcK8JIkTGvwpQ=="
storage_container="jsoy-container-example"

# client = KustoIngestClient(
#     KustoConnectionStringBuilder.with_aad_application_key_authentication(
#        kusto_url,app_id,app_key,auth_id
#     )
# )

mapping = [
    {"column": "Id", "path": "$.id"},
    {"column": "Type", "path": "$.type"},
    {"column": "Actor", "path": "$.actor"},
    {"column": "Repo", "path": "$.repo"},
    {"column": "Payload", "path": "$.payload"},
    {"column": "Public", "path": "$.public"},
    {"column": "CreatedAt", "path": "$.created_at"},
]


def ingest(file, size):
    props = IngestionProperties(
        database="GitHub",
        table="GithubEvent",
        dataFormat=DataFormat.json,
        mapping=mapping,
        ingestIfNotExists=[file],
        ingestByTags=[file],
        dropByTags=[file[57:67]],
    )

    client.ingest_from_blob(BlobDescriptor(file, size), props)

    print("ingested {}".format(file))


def main():
    storage_client = CloudStorageAccount(account_name=storage_account_name, account_key=storage_account_key)
    blob_service = storage_client.create_block_blob_service()

    years = range(2016, 2019)
    months = range(1, 13)
    days = range(1, 32)
    hours = range(0, 24)
    url_path = "{y}-{m:02d}-{d:02d}-{h}.json.gz"

    for y in years:
        for m in months:
            for d in days:
                for h in hours:
                    p = url_path.format(y=y, m=m, d=d, h=h)

                    if os.path.exists(p + "_done"):
                        print("{} ingested. skipping...".format(p))
                        continue

                    ingest(
                        "https://{}.blob.core.windows.net/{}/{};{}".format(
                            storage_account_name, storage_container, p, storage_account_key
                        ),
                        50 * 1024 * 1024,
                    )

                    with open(p + "_done", "w+") as f:
                        f.write(" ")


if __name__ == "__main__":
    main()