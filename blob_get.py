from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
import os

#name of your storage account and the access key from Settings->AccessKeys->key1
block_blob_service = BlockBlobService(account_name='jsoyteststorageaccount', account_key='xgpW5An8/ZdM1pld1Jkm2hMHj2F/7UWzNmZ4lMLgZY449quN8DDR98hsYRDrR8PWIjE/NaE1OBcK8JIkTGvwpQ==')

#name of the container
generator = block_blob_service.list_blobs('jsoy-container-example')

#code below lists all the blobs in the container and downloads them one after another
for blob in generator:
    print(blob.name)
    print("{}".format(blob.name))
    #check if the path contains a folder structure, create the folder structure
    if "/" in "{}".format(blob.name):
        print("there is a path in this")
        #extract the folder path and check if that folder exists locally, and if not create it
        head, tail = os.path.split("{}".format(blob.name))
        print(head)
        print(tail)
        if (os.path.isdir(os.getcwd()+ "/" + head)):
            #download the files to this directory
            print("directory and sub directories exist")
            block_blob_service.get_blob_to_path('jsoy-container-example',blob.name,os.getcwd()+ "/" + head + "/" + tail)
        else:
            #create the diretcory and download the file to it
            print("directory doesn't exist, creating it now")
            os.makedirs(os.getcwd()+ "/" + head, exist_ok=True)
            print("directory created, download initiated")
            block_blob_service.get_blob_to_path('jsoy-container-example',blob.name,os.getcwd()+ "/" + head + "/" + tail)
    else:
        print(blob.name)
        block_blob_service.get_blob_to_path('jsoy-container-example',blob.name,blob.name)
