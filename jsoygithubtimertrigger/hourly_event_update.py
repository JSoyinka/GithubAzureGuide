# Modify universal code to run per hour
# Pull from the last hour

for blob in generator:
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