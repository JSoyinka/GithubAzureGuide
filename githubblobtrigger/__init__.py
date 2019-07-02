import logging
import os
import azure.functions as func
from .cleaner import parse_issue_events
from .cleaner import satiisfies_conditions

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    # Reads text file of file days read and once it passes a certain limit
    if my.blob.name(satiisfies_conditions()):
        parse_issue_events()

