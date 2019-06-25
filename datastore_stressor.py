#!/bin/env python

"""
A module to test pushing and pulling data to/from the Ground's Datastore API.
"""

# Standard libraries
import argparse
import base64
import gzip
import json
import logging
import random
import requests
import string
import subprocess
import time
import typing


DATA_FILE = './data.txt'
SERIAL = 'M000000000000'
TYPE = 'inquire.network'
DATA_FILE_KEY = 'dataFileUri'

PUSH_URL = 'https://services.bsg.stage.gogoair.com/datastore/v1/item'
PULL_URL = 'https://services.bsg.stage.gogoair.com/datastore/v1/item/data/all'

ID_LEN = 5
WAIT_TIME = 2


def main(tries: int):
    """
    Main.
    """
    if not create_data_file(DATA_FILE):
        logging.error('Unable to create data file, exiting')
        return -1

    for attempt in range(1, tries + 1):
        tag = create_id()
        logging.info('##########\n# Attempt #{}:'.format(attempt))
        logging.info('# serial: {}'.format(SERIAL))
        logging.info('# id: {}'.format(tag))

        push(TYPE, PUSH_URL, SERIAL, filepath=DATA_FILE, tag=tag)
        response = pull(PULL_URL, SERIAL, TYPE, tag)

        uris = parse_response(response)

        with open(DATA_FILE, 'rb') as file:
            built_data = file.read()

            for uri in uris:
                runtime_data = requests.get(uri).content

                if runtime_data == built_data:
                    logging.info('# PASS')

                else:
                    logging.info('# FAIL')

        logging.info('##########')

        if attempt < tries:
            time.sleep(WAIT_TIME)


def create_data_file(path: str) -> bool:
    """
    Create a dataset for publishing to the datastore.  This is just the output of 'dmesg'
    gzipped.
    """
    proc = subprocess.run(['dmesg'], capture_output=True)

    if proc.returncode:
        logging.error('# Failed to generate data: {}'.format(proc.stderr))
        return False

    try:
        with gzip.open(path, 'wb') as file:
            file.write(proc.stdout)

    except IOError as err:
        logging.error('# Failed to write generated data: {}'.format(err))
        return False

    return True


def create_id() -> str:
    """
    Returns a string of the form 'test-<letters+digits>' for use as an ID.
    """
    rand = ''.join(random.choices(string.ascii_letters + string.digits, k=ID_LEN))

    return 'test-' + rand


def parse_response(response: requests.Response) -> typing.List[str]:
    """
    Parse the given response, returning a list of all 'dataFileUri's in the response.
    """
    datafileuri = 'dataFileUri'
    json_response: dict = {}

    if response.ok:
        try:
            json_response = response.json()

        except ValueError as err:
            logging.warning('# Failed to decode json response: {}'.format(err))

    else:
        logging.warning('# Bad response: {}: {}'.format(response.status_code,
                                                        response.reason))

    uris = []
    for element in json_response:
        if element and datafileuri in element:
            uris.append(element[datafileuri])

    return uris


def pull(url: str, serial: str, data_type: str, tag: str):
    """
    Pull a response from the datastore.
    """
    headers = {'Content-type': 'application/json'}
    data = {"serial": serial, "type": data_type, "tag": tag}

    data_json = json.dumps(data)

    try:
        logging.info('# pulling from datastore')
        response = requests.post(url, headers=headers, data=data_json)

    except Exception as err:
        logging.error('# Error posting request: {}'.format(err))
        return ''

    return response


def push(data_type: str, url: str, serial: str, filepath="", data="", tag="") -> bool:
    """
    Push data to the datastore based on the configuration given by parameters.  Returns
    a boolean indicating success.

    Notes:
    Either 'file' or 'data' parameters must be passed or the call will fail.

    data_type: the "type" parameter for the Datastore API.
    serial: the LRU serial number.
    file: a file to parse and use as data.
    data: a str to use as data.
    tag: an optional arbitrary string.  Example: the instruction id used by instructd

    Exceptions:
        IOError
        OSError
        requests.exception.RequestException
    """

    if filepath is None and data is None:
        raise ValueError('Missing local_file or data_item')

    # item is a json string. It has 4 parameters.
    item_obj = {'serial': serial, 'type': str(data_type)}

    # Add the data.
    if data:
        # data_item needs to be a str with at least 2 bytes
        item_obj['data'] = base64.standard_b64encode(data).decode('utf-8')

    # Add the tag.
    if tag:
        item_obj['tag'] = tag

    form_data: dict = {"item": (None, json.dumps(item_obj))}

    file_handle = None
    try:
        if filepath:
            file_handle = open(filepath, 'rb')
            form_data['dataFile'] = file_handle

        # always send as multipart/form-data
        logging.info('# pushing to datastore')
        response = requests.post(url, files=form_data)
        logging.info('# datastore response: %s', response.text)

        if response.ok:
            # The expected response string is {"success":true}.
            if not response.json()['success']:
                logging.warning('# Datastore API responded with failure: {}',
                                response.text)
                return False

            return True

        else:
            logging.warning('# Pushing to the datastore failed: status {}: {}'
                            .format(response.status_code, response.text))
            return False

    except ValueError as err:
        logging.warning('# Datastore returned an unknown response: {} ({})'
                        .format(err, response.text))
        return False

    except requests.RequestException as err:
        logging.warning('# Network error pushing to datastore: {}'.format(err))
        return False

    finally:
        # Depending on the failure, the file may not have been closed by requests.
        if file_handle:
            file_handle.close()

    return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(description='Stress the datastore')
    parser.add_argument('--tries', '-t', type=int, default=1,
                        help='The number of tries to run')
    args = parser.parse_args()

    main(args.tries)
