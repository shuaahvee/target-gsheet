#!/usr/bin/env python3

import argparse
import collections
import functools
import http.client
import io
import json
import logging
import sys
import threading
import urllib

import httplib2
import pkg_resources
import singer
from apiclient import discovery
from jsonschema import validate
from oauth2client import client, tools
from oauth2client.file import Storage

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = "https://www.googleapis.com/auth/spreadsheets"
CLIENT_SECRET_FILE = "client_secret.json"
CREDENTIAL_FILE = "sheets.googleapis.com-singer-target.json"
APPLICATION_NAME = "Singer Sheets Target"

# Options
INSERT_OPTION_APPEND = "append"
INSERT_OPTION_REPLACE = "replace"


def get_credentials(config):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    client_secret = config.get("clientSecret", CLIENT_SECRET_FILE)
    credential_path = config.get("creds", CREDENTIAL_FILE)
    application_name = config.get("applicationName", APPLICATION_NAME)

    if client_secret_string := config.get("clientSecret", CLIENT_SECRET_FILE):
        logging.info("Using client secret string from config")
        client_secret_dict = json.loads(client_secret_string)
        client_secret_json = client.Credentials.to_json(client_secret_dict)
        return client.Credentials.new_from_json(client_secret_json)

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret, SCOPES)
        flow.user_agent = application_name
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print("Storing credentials to " + credential_path)
    return credentials


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def get_spreadsheet(service, spreadsheet_id):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def get_values(service, spreadsheet_id, range):
    return (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )


def add_sheet(service, spreadsheet_id, title):
    return (
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": title,
                                "gridProperties": {"rowCount": 1000, "columnCount": 26},
                            }
                        }
                    }
                ]
            },
        )
        .execute()
    )


def append_to_sheet(service, spreadsheet_id, range, values, insert_option=None):
    insert_option = insert_option or "insert"
    return (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=range,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        )
        .execute()
    )


def clear_sheet(service, spreadsheet_id, range):
    return (
        service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=range, body={})
        .execute()
    )


def flatten(d, parent_key="", sep="__"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def batch_lines(batch_size, batches, msg):
    def inner(func):
        @functools.wraps(func)
        def wrapper(line):
            if msg.stream not in batches:
                batches[msg.stream] = []
            if line:
                batches[msg.stream].append(line)
            if len(batches[msg.stream]) >= batch_size:
                func(batches[msg.stream])
                batches[msg.stream] = []

        return wrapper

    return inner


def persist_lines(service, spreadsheet, lines, config):
    batch_size = config.get("batchSize", 1000)
    insert_option = config.get("insertOption", INSERT_OPTION_APPEND)
    sheet_titles = {
        title.get("stream"): title.get("title")
        for title in config.get("sheetTitles", [])
        if title.get("stream")
    }

    state = None
    schemas = {}
    key_properties = {}

    headers_by_stream = {}

    cleared_sheets = []

    batches = {}

    append_send = functools.partial(
        append_to_sheet,
        service,
        spreadsheet["spreadsheetId"],
        insert_option=insert_option,
    )

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        msg.stream
                    )
                )

            schema = schemas[msg.stream]
            validate(msg.record, schema)
            flattened_record = flatten(msg.record)

            sheet_title = sheet_titles.get(msg.stream) or msg.stream
            matching_sheet = [
                s
                for s in spreadsheet["sheets"]
                if s["properties"]["title"] == sheet_title
            ]
            new_sheet_needed = len(matching_sheet) == 0
            range_name = "{}!A1:ZZZ".format(sheet_title)
            append = batch_lines(batch_size, batches, msg)(
                functools.partial(append_send, range_name)
            )

            if new_sheet_needed:
                add_sheet(service, spreadsheet["spreadsheetId"], sheet_title)
                spreadsheet = get_spreadsheet(
                    service, spreadsheet["spreadsheetId"]
                )  # refresh this for future iterations
                headers_by_stream[msg.stream] = list(flattened_record.keys())
                append(headers_by_stream[msg.stream])

            elif (
                insert_option == INSERT_OPTION_REPLACE
                and sheet_title not in cleared_sheets
            ):
                clear_sheet(service, spreadsheet["spreadsheetId"], range_name)
                cleared_sheets.append(sheet_title)
                headers_by_stream[msg.stream] = list(flattened_record.keys())
                append(headers_by_stream[msg.stream])

            elif msg.stream not in headers_by_stream:
                first_row = get_values(
                    service, spreadsheet["spreadsheetId"], range_name + "1"
                )
                if "values" in first_row:
                    headers_by_stream[msg.stream] = first_row.get("values", None)[0]
                else:
                    headers_by_stream[msg.stream] = list(flattened_record.keys())
                    append(headers_by_stream[msg.stream])

            append(
                [flattened_record.get(x, None) for x in headers_by_stream[msg.stream]]
            )  # order by actual headers found in sheet

            state = None
        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value
        elif isinstance(msg, singer.SchemaMessage):
            schemas[msg.stream] = msg.schema
            key_properties[msg.stream] = msg.key_properties
        else:
            raise Exception("Unrecognized message {}".format(msg))
    # Load remaining in batch
    for stream, batch in batches.items():
        if batch:
            sheet_title = sheet_titles.get(stream) or stream
            range_name = "{}!A1:ZZZ".format(sheet_title)
            append_send(range_name, batch)

    return state


def collect():
    try:
        version = pkg_resources.get_distribution("target-gsheet").version
        conn = http.client.HTTPSConnection("collector.stitchdata.com", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-gsheet",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug("Collection request failed")


def main():
    with open(flags.config) as input:
        config = json.load(input)

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to stitchdata.com. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=collect).start()

    credentials = get_credentials(config)
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = "https://sheets.googleapis.com/$discovery/rest?" "version=v4"
    service = discovery.build(
        "sheets", "v4", http=http, discoveryServiceUrl=discoveryUrl
    )

    spreadsheet = get_spreadsheet(service, config["spreadsheet_id"])

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = None
    state = persist_lines(service, spreadsheet, input, config)
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
