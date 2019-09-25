import argparse
import logging
import time

from google.cloud import firestore
from retry_redis import Redis

LOGGER = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parent_parser = argparse.ArgumentParser(add_help=False)

    datastore_parser = argparse.ArgumentParser(add_help=False)
    datastore_parser.add_argument('--service-key-path',
                                  action='store',
                                  help='service key json path')

    memorystore_parser = argparse.ArgumentParser(add_help=False)
    memorystore_parser.add_argument('--host', action='store', required=True)
    memorystore_parser.add_argument('--port', action='store', required=True)

    command_subparser = parser.add_subparsers(dest='client_type')
    command_subparser.required = True
    command_subparser.add_parser("datastore", parents=[parent_parser,
                                                       datastore_parser])
    command_subparser.add_parser("memorystore", parents=[parent_parser,
                                                         memorystore_parser])

    return parser.parse_args()


def _main():
    options = _parse_args()
    client_type = options.client_type

    if client_type == "datastore":
        db = None
        # Using firestore
        service_key_path = options.service_key_path
        if service_key_path:
            db = firestore.Client.from_service_account_json(service_key_path)
        else:
            db = firestore.Client()
        start_time = time.time()
        doc_ref = db.collection('test-henley').document('34')
        doc = doc_ref.get()
        end_time = time.time()
        elapsed_time = end_time - start_time
        LOGGER.info('Document data: {} | Elapsed time: {}'.format(
            doc.to_dict(), elapsed_time))
    elif client_type == "memorystore":
        host = options.host
        port = options.port
        redis_client = Redis(host=host, port=port)
        start_time = time.time()
        data = redis_client.hget("test-henley", "34")
        end_time = time.time()
        elapsed_time = end_time - start_time
        LOGGER.info('Document data: {} | Elapsed time: {}'.format(
            data, elapsed_time))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
