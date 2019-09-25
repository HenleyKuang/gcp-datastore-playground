import argparse
import logging
import time

from google.cloud import firestore

LOGGER = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parent_parser = argparse.ArgumentParser(add_help=False)

    datastore_parser = argparse.ArgumentParser(add_help=False)
    datastore_parser.add_argument('--service-key-path',
                                  action='store',
                                  required=True,
                                  help='service key json path')

    command_subparser = parser.add_subparsers(dest='client_type')
    command_subparser.required = True
    command_subparser.add_parser("datastore", parents=[parent_parser,
                                                       datastore_parser])

    return parser.parse_args()


def _main():
    options = _parse_args()
    client_type = options.client_type

    if client_type == "datastore":
        service_key_path = options.service_key_path
        db = firestore.Client.from_service_account_json(service_key_path)
        docs = db.collection(u'test-henley').stream()
        for doc in docs:
            LOGGER.info('{} => {}'.format(doc.id, doc.to_dict()))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
