#!/usr/bin/env python3
"""
Work files and metadata disseminator

Call custom workflows to retrieve work-related files and metadata
and upload them in the appropriate format to various platforms.
"""

__version__ = '0.1.27'

import argparse
import logging
import sys
from dotenv import load_dotenv
from pathlib import Path
from iauploader import IAUploader
from oapenuploader import OAPENUploader
from souploader import SOUploader
from culuploader import CULUploader
from crossrefuploader import CrossrefUploader
from fsuploader import FigshareUploader
from zenodouploader import ZenodoUploader
from museuploader import MUSEUploader
from jstoruploader import JSTORUploader
from ebscouploader import EBSCOUploader
from proquestuploader import ProquestUploader
from googleplayuploader import GooglePlayUploader
from bkciuploader import BKCIUploader

UPLOADERS = {
    "InternetArchive": IAUploader,
    "OAPEN": OAPENUploader,
    "ScienceOpen": SOUploader,
    "CUL": CULUploader,
    "Crossref": CrossrefUploader,
    "Figshare": FigshareUploader,
    "Zenodo": ZenodoUploader,
    "ProjectMUSE": MUSEUploader,
    "JSTOR": JSTORUploader,
    "EBSCOHost": EBSCOUploader,
    "ProQuest": ProquestUploader,
    "GooglePlay": GooglePlayUploader,
    "BKCI": BKCIUploader,
}

UPLOADERS_STR = ', '.join("%s" % (key) for (key, _) in UPLOADERS.items())

ARGS = [
    {
        "val": "--work",
        "dest": "work_id",
        "action": "store",
        "help": "Thoth Work ID of work to be disseminated",
    }, {
        "val": "--platform",
        "dest": "platform",
        "action": "store",
        "help": "Platform to which to disseminate work. One of: {}".format(UPLOADERS_STR)
    }, {
        "val": "--export-url",
        "dest": "export_url",
        "action": "store",
        "default": "https://export.thoth.pub",
        "help": "Thoth's Export API endpoint URL",
    }, {
        "val": "--client-url",
        "dest": "client_url",
        "action": "store",
        "default": None,
        "help": "URL of GraphQL API endpoint to use with Thoth Client",
    }
]


def run(work_id, platform, export_url, client_url):
    """Execute a dissemination uploader based on input parameters"""
    logging.info('Beginning upload of {} to {}'.format(work_id, platform))
    try:
        uploader = UPLOADERS[platform](
            work_id, export_url, client_url, __version__)
    except KeyError:
        logging.error('{} not supported: platform must be one of {}'.format(
            platform, UPLOADERS_STR))
        sys.exit(1)
    uploader.run()


def get_arguments():
    """Parse input arguments using ARGS"""
    parser = argparse.ArgumentParser()
    for arg in ARGS:
        if 'default' in arg:
            parser.add_argument(arg["val"], dest=arg["dest"],
                                default=arg["default"], action=arg["action"],
                                help=arg["help"])
        else:
            parser.add_argument(arg["val"], dest=arg["dest"], required=True,
                                action=arg["action"], help=arg["help"])
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')
    # DEBUG level urllib3 logs may contain sensitive information
    # such as passwords (where sent as URL query parameters)
    # and should never be output publicly (e.g. in GitHub Actions)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    # dotenv only required for running locally - when running
    # with Docker, --env-file option could be used instead
    dotenv_path = Path('./config.env')
    load_dotenv(dotenv_path=dotenv_path)
    ARGUMENTS = get_arguments()
    run(ARGUMENTS.work_id, ARGUMENTS.platform,
        ARGUMENTS.export_url, ARGUMENTS.client_url)
