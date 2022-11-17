#!/usr/bin/env python3
"""
Work files and metadata disseminator

Call custom workflows to retrieve work-related files and metadata
and upload them in the appropriate format to various platforms.
"""

__version__ = 'unversioned'

import argparse
import logging
from dotenv import load_dotenv
from pathlib import Path
from iauploader import IAUploader
from oapenuploader import OAPENUploader
from souploader import SOUploader
from swordv2uploader import SwordV2Uploader

UPLOADERS = {
    "InternetArchive": IAUploader,
    "OAPEN": OAPENUploader,
    "ScienceOpen": SOUploader,
    "SWORD": SwordV2Uploader,
}

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
        "help": "Platform to which to disseminate work. One of: {}".format(
            ', '.join("%s" % (key) for (key, val) in UPLOADERS.items())
        )
    }, {
        "val": "--export-url",
        "dest": "export_url",
        "action": "store",
        "default": "https://export.thoth.pub",
        "help": "Thoth's Export API endpoint URL",
    }
]


def run(work_id, platform, export_url):
    """Execute a dissemination uploader based on input parameters"""
    uploader = UPLOADERS[platform](work_id, export_url, __version__)
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
    # dotenv only required for running locally - when running
    # with Docker, --env-file option could be used instead
    dotenv_path = Path('./config.env')
    load_dotenv(dotenv_path=dotenv_path)
    ARGUMENTS = get_arguments()
    run(ARGUMENTS.work_id, ARGUMENTS.platform, ARGUMENTS.export_url)
