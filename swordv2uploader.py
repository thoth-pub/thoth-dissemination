#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to a server using SWORD v2
"""

import logging
import sys
import sword2
from io import BytesIO
from os import environ
from uploader import Uploader


class SwordV2Uploader(Uploader):
    """Dissemination logic for SWORD v2"""
    # using UCamApollo DSpace 7 as hard-coded logic for now.

    def upload_to_platform(self):
        """Upload work in required format to SWORD v2"""

        # Metadata file format TBD: use CSV for now
        metadata_bytes = self.get_formatted_metadata('csv::thoth')
        pdf_bytes = self.get_pdf_bytes()

        # Convert Thoth work metadata into SWORD v2 format
        sword_metadata = self.parse_metadata()

        # Set up SWORD v2 endpoint connection
        conn = sword2.Connection(
            service_document_iri="https://dspace7-back.lib.cam.ac.uk/server/swordv2/collection/1810/339712",
            user_name=environ.get('cam_ds7_user'),
            user_pass=environ.get('cam_ds7_pw'),
            # SWORD2 library doesn't handle timeout-related errors gracefully and large files
            # (e.g. 50MB) can't be fully uploaded within the 30-second default timeout.
            # Allow lots of leeway. (This otherwise matches the default `http_impl`.)
            http_impl=sword2.http_layer.HttpLib2Layer(timeout=120.0)
        )

        try:
            receipt = conn.create(
                col_iri="https://dspace7-back.lib.cam.ac.uk/server/swordv2/collection/1810/339712",
                metadata_entry=sword_metadata,
                in_progress=True,
            )
        except sword2.exceptions.Forbidden:
            logging.error(
                'Could not connect to SWORD v2 server: authorisation failed')
            sys.exit(1)

        if receipt.code != 201:
            logging.error(
                'Error uploading item data to SWORD v2')
            sys.exit(1)

        try:
            pdf_receipt = conn.add_file_to_resource(
                edit_media_iri=receipt.edit_media,
                payload=pdf_bytes,
                # Filename TBD: use work ID for now
                filename='{}.pdf'.format(self.work_id),
                mimetype='application/pdf',
                in_progress=True,
            )
        except sword2.exceptions.Forbidden:
            logging.error(
                'Could not connect to SWORD v2 server: authorisation failed')
            sys.exit(1)

        if pdf_receipt.code != 201:
            logging.error(
                'Error uploading PDF file to SWORD v2')
            sys.exit(1)

        try:
            metadata_receipt = conn.add_file_to_resource(
                edit_media_iri=receipt.edit_media,
                payload=metadata_bytes,
                # Filename TBD: use work ID for now
                filename='{}.csv'.format(self.work_id),
                mimetype='text/csv',
                in_progress=True,
            )
        except sword2.exceptions.Forbidden:
            logging.error(
                'Could not connect to SWORD v2 server: authorisation failed')
            sys.exit(1)

        if metadata_receipt.code != 201:
            logging.error(
                'Error uploading metadata file to SWORD v2')
            sys.exit(1)

        logging.info(
            'Successfully uploaded to SWORD v2 at {}'.format(receipt.location))

    def parse_metadata(self):
        """Convert work metadata into SWORD v2 format"""
        work_metadata = self.metadata.get('data').get('work')
        sword_metadata = sword2.Entry(
            title=work_metadata.get('fullTitle')
        )

        return sword_metadata
