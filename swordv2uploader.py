#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to a server using SWORD v2
"""

import logging
import sys
import sword2
from io import BytesIO
from os import environ
from errors import DisseminationError
from uploader import Uploader


class SwordV2Uploader(Uploader):
    """Dissemination logic for SWORD v2"""
    # using UCamApollo DSpace 7 as hard-coded logic for now.

    def upload_to_platform(self):
        """Upload work in required format to SWORD v2"""

        # Metadata file format TBD: use CSV for now
        metadata_bytes = self.get_formatted_metadata('csv::thoth')
        # Can't continue if no PDF file is present
        try:
            pdf_bytes = self.get_pdf_bytes()
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

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
                # Hacky workaround for an issue with mishandling of encodings within sword2 library,
                # which meant metadata containing special characters could not be submitted.
                # Although the `metadata_entry` parameter ought to be of type `Entry`, sending a
                # `str` as below triggers no errors. Ultimately it's passed to `http/client.py/_encode()`,
                # which defaults to encoding it as 'latin-1'. Pre-emptively encoding/decoding it here
                # seems to mean that the string sent to the server is in correct utf-8 format.
                metadata_entry=str(sword_metadata).encode(
                    'utf-8').decode('latin-1'),
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
            # All fields are non-mandatory and any None values are ignored on ingest
            # (within Apollo DSpace 7 - yet to test other SWORD2-based platforms)
            # Some of the below fields do not appear to be stored/
            # correctly displayed by Apollo, although they are valid within SWORD2
            title=work_metadata.get('fullTitle'),
            dcterms_publisher=self.get_publisher_name(),
            dcterms_issued=work_metadata.get('publicationDate'),
            dcterms_description=work_metadata.get('longAbstract'),
            dcterms_identifier=work_metadata.get('doi'),
            dcterms_license=work_metadata.get('license'),
            dcterms_tableOfContents=work_metadata.get('toc'),
        )
        # Workaround for adding repeatable fields
        for contributor in [n.get('fullName') for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            sword_metadata.add_field("dcterms_contributor", contributor)
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            sword_metadata.add_field("dcterms_subject", subject)
        for isbn in [n.get('isbn').replace(
                '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]:
            sword_metadata.add_field("dcterms_identifier", isbn)
        for language in [n.get('languageCode') for n in work_metadata.get('languages')]:
            sword_metadata.add_field("dcterms_language", language)

        return sword_metadata
