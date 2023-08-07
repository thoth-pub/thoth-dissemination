#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to a server using SWORD v2
"""

import logging
import sys
import sword2
from enum import Enum
from errors import DisseminationError
from uploader import Uploader


class RequestType(Enum):
    CREATE_ITEM = 1
    UPLOAD_PDF = 2
    UPLOAD_METADATA = 3
    COMPLETE_DEPOSIT = 4
    DELETE_ITEM = 5


class SwordV2Uploader(Uploader):
    """Dissemination logic for SWORD v2"""
    # using UCamApollo DSpace 7 as hard-coded logic for now.

    def __init__(self, work_id, export_url, version):
        """Create connection to SWORD v2 endpoint"""
        super().__init__(work_id, export_url, version)
        try:
            user_name = self.get_credential_from_env(
                'cam_ds7_user', 'SWORD v2')
            user_pass = self.get_credential_from_env('cam_ds7_pw', 'SWORD v2')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)
        self.conn = sword2.Connection(
            service_document_iri='https://dspace7-back.lib.cam.ac.uk/server/swordv2/collection/1810/339712',
            user_name=user_name,
            user_pass=user_pass,
            # SWORD2 library doesn't handle timeout-related errors gracefully and large files
            # (e.g. 50MB) can't be fully uploaded within the 30-second default timeout.
            # Allow lots of leeway. (This otherwise matches the default `http_impl`.)
            http_impl=sword2.http_layer.HttpLib2Layer(timeout=120.0)
        )

    def upload_to_platform(self):
        """Upload work in required format to SWORD v2"""

        # Metadata file format TBD: use JSON for now
        metadata_bytes = self.get_formatted_metadata('json::thoth')
        # Can't continue if no PDF file is present
        try:
            pdf_bytes = self.get_publication_bytes('PDF')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Convert Thoth work metadata into SWORD v2 format
        sword_metadata = self.parse_metadata()

        try:
            receipt = self.handle_request(
                request_type=RequestType.CREATE_ITEM,
                expected_status=201,
                # Hacky workaround for an issue with mishandling of encodings within sword2 library,
                # which meant metadata containing special characters could not be submitted.
                # Although the `metadata_entry` parameter ought to be of type `Entry`, sending a
                # `str` as below triggers no errors. Ultimately it's passed to `http/client.py/_encode()`,
                # which defaults to encoding it as 'latin-1'. Pre-emptively encoding/decoding it here
                # seems to mean that the string sent to the server is in correct utf-8 format.
                metadata_entry=str(sword_metadata).encode(
                    'utf-8').decode('latin-1'),
            )
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Any failure after this point will leave incomplete data in
        # SWORD v2 server which will need to be removed.
        try:
            pdf_receipt = self.handle_request(
                request_type=RequestType.UPLOAD_PDF,
                expected_status=201,
                edit_media_iri=receipt.edit_media,
                payload=pdf_bytes,
            )

            metadata_receipt = self.handle_request(
                request_type=RequestType.UPLOAD_METADATA,
                expected_status=201,
                edit_media_iri=receipt.edit_media,
                payload=metadata_bytes,
            )

            deposit_receipt = self.handle_request(
                request_type=RequestType.COMPLETE_DEPOSIT,
                expected_status=200,
                se_iri=receipt.edit,
            )
        except DisseminationError as error:
            # Report failure, and delete the partially-created item
            logging.error(error)
            try:
                deletion_receipt = self.handle_request(
                    request_type=RequestType.DELETE_ITEM,
                    expected_status=200,
                    resource_iri=receipt.edit,
                )
            except DisseminationError as deletion_error:
                logging.error('Failed to delete incomplete item: {}'.format(deletion_error))
            sys.exit(1)
        except:
            # Unexpected failure. Let program crash, but still need to delete item
            try:
                deletion_receipt = self.handle_request(
                    request_type=RequestType.DELETE_ITEM,
                    expected_status=200,
                    resource_iri=receipt.edit,
                )
            except DisseminationError as deletion_error:
                logging.error('Failed to delete incomplete item: {}'.format(deletion_error))
            raise

        logging.info(
            'Successfully uploaded to SWORD v2 at {}'.format(receipt.location))

    def handle_request(self, request_type, expected_status, **kwargs):
        try:
            request_receipt = self.send_request(
                request_type=request_type, **kwargs)
        except sword2.exceptions.Forbidden:
            raise DisseminationError(
                'Could not connect to SWORD v2 server: authorisation failed')
        except sword2.HTTPResponseError as error:
            raise DisseminationError(
                'Could not connect to SWORD v2 server (status code {})'.format(error.response['status']))

        if request_receipt.code != expected_status:
            # Placeholder for error message
            request_contents = 'item'
            if request_type == RequestType.CREATE_ITEM:
                request_contents = 'item data'
            elif request_type == RequestType.UPLOAD_PDF:
                request_contents = 'PDF file'
            elif request_type == RequestType.UPLOAD_METADATA:
                request_contents = 'metadata file'
            raise DisseminationError(
                'Error uploading {} to SWORD v2'.format(request_contents))

        return request_receipt

    def send_request(self, request_type, **kwargs):
        if request_type == RequestType.CREATE_ITEM:
            request_receipt = self.conn.create(
                col_iri='https://dspace7-back.lib.cam.ac.uk/server/swordv2/collection/1810/339712',
                in_progress=True,
                # Required kwargs: metadata_entry
                **kwargs,
            )
        elif request_type == RequestType.UPLOAD_PDF:
            request_receipt = self.conn.add_file_to_resource(
                # Filename TBD: use work ID for now
                filename='{}.pdf'.format(self.work_id),
                mimetype='application/pdf',
                in_progress=True,
                # Required kwargs: edit_media_iri, payload
                **kwargs,
            )
        elif request_type == RequestType.UPLOAD_METADATA:
            request_receipt = self.conn.add_file_to_resource(
                # Filename TBD: use work ID for now
                filename='{}.json'.format(self.work_id),
                mimetype='application/json',
                in_progress=True,
                # Required kwargs: edit_media_iri, payload
                **kwargs,
            )
        elif request_type == RequestType.COMPLETE_DEPOSIT:
            request_receipt = self.conn.complete_deposit(
                # Required kwargs: se_iri (OR dr)
                **kwargs,
            )
        elif request_type == RequestType.DELETE_ITEM:
            request_receipt = self.conn.delete(
                # Required kwargs: resource_iri
                **kwargs,
            )
        else:
            raise NotImplementedError

        return request_receipt

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
