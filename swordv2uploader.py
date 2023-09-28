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
        self.api = SwordV2Api(work_id, user_name, user_pass)

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
            receipt = self.api.create_item(sword_metadata)
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Any failure after this point will leave incomplete data in
        # SWORD v2 server which will need to be removed.
        try:
            self.api.upload_pdf(receipt.edit_media, pdf_bytes)
            self.api.upload_metadata(receipt.edit_media, metadata_bytes)
            self.api.complete_deposit(receipt.edit)
        except Exception as error:
            # In all cases, we need to delete the partially-created item
            # For expected failures, log before attempting deletion, then just exit
            # For unexpected failures, attempt deletion then let program crash
            if isinstance(error, DisseminationError):
                logging.error(error)
            try:
                self.api.delete_item(receipt.edit)
            except DisseminationError as deletion_error:
                logging.error('Failed to delete incomplete item: {}'.format(deletion_error))
            if isinstance(error, DisseminationError):
                sys.exit(1)
            else:
                raise

        logging.info(
            'Successfully uploaded to SWORD v2 at {}'.format(receipt.location))

    def parse_metadata(self):
        """Convert work metadata into SWORD v2 format"""
        # Select the desired metadata profile
        # TODO make this a switch passed in from higher-level specific platform class
        sword_metadata = self.profile_basic()
        # sword_metadata = self.profile_rioxx()
        # sword_metadata = self.profile_cul_pilot()

        return sword_metadata

    def profile_cul_pilot(self):
        """
        Richer, customised, non-standard metadata profile developed in
        discussion with CUL (requiring configuration work on their side)
        """
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

    def profile_basic(self):
        """
        Metadata profile based on DSpace configuration defaults i.e. SWORD profile
        See https://github.com/DSpace/DSpace/blob/main/dspace/config/modules/swordv2-server.cfg
        """
        work_metadata = self.metadata.get('data').get('work')
        basic_metadata = sword2.Entry(
            # swordv2-server.simpledc.abstract
            # swordv2-server.atom.summary
            dc_description_abstract=work_metadata.get('longAbstract'),
            # swordv2-server.simpledc.description
            dc_description=work_metadata.get('longAbstract'),
            # swordv2-server.simpledc.accessRights
            # swordv2-server.simpledc.rights
            # swordv2-server.simpledc.rightsHolder
            # swordv2-server.atom.rights
            dc_rights=work_metadata.get('license'),
            # swordv2-server.simpledc.available
            dc_date_available=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.created
            # swordv2-server.atom.published
            # swordv2-server.atom.updated
            dc_date_created=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.date
            dc_date=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.issued
            dc_date_issued=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.publisher
            dc_publisher=self.get_publisher_name(),
            # swordv2-server.simpledc.coverage
            dc_coverage='open access',
            # swordv2-server.simpledc.spatial
            dc_coverage_spatial='global',
            # swordv2-server.simpledc.temporal
            dc_coverage_temporal='all time',
            # swordv2-server.simpledc.title
            # swordv2-server.atom.title
            dc_title=work_metadata.get('fullTitle'),
            # swordv2-server.simpledc.type
            # "Recommended practice is to use a controlled vocabulary such as the DCMI Type Vocabulary"
            # (see https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#section-7)
            dc_type='text',

            # TODO Thoth stores relations/references but not currently retrieved by Thoth Client:
            # # swordv2-server.simpledc.isPartOf
            # dc_relation_ispartof=
            # # swordv2-server.simpledc.isReplacedBy
            # dc_relation_isreplacedby=
            # # swordv2-server.simpledc.references
            # dc_relation_references=
            # # swordv2-server.simpledc.relation
            # dc_relation=
            # # swordv2-server.simpledc.replaces
            # dc_relation_replaces=

            # Not appropriate as we may be submitting multiple formats (PDF, XML etc):
            # # swordv2-server.simpledc.extent
            # dc_format_extent=
            # # swordv2-server.simpledc.format
            # dc_format=
            # # swordv2-server.simpledc.medium
            # dc_format_medium=

            # Not supported by Thoth:
            # # swordv2-server.simpledc.alternative
            # dc_title_alternative=
            # # swordv2-server.simpledc.bibliographicCitation
            # dc_identifier_citation=
            # # swordv2-server.simpledc.dateAccepted
            # dc_date_accepted=
            # # swordv2-server.simpledc.dateSubmitted
            # dc_date_submitted=
            # # swordv2-server.simpledc.isReferencedBy
            # dc_relation_isreferencedby=
            # "A related resource that requires the described resource to support its function, delivery, or coherence"
            # # swordv2-server.simpledc.isRequiredBy
            # dc_relation_isrequiredby=
            # # swordv2-server.simpledc.modified
            # dc_date_modified=
            # # swordv2-server.simpledc.provenance
            # dc_description_provenance=
            # "A related resource that is required by the described resource to support its function, delivery, or coherence"
            # # swordv2-server.simpledc.requires
            # dc_relation_requires=
            # "A related resource from which the described resource is derived"
            # (e.g. print version of scan); most cases would be covered by dc_relation fields
            # # swordv2-server.simpledc.source
            # dc_source=
        )

        for contributor in [n.get('fullName') for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            # swordv2-server.simpledc.contributor
            basic_metadata.add_field("dc_contributor", contributor)
            # swordv2-server.simpledc.creator
            # swordv2-server.atom.author
            basic_metadata.add_field("dc_contributor_author", contributor)
        # swordv2-server.simpledc.identifier
        basic_metadata.add_field("dc_identifier", work_metadata.get('doi'))
        for isbn in [n.get('isbn').replace(
                '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]:
            basic_metadata.add_field("dc_identifier", isbn)
        for language in [n.get('languageCode') for n in work_metadata.get('languages')]:
            # swordv2-server.simpledc.language
            basic_metadata.add_field("dc_language", language)
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            # swordv2-server.simpledc.subject
            basic_metadata.add_field("dc_subject", subject)

        return basic_metadata


class SwordV2Api:

    def __init__(self, work_id, user_name, user_pass):
        self.work_id = work_id
        self.conn = sword2.Connection(
            service_document_iri="https://copim-b-dev.lib.cam.ac.uk/server/swordv2/servicedocument",
            user_name=user_name,
            user_pass=user_pass,
            # SWORD2 library doesn't handle timeout-related errors gracefully and large files
            # (e.g. 50MB) can't be fully uploaded within the 30-second default timeout.
            # Allow lots of leeway. (This otherwise matches the default `http_impl`.)
            http_impl=sword2.http_layer.HttpLib2Layer(timeout=120.0)
        )

    def create_item(self, metadata_entry):
        return self.handle_request(
            request_type=RequestType.CREATE_ITEM,
            expected_status=201,
            # Hacky workaround for an issue with mishandling of encodings within sword2 library,
            # which meant metadata containing special characters could not be submitted.
            # Although the `metadata_entry` parameter ought to be of type `Entry`, sending a
            # `str` as below triggers no errors. Ultimately it's passed to `http/client.py/_encode()`,
            # which defaults to encoding it as 'latin-1'. Pre-emptively encoding/decoding it here
            # seems to mean that the string sent to the server is in correct utf-8 format.
            metadata_entry=str(metadata_entry).encode(
                'utf-8').decode('latin-1'),
        )

    def delete_item(self, resource_iri):
        return self.handle_request(
            request_type=RequestType.DELETE_ITEM,
            expected_status=200,
            resource_iri=resource_iri,
        )

    def upload_pdf(self, edit_media_iri, payload):
        return self.handle_request(
            request_type=RequestType.UPLOAD_PDF,
            expected_status=201,
            edit_media_iri=edit_media_iri,
            payload=payload,
        )

    def upload_metadata(self, edit_media_iri, payload):
        return self.handle_request(
            request_type=RequestType.UPLOAD_METADATA,
            expected_status=201,
            edit_media_iri=edit_media_iri,
            payload=payload,
        )

    def complete_deposit(self, se_iri):
        return self.handle_request(
            request_type=RequestType.COMPLETE_DEPOSIT,
            expected_status=200,
            se_iri=se_iri,
        )

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
                col_iri="https://copim-b-dev.lib.cam.ac.uk/server/swordv2/collection/1811/7",
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
