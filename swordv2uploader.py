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
        # sword_metadata = self.profile_jisc_router()
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
            dcterms_abstract=work_metadata.get('longAbstract'),
            # swordv2-server.simpledc.description
            dcterms_description=work_metadata.get('longAbstract'),
            # swordv2-server.simpledc.accessRights
            # swordv2-server.simpledc.rights
            # swordv2-server.simpledc.rightsHolder
            # swordv2-server.atom.rights
            dcterms_rights=work_metadata.get('license'),
            # swordv2-server.simpledc.available
            dcterms_available=work_metadata.get('publicationDate'),
            # Needs to be sent in datetime format (otherwise causes error 500),
            # but is retrieved as `str` so can just append required elements
            # swordv2-server.simpledc.created
            # swordv2-server.atom.published
            # swordv2-server.atom.updated
            dcterms_created="{}T00:00:00Z".format(work_metadata.get('publicationDate')),
            # swordv2-server.simpledc.date
            dcterms_date=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.issued
            dcterms_issued=work_metadata.get('publicationDate'),
            # swordv2-server.simpledc.publisher
            dcterms_publisher=self.get_publisher_name(),
            # swordv2-server.simpledc.coverage
            dcterms_coverage='open access',
            # swordv2-server.simpledc.spatial
            dcterms_spatial='global',
            # swordv2-server.simpledc.temporal
            dcterms_temporal='all time',
            # swordv2-server.simpledc.title
            # swordv2-server.atom.title
            dcterms_title=work_metadata.get('fullTitle'),
            # swordv2-server.simpledc.type
            # "Recommended practice is to use a controlled vocabulary such as the DCMI Type Vocabulary"
            # (see https://www.dublincore.org/specifications/dublin-core/dcmi-terms/#section-7)
            dcterms_type='text',
            #^should we Include the dcmitype namespace for this?

            # Not appropriate as we may be submitting multiple formats (PDF, XML etc):
            # # swordv2-server.simpledc.extent
            # dcterms_format_extent=
            # # swordv2-server.simpledc.format
            # dcterms_format=
            # # swordv2-server.simpledc.medium
            # dcterms_format_medium=

            # Not supported by Thoth:
            # # swordv2-server.simpledc.alternative
            # dcterms_title_alternative=
            # # swordv2-server.simpledc.bibliographicCitation
            # dcterms_identifier_citation=
            # # swordv2-server.simpledc.dateAccepted
            # dcterms_date_accepted=
            # # swordv2-server.simpledc.dateSubmitted
            # dcterms_date_submitted=
            # # swordv2-server.simpledc.isReferencedBy
            # dcterms_relation_isreferencedby=
            # "A related resource that requires the described resource to support its function, delivery, or coherence"
            # # swordv2-server.simpledc.isRequiredBy
            # dcterms_relation_isrequiredby=
            # # swordv2-server.simpledc.modified
            # dcterms_date_modified=
            # # swordv2-server.simpledc.provenance
            # dcterms_description_provenance=
            # "A related resource that is required by the described resource to support its function, delivery, or coherence"
            # # swordv2-server.simpledc.requires
            # dcterms_relation_requires=
            # "A related resource from which the described resource is derived"
            # (e.g. print version of scan); most cases would be covered by dc_relation fields
            # # swordv2-server.simpledc.source
            # dcterms_source=
        )

        for contributor in [n.get('fullName') for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            # swordv2-server.simpledc.contributor
            basic_metadata.add_field("dcterms_contributor", contributor)
            # swordv2-server.simpledc.creator
            # swordv2-server.atom.author
            # Doesn't seem to work; not clear how to represent "dc.contributor.author" in dcterms
            basic_metadata.add_field("dcterms_author", contributor)
        # swordv2-server.simpledc.identifier
        basic_metadata.add_field("dcterms_identifier", work_metadata.get('doi'))
        for isbn in [n.get('isbn').replace(
                '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]:
            basic_metadata.add_field("dcterms_identifier", isbn)
        for language in [n.get('languageCode') for n in work_metadata.get('languages')]:
            # swordv2-server.simpledc.language
            basic_metadata.add_field("dcterms_language", language)
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            # swordv2-server.simpledc.subject
            basic_metadata.add_field("dcterms_subject", subject)
        for (relation_type, relation_doi) in [(n.get('relationType'), n.get('relatedWork').get('doi'))
                for n in work_metadata.get('relations')]:
            if relation_type == 'IS_PART_OF' or relation_type == 'IS_CHILD_OF':
                # swordv2-server.simpledc.isPartOf
                basic_metadata.add_field("dcterms_isPartOf", relation_doi)
            elif relation_type == 'IS_REPLACED_BY':
                # swordv2-server.simpledc.isReplacedBy
                basic_metadata.add_field("dcterms_isReplacedBy", relation_doi)
            elif relation_type == 'REPLACES':
                # swordv2-server.simpledc.replaces
                basic_metadata.add_field("dcterms_replaces", relation_doi)
            else:
                # swordv2-server.simpledc.relation
                basic_metadata.add_field("dcterms_relation", relation_doi)
        for (reference_citation, reference_doi) in [(n.get('unstructuredCitation'), n.get('doi'))
                for n in work_metadata.get('references')]:
            # will always have one or the other (if not both)
            reference = reference_citation if reference_citation else reference_doi
            # swordv2-server.simpledc.references
            basic_metadata.add_field("dcterms_references", reference)

        return basic_metadata

    def profile_jisc_router(self):
        """
        Metadata profile based on Jisc Publications Router schema (currently articles-only)
        See https://github.com/jisc-services/Public-Documentation/blob/master/PublicationsRouter/sword-out/DSpace-XML.md
        """
        work_metadata = self.metadata.get('data').get('work')
        jisc_router_metadata = sword2.Entry(
            dcterms_publisher=self.get_publisher_name(),
            dcterms_title=work_metadata.get('fullTitle'),
            dcterms_abstract=work_metadata.get('longAbstract'),
            dcterms_identifier="doi: {}".format(work_metadata.get('doi')),
            dcterms_issued=work_metadata.get('publicationDate'),
            dcterms_rights="License for VoR version of this work: {}".format(work_metadata.get('license')),
            dcterms_description="Publication status: {}".format(work_metadata.get('workStatus')),

            # Not supported by Thoth:
            # dcterms_bibliographicCitation=
            # dcterms_dateAccepted=
            # "A related resource from which the described resource is derived"
            # Jisc Router uses this to indicate the article's parent journal; not relevant for books
            # dcterms_source=
        )

        for language in [n.get('languageCode') for n in work_metadata.get('languages')]:
            jisc_router_metadata.add_field("dcterms_language", language)
        for isbn in [n.get('isbn').replace(
                '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]:
            jisc_router_metadata.add_field("dcterms_identifier", "isbn: {}".format(isbn))
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            jisc_router_metadata.add_field("dcterms_subject", subject)
        for contribution in [n for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            first_name = contribution.get('firstName')
            orcid = contribution.get('contributor').get('orcid')
            affiliations = contribution.get('affiliations')
            first_institution = next((a.get('institution').get('institutionName') for a in affiliations if affiliations), None)
            contributor_string = contribution.get('fullName') if first_name is None else "{}, {}".format(contribution.get('lastName'), first_name)
            if orcid is not None:
                contributor_string += '; orcid: {}'.format(orcid)
            if first_institution is not None:
                contributor_string += '; {}'.format(first_institution)
            contribution_type = contribution.get('contributionType')
            if contribution_type == 'AUTHOR':
                jisc_router_metadata.add_field("dcterms_creator", contributor_string)
            else:
                jisc_router_metadata.add_field("dcterms_contributor", "{}: {}".format(contribution_type, contributor_string))
        jisc_router_metadata.add_field("dcterms_rights", "Embargo: none")
        jisc_router_metadata.add_field("dcterms_description", "Work version: VoR")
        jisc_router_metadata.add_field("dcterms_description", "From {} via Thoth".format(self.get_publisher_name()))
        for funding in work_metadata.get('fundings'):
            funding_string = "Funder: {}".format(funding.get('institution').get('institutionName'))
            ror = funding.get('institution').get('ror')
            doi = funding.get('institution').get('institutionDoi')
            grant_number = funding.get('grantNumber')
            if ror is not None:
                funding_string += ", ror: {}".format(ror)
            elif doi is not None:
                funding_string += ", doi: {}".format(doi)
            if grant_number is not None:
                funding_string += ", Grant(s): {}".format(grant_number)
            jisc_router_metadata.add_field("dcterms_description", funding_string)

        return jisc_router_metadata


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
