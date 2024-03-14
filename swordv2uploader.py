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


class MetadataProfile(Enum):
    BASIC = 1
    JISC_ROUTER = 2
    # RIOXX = 3
    CUL_PILOT = 4


class SwordV2Uploader(Uploader):
    """Dissemination logic for SWORD v2"""

    def __init__(self, work_id, export_url, client_url, version, user_name_string, user_pass_string,
                 service_document_iri, collection_iri, metadata_profile):
        """Create connection to SWORD v2 endpoint"""
        super().__init__(work_id, export_url, client_url, version)
        try:
            user_name = self.get_credential_from_env(
                user_name_string, 'SWORD v2')
            user_pass = self.get_credential_from_env(
                user_pass_string, 'SWORD v2')
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)
        self.api = SwordV2Api(work_id, user_name, user_pass,
                              service_document_iri, collection_iri)
        self.metadata_profile = metadata_profile

    def upload_to_platform(self):
        """Upload work in required format to SWORD v2"""

        # TODO SWORD2 is designed for deposit rather than retrieval, hence
        # there's no easy way to search existing items i.e. check for duplicates.
        # One option would be to 1) call get_resource() on the collection URL,
        # 2) extract all of the item URLs from the atom-xml response,
        # 3) call get_resource() on each URL in turn, and 4) check that none
        # of the atom-xml responses contained the relevant `thoth-work-id`,
        # but this would be cumbersome.

        # Include full work metadata file in JSON format,
        # as a supplement to filling out SWORD2 metadata fields.
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
            create_receipt = self.api.create_item(sword_metadata)
        except DisseminationError as error:
            logging.error(error)
            sys.exit(1)

        # Any failure after this point will leave incomplete data in
        # SWORD v2 server which will need to be removed.
        try:
            self.api.upload_pdf(create_receipt.edit_media, pdf_bytes)
            self.api.upload_metadata(create_receipt.edit_media, metadata_bytes)
            deposit_receipt = self.api.complete_deposit(create_receipt.edit)
        except Exception as error:
            # In all cases, we need to delete the partially-created item
            # For expected failures, log before attempting deletion, then just exit
            # For unexpected failures, attempt deletion then let program crash
            if isinstance(error, DisseminationError):
                logging.error(error)
            try:
                self.api.delete_item(create_receipt.edit)
            except DisseminationError as deletion_error:
                logging.error(
                    'Failed to delete incomplete item: {}'.format(deletion_error))
            if isinstance(error, DisseminationError):
                sys.exit(1)
            else:
                raise

        logging.info(
            # If automatic deposit (no curation) is enabled, `alternate` should show
            # the front-end URL of the upload (alternatively, use `location` for back-end URL)
            'Successfully uploaded to SWORD v2 at {}'.format(deposit_receipt.alternate))

    def parse_metadata(self):
        """Convert work metadata into SWORD v2 format"""
        # Select the desired metadata profile
        if self.metadata_profile == MetadataProfile.BASIC:
            sword_metadata = self.profile_basic()
        elif self.metadata_profile == MetadataProfile.JISC_ROUTER:
            sword_metadata = self.profile_jisc_router()
        # elif self.metadata_profile == MetadataProfile.RIOXX:
            # sword_metadata = self.profile_rioxx()
        elif self.metadata_profile == MetadataProfile.CUL_PILOT:
            sword_metadata = self.profile_cul_pilot()
        else:
            raise NotImplementedError

        return sword_metadata

    def profile_cul_pilot(self):
        """
        Richer, customised, non-standard metadata profile developed in
        discussion with CUL (requiring configuration work on their side)
        """
        work_metadata = self.metadata.get('data').get('work')
        cul_pilot_metadata = sword2.Entry(
            # All fields are non-mandatory and any None values are ignored on ingest
            # (within Apollo DSpace 7 - yet to test other SWORD2-based platforms)
            # Some of the below fields do not appear to be stored/
            # correctly displayed by Apollo, although they are valid within SWORD2
            title=work_metadata.get('fullTitle'),
            dcterms_type=work_metadata.get('workType'),
            dcterms_publisher=self.get_publisher_name(),
            dcterms_issued=work_metadata.get('publicationDate'),
            dcterms_description=work_metadata.get('longAbstract'),
            dcterms_identifier="doi:{}".format(work_metadata.get('doi')),
            dcterms_license=work_metadata.get('license'),
            dcterms_tableOfContents=work_metadata.get('toc'),
        )
        # Workaround for adding repeatable fields
        for contribution in [n for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            first_name = contribution.get('firstName')
            orcid = contribution.get('contributor').get('orcid')
            contributor_string = contribution.get('fullName') if first_name is None else "{}, {}".format(
                contribution.get('lastName'), first_name)
            if orcid is not None:
                contributor_string += ' [orcid: {}]'.format(orcid)
            cul_pilot_metadata.add_field(
                "dcterms_contributor", contributor_string)
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            cul_pilot_metadata.add_field("dcterms_subject", subject)
        for isbn in [n.get('isbn').replace(
                '-', '') for n in work_metadata.get('publications') if n.get('isbn') is not None]:
            cul_pilot_metadata.add_field(
                "dcterms_identifier", "isbn:{}".format(isbn))
        for language in [n.get('languageCode') for n in work_metadata.get('languages')]:
            cul_pilot_metadata.add_field("dcterms_language", language)
        cul_pilot_metadata.add_field(
            "dcterms_identifier", "thoth-work-id:{}".format(self.work_id))

        return cul_pilot_metadata

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
            dcterms_created="{}T00:00:00Z".format(
                work_metadata.get('publicationDate')),
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
        basic_metadata.add_field("dcterms_identifier",
                                 work_metadata.get('doi'))
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
        basic_metadata.add_field("dcterms_identifier",
                                 "thoth-work-id:{}".format(self.work_id))

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
            dcterms_rights="License for VoR version of this work: {}".format(
                work_metadata.get('license')),
            dcterms_description="Publication status: {}".format(
                work_metadata.get('workStatus')),

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
            jisc_router_metadata.add_field(
                "dcterms_identifier", "isbn: {}".format(isbn))
        for subject in [n.get('subjectCode') for n in work_metadata.get('subjects')]:
            jisc_router_metadata.add_field("dcterms_subject", subject)
        for contribution in [n for n in work_metadata.get('contributions') if n.get('mainContribution') == True]:
            first_name = contribution.get('firstName')
            orcid = contribution.get('contributor').get('orcid')
            affiliations = contribution.get('affiliations')
            first_institution = next((a.get('institution').get(
                'institutionName') for a in affiliations if affiliations), None)
            contributor_string = contribution.get('fullName') if first_name is None else "{}, {}".format(
                contribution.get('lastName'), first_name)
            if orcid is not None:
                contributor_string += '; orcid: {}'.format(orcid)
            if first_institution is not None:
                contributor_string += '; {}'.format(first_institution)
            contribution_type = contribution.get('contributionType')
            if contribution_type == 'AUTHOR':
                jisc_router_metadata.add_field(
                    "dcterms_creator", contributor_string)
            else:
                jisc_router_metadata.add_field("dcterms_contributor", "{}: {}".format(
                    contribution_type, contributor_string))
        jisc_router_metadata.add_field("dcterms_rights", "Embargo: none")
        jisc_router_metadata.add_field(
            "dcterms_description", "Work version: VoR")
        jisc_router_metadata.add_field(
            "dcterms_description", "From {} via Thoth".format(self.get_publisher_name()))
        for funding in work_metadata.get('fundings'):
            funding_string = "Funder: {}".format(
                funding.get('institution').get('institutionName'))
            ror = funding.get('institution').get('ror')
            doi = funding.get('institution').get('institutionDoi')
            grant_number = funding.get('grantNumber')
            # Schema states comma-separated but actual Publications Router data is semicolon-separated
            if ror is not None:
                funding_string += "; ror: {}".format(ror)
            elif doi is not None:
                funding_string += "; doi: {}".format(doi)
            if grant_number is not None:
                funding_string += "; Grant(s): {}".format(grant_number)
            jisc_router_metadata.add_field(
                "dcterms_description", funding_string)
        jisc_router_metadata.add_field(
            "dcterms_identifier", "thoth-work-id:{}".format(self.work_id))

        return jisc_router_metadata


class SwordV2Api:

    def __init__(self, work_id, user_name, user_pass, service_document_iri, collection_iri):
        """Set up connection to API."""
        self.work_id = work_id
        self.collection_iri = collection_iri
        self.conn = sword2.Connection(
            service_document_iri=service_document_iri,
            user_name=user_name,
            user_pass=user_pass,
            # SWORD2 library doesn't handle timeout-related errors gracefully and large files
            # (e.g. 50MB) can't be fully uploaded within the 30-second default timeout.
            # Allow lots of leeway. (This otherwise matches the default `http_impl`.)
            http_impl=sword2.http_layer.HttpLib2Layer(timeout=120.0)
        )

    def create_item(self, metadata_entry):
        """Create an item with the specified metadata."""
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
        """Delete the specified item."""
        return self.handle_request(
            request_type=RequestType.DELETE_ITEM,
            expected_status=204,
            resource_iri=resource_iri,
        )

    def upload_pdf(self, edit_media_iri, payload):
        """Upload the supplied PDF file (as bytes) under the specified item."""
        return self.handle_request(
            request_type=RequestType.UPLOAD_PDF,
            expected_status=201,
            edit_media_iri=edit_media_iri,
            payload=payload,
        )

    def upload_metadata(self, edit_media_iri, payload):
        """Upload the supplied JSON metadata file (as bytes) under the specified item."""
        return self.handle_request(
            request_type=RequestType.UPLOAD_METADATA,
            expected_status=201,
            edit_media_iri=edit_media_iri,
            payload=payload,
        )

    def complete_deposit(self, se_iri):
        """Publish the specified item."""
        return self.handle_request(
            request_type=RequestType.COMPLETE_DEPOSIT,
            expected_status=200,
            se_iri=se_iri,
        )

    def handle_request(self, request_type, expected_status, **kwargs):
        """Call through to API and handle any errors."""
        try:
            request_receipt = self.send_request(
                request_type=request_type, **kwargs)
        except sword2.exceptions.Forbidden:
            # Error object doesn't contain anything useful to the user
            raise DisseminationError(
                'Could not connect to SWORD v2 server: authorisation failed')
        except sword2.HTTPResponseError as error:
            raise DisseminationError(
                'Could not connect to SWORD v2 server (status code {})'.format(error.response['status']))

        # Receipt may not contain useful information
        if request_receipt.code != expected_status:
            if request_type == RequestType.CREATE_ITEM:
                raise DisseminationError(
                    'Error uploading item data to SWORD v2')
            elif request_type == RequestType.UPLOAD_PDF:
                raise DisseminationError(
                    'Error uploading PDF file to SWORD v2')
            elif request_type == RequestType.UPLOAD_METADATA:
                raise DisseminationError(
                    'Error uploading metadata file to SWORD v2')
            elif request_type == RequestType.COMPLETE_DEPOSIT:
                raise DisseminationError('Error publishing item to SWORD v2')
            elif request_type == RequestType.DELETE_ITEM:
                raise DisseminationError('Error deleting item from SWORD v2')
            else:
                raise DisseminationError('Error uploading item to SWORD v2')

        return request_receipt

    def send_request(self, request_type, **kwargs):
        """Build appropriate request and send to API."""
        if request_type == RequestType.CREATE_ITEM:
            request_receipt = self.conn.create(
                col_iri=self.collection_iri,
                in_progress=True,
                # This sets `dc.identifier.slug` i.e. suggested URI -
                # but not supported in DSpace by default.
                # suggested_identifier=self.work_id,
                # Required kwargs: metadata_entry
                **kwargs,
            )
        elif request_type == RequestType.UPLOAD_PDF:
            request_receipt = self.conn.add_file_to_resource(
                filename='{}.pdf'.format(self.work_id),
                mimetype='application/pdf',
                in_progress=True,
                # Required kwargs: edit_media_iri, payload
                **kwargs,
            )
        elif request_type == RequestType.UPLOAD_METADATA:
            request_receipt = self.conn.add_file_to_resource(
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
