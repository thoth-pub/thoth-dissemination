#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to a DSpace repository
"""

from swordv2uploader import SwordV2Uploader, MetadataProfile


class DSpaceUploader(SwordV2Uploader):
    # Currently DSpace dissemination is only implemented via SWORDv2
    # This class will be extended once we implement e.g. DSpace REST API
    def __init__(
            self,
            work_id,
            export_url,
            client_url,
            version,
            user_name_string,
            user_pass_string,
            service_document_iri,
            collection_iri,
            metadata_profile):
        super().__init__(
            work_id,
            export_url,
            client_url,
            version,
            user_name_string,
            user_pass_string,
            service_document_iri,
            collection_iri,
            metadata_profile)
