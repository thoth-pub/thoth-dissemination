#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to Cambridge University Library DSpace
"""

from dspaceuploader import DSpaceUploader, MetadataProfile


class CULUploader(DSpaceUploader):
    def __init__(self, work_id, export_url, client_url, version):
        """Set CUL-specific parameters and pass them to DSpaceUploader"""
        user_name_string = 'cam_ds7_user'
        user_pass_string = 'cam_ds7_pw'
        service_document_iri = 'https://copim-b-dev.lib.cam.ac.uk/server/swordv2/servicedocument'
        collection_iri = 'https://copim-b-dev.lib.cam.ac.uk/server/swordv2/collection/1811/7'
        metadata_profile = MetadataProfile.JISC_ROUTER
        super().__init__(work_id, export_url, client_url, version, user_name_string, user_pass_string,
                         service_document_iri, collection_iri, metadata_profile)
