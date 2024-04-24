#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to
Cambridge University Library DSpace
"""

from dspaceuploader import DSpaceUploader, MetadataProfile


class CULUploader(DSpaceUploader):
    def __init__(self, work_id, export_url, client_url, version):
        """Set CUL-specific parameters and pass them to DSpaceUploader"""
        user_name_string = 'cam_ds7_user'
        user_pass_string = 'cam_ds7_pw'
        service_document_iri = (
            'https://copim-b-dev.lib.cam.ac.uk/server/swordv2/servicedocument'
        )
        collection_iri = (
            'https://copim-b-dev.lib.cam.ac.uk/server/swordv2/collection/1811/'
            '7'
        )
        metadata_profile = MetadataProfile.JISC_ROUTER
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

    def upload_to_platform(self):
        (publication_id, pdf_upload_receipt, deposit_receipt) = super().upload_to_platform()
        landing_page = deposit_receipt.alternate
        bitstream_id = pdf_upload_receipt.location.partition(
            '/bitstream/')[2].partition('/')[0]
        if len(bitstream_id) > 0:
            full_text_url = 'https://copim-f-dev.lib.cam.ac.uk/bitstreams/{}/download'.format(bitstream_id)
        else:
            full_text_url = None
        location_platform = 'OTHER'
        print(publication_id, location_platform, landing_page, full_text_url)
