#!/usr/bin/env python3
"""
Retrieve and disseminate files and metadata to
OAPEN DSpace using SWORD protocol
"""

from dspaceuploader import DSpaceUploader, MetadataProfile
from uploader import Location


class OAPENSWORDUploader(DSpaceUploader):
    """Dissemination logic for OAPEN SWORD"""

    def __init__(self, work_id, export_url, client_url, version):
        """Set OAPEN-specific parameters and pass them to DSpaceUploader"""
        user_name_string = 'oapen_sword_user'
        user_pass_string = 'oapen_sword_pw'
        service_document_iri = ("https://admin.oapen-dev.siscern.org/swordv2/servicedocument")
        collection_iri = ("https://admin.oapen-dev.siscern.org/swordv2/collection/20.500.12657/97099")
        metadata_profile = MetadataProfile.OAPEN
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
        """Perform standard upload then OAPEN-specific processing"""
        (publication_id, pdf_upload_receipt,
         deposit_receipt) = super().upload_to_platform()
        # Return details of created upload to be entered as a Thoth Location
        landing_page = deposit_receipt.alternate
        # Receipt only contains SWORDv2 server URL - translate to frontend URL
        # bitstream_id = pdf_upload_receipt.location.partition(
        #     '/bitstream/')[2].partition('/')[0]
        # if len(bitstream_id) > 0:
        #     full_text_url = ('https://thoth-arch.lib.cam.ac.uk/bitstreams/{}/'
        #                      'download'.format(bitstream_id))
        # else:
        #     full_text_url = None
        # location_platform = 'OTHER'
        full_text_url = None
        location_platform = 'OTHER'
        return [Location(publication_id, location_platform, landing_page,
                         full_text_url)]
        # pass
