#!/usr/bin/env python3
"""
Acquire a list of locations to be added to Thoth (in same format as output by disseminator.py).
Purpose: automate updating of Thoth records for platforms where location is not immediately
         returned as part of initial Project MUSE dissemination process.
"""
import csv
import logging
import json
import sys
import requests
from io import StringIO
from thothlibrary import ThothClient, ThothError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# thoth = ThothClient()

# MUSE_API_URL = 'https://about.muse.jhu.edu/lib/metadata?no_auth=1&format=kbart&content=book&collection_ids=2652'

# file = requests.get(MUSE_API_URL).text

# success = True

# try:
#     data = csv.DictReader(StringIO(file), delimiter="\t")
# except (ValueError, NameError):
#     raise ValueError('Project MUSE data not found, or not in expected tab-separated string format')

# locations = []
# for row in data:
#     try:
#         isbn = str(row['online_identifier']).strip()
#         publications = thoth.publications(search=isbn)
#         if len(publications) == 0:
#             logging.error(f"No publications found for ISBN {isbn}")
#             success = False
#             continue
#         # We may have submitted either PDF or EPUB or both - no way to check
#         # Assume that the set of publications remains unchanged since submission
#         # and add locations to all relevant publications accordingly
#         elif len(publications) > 1:
#             pdfs = [n for n in publications if n.publicationType == 'PDF']
#             epubs = [n for n in publications if n.publicationType == 'EPUB']
#             if not pdfs and not epubs:
#                 logging.error(f"No PDF or EPUB publications found for {isbn}")
#                 success = False
#                 continue
#             elif len(pdfs) > 1 or len(epubs) > 1:
#                 logging.error(f"Multiple publications of same type found for {isbn}")
#                 success = False
#                 continue
#         landing_page = row['title_url'].strip()
#     except KeyError:
#         raise ValueError('Project MUSE data missing expected column header')
#     except ThothError as e:
#         raise ValueError(f'Error connecting to Thoth: {e}')

#     for publication in publications:
#         if publication.publicationType == 'PDF':
#             full_text_url = '{}/pdf/download'.format(landing_page)
#         elif publication.publicationType == 'EPUB':
#             full_text_url = '{}/epub'.format(landing_page)
#         else:
#             continue
#         try:
#             # Check for any existing MUSE location, to detect inconsistencies
#             existing_landing_page = [n.landingPage for n in publication.locations
#                                      if n.locationPlatform == 'PROJECT_MUSE' and n.landingPage.strip()][0]
#             if existing_landing_page != landing_page:
#                 logging.error(f"Landing page {landing_page} given in data for {isbn}, but record already has " +
#                               f"landing page {existing_landing_page}")
#             else:
#                 logging.info(f"Landing page {landing_page} already present in record for {isbn} - skipping")
#             continue
#         except IndexError:
#             pass
#         locations.append('{} PROJECT_MUSE {} {} {} {}'.format(publication.publicationId, landing_page, full_text_url, None, None))

success = False
locations = ['216ac731-b34e-4a22-9c73-c994ea16b449 PROJECT_MUSE https://muse.jhu.edu/book/100418 https://muse.jhu.edu/book/100418/epub None None', 'c40a56c5-1db4-4455-a91f-5835346bf54e PROJECT_MUSE https://muse.jhu.edu/book/136409 https://muse.jhu.edu/book/136409/pdf/download None None', '3f6e7590-1bd5-4544-8d78-8f179cc8a60a PROJECT_MUSE https://muse.jhu.edu/book/136410 https://muse.jhu.edu/book/136410/pdf/download None None', '347dab92-78ce-46b0-a767-5ec9747b2047 PROJECT_MUSE https://muse.jhu.edu/book/136411 https://muse.jhu.edu/book/136411/pdf/download None None', 'c8ba8308-a0ba-4062-9416-57e65fa2ee00 PROJECT_MUSE https://muse.jhu.edu/book/136412 https://muse.jhu.edu/book/136412/pdf/download None None', '37b7d289-97db-4504-b330-cf01428e6ade PROJECT_MUSE https://muse.jhu.edu/book/66809 https://muse.jhu.edu/book/66809/pdf/download None None', 'c4bc7580-8580-434c-a4f2-5e53326c6bb4 PROJECT_MUSE https://muse.jhu.edu/book/66813 https://muse.jhu.edu/book/66813/pdf/download None None', '84bc3bff-549b-4a83-9d08-b6fe9a8af1cb PROJECT_MUSE https://muse.jhu.edu/book/66814 https://muse.jhu.edu/book/66814/pdf/download None None', '96a5e9b7-5cfd-4ef0-8777-73899be5629b PROJECT_MUSE https://muse.jhu.edu/book/66815 https://muse.jhu.edu/book/66815/pdf/download None None', '665788c8-868c-4784-8149-3ed4e3bfbfc9 PROJECT_MUSE https://muse.jhu.edu/book/66816 https://muse.jhu.edu/book/66816/pdf/download None None', 'df16dfba-ced1-42c4-9bf7-611c6ccec364 PROJECT_MUSE https://muse.jhu.edu/book/66817 https://muse.jhu.edu/book/66817/pdf/download None None', '79ba9d81-83c7-4f12-a118-58fcac76b349 PROJECT_MUSE https://muse.jhu.edu/book/66818 https://muse.jhu.edu/book/66818/pdf/download None None', 'e4877e64-5b15-4052-8acb-67c45731a9d5 PROJECT_MUSE https://muse.jhu.edu/book/66819 https://muse.jhu.edu/book/66819/pdf/download None None', '0aee050f-ab39-4a05-959b-65a6ab1ffb74 PROJECT_MUSE https://muse.jhu.edu/book/66820 https://muse.jhu.edu/book/66820/pdf/download None None', '0818837d-2f7f-4e38-8643-5c917e617b75 PROJECT_MUSE https://muse.jhu.edu/book/66821 https://muse.jhu.edu/book/66821/pdf/download None None', '63c0046d-ee23-4c37-a06d-654ada44cd28 PROJECT_MUSE https://muse.jhu.edu/book/66822 https://muse.jhu.edu/book/66822/pdf/download None None', 'e557e532-1126-420b-8bab-6bfee3708a25 PROJECT_MUSE https://muse.jhu.edu/book/66823 https://muse.jhu.edu/book/66823/pdf/download None None', '9f48a54d-860a-4777-8f0b-250016d90f2e PROJECT_MUSE https://muse.jhu.edu/book/75666 https://muse.jhu.edu/book/75666/pdf/download None None', 'f741cde5-f186-43bf-ab78-3ca36b00bb33 PROJECT_MUSE https://muse.jhu.edu/book/75672 https://muse.jhu.edu/book/75672/pdf/download None None', '463a9d2d-2a19-491c-b809-e5ddc79b953b PROJECT_MUSE https://muse.jhu.edu/book/76509 https://muse.jhu.edu/book/76509/pdf/download None None', '566a4bcb-09fc-4769-aaec-8805e3f2936a PROJECT_MUSE https://muse.jhu.edu/book/76511 https://muse.jhu.edu/book/76511/pdf/download None None', '6c45e861-4298-4088-8af7-ff3e49e6db8d PROJECT_MUSE https://muse.jhu.edu/book/80762 https://muse.jhu.edu/book/80762/pdf/download None None', '398e1483-8e44-4120-84aa-ce0019888e47 PROJECT_MUSE https://muse.jhu.edu/book/80764 https://muse.jhu.edu/book/80764/pdf/download None None', '91569c2b-f3a0-4163-9404-8c5ae8b49419 PROJECT_MUSE https://muse.jhu.edu/book/80766 https://muse.jhu.edu/book/80766/pdf/download None None', 'e3ec6987-8432-4770-b265-797ee1ea3236 PROJECT_MUSE https://muse.jhu.edu/book/80767 https://muse.jhu.edu/book/80767/pdf/download None None', '9cf8cb9e-2080-41ba-9ffd-885557d4afbc PROJECT_MUSE https://muse.jhu.edu/book/80768 https://muse.jhu.edu/book/80768/pdf/download None None', 'bcfb4ef3-4e3e-4187-a8c7-1443abe37c5a PROJECT_MUSE https://muse.jhu.edu/book/80770 https://muse.jhu.edu/book/80770/pdf/download None None', 'b5273b30-19a0-4787-bcf1-3b80285c6736 PROJECT_MUSE https://muse.jhu.edu/book/80771 https://muse.jhu.edu/book/80771/pdf/download None None', '2ce1910e-3f6e-454c-b9ea-ec9447604198 PROJECT_MUSE https://muse.jhu.edu/book/80774 https://muse.jhu.edu/book/80774/pdf/download None None', 'cca892a1-711f-4599-b6a1-91aea86d52c7 PROJECT_MUSE https://muse.jhu.edu/book/80832 https://muse.jhu.edu/book/80832/epub None None', '87c48404-eb27-4def-87e4-f34c3e17c842 PROJECT_MUSE https://muse.jhu.edu/book/80833 https://muse.jhu.edu/book/80833/epub None None', 'fabab9b4-2f84-4925-89ce-fc96045e8ecf PROJECT_MUSE https://muse.jhu.edu/book/81037 https://muse.jhu.edu/book/81037/pdf/download None None', '3acdd65c-05bd-472a-90d8-32ba6f55fda8 PROJECT_MUSE https://muse.jhu.edu/book/81373 https://muse.jhu.edu/book/81373/epub None None', '1a15612e-ee40-4e8e-8b36-6fbe6e002889 PROJECT_MUSE https://muse.jhu.edu/book/82857 https://muse.jhu.edu/book/82857/pdf/download None None', '72b6bd20-e17e-4a58-be19-d6cf100e928a PROJECT_MUSE https://muse.jhu.edu/book/82858 https://muse.jhu.edu/book/82858/pdf/download None None', '58c2de7a-1234-49a0-8aa1-70ee60caaafe PROJECT_MUSE https://muse.jhu.edu/book/82859 https://muse.jhu.edu/book/82859/pdf/download None None', '8a085ca3-fa40-4994-81d7-4ec709dc008d PROJECT_MUSE https://muse.jhu.edu/book/82860 https://muse.jhu.edu/book/82860/pdf/download None None', '82d6b392-77cf-4323-8f22-5f53765c9892 PROJECT_MUSE https://muse.jhu.edu/book/82879 https://muse.jhu.edu/book/82879/pdf/download None None', 'dc55c546-c442-4523-ac47-8726e50b8fd3 PROJECT_MUSE https://muse.jhu.edu/book/83354 https://muse.jhu.edu/book/83354/pdf/download None None', '05959029-e0a7-4daa-9960-3b00eaed64e3 PROJECT_MUSE https://muse.jhu.edu/book/83545 https://muse.jhu.edu/book/83545/pdf/download None None', '689af7b8-f987-45af-b9f2-4e6b07ed09c5 PROJECT_MUSE https://muse.jhu.edu/book/83818 https://muse.jhu.edu/book/83818/pdf/download None None', '7ee2b781-ed97-42d7-8c82-65cde71bd432 PROJECT_MUSE https://muse.jhu.edu/book/84167 https://muse.jhu.edu/book/84167/pdf/download None None', 'fe112efb-8204-42c4-a212-1e4d1b48bde2 PROJECT_MUSE https://muse.jhu.edu/book/84168 https://muse.jhu.edu/book/84168/pdf/download None None', 'd041543d-7801-4272-9c73-1eda245a1179 PROJECT_MUSE https://muse.jhu.edu/book/84169 https://muse.jhu.edu/book/84169/pdf/download None None', 'ffc32efd-f21a-42ee-a48a-097ab88f7d78 PROJECT_MUSE https://muse.jhu.edu/book/84171 https://muse.jhu.edu/book/84171/pdf/download None None', '7251bbe9-63a1-4f72-bbe2-76f2e5ec3c8e PROJECT_MUSE https://muse.jhu.edu/book/84172 https://muse.jhu.edu/book/84172/pdf/download None None', '510c24e1-cb12-4124-b538-43bb4dbdc4c2 PROJECT_MUSE https://muse.jhu.edu/book/84174 https://muse.jhu.edu/book/84174/pdf/download None None', '1a691d59-a383-49c7-95d7-434fe6d6b176 PROJECT_MUSE https://muse.jhu.edu/book/84175 https://muse.jhu.edu/book/84175/pdf/download None None', '7b001205-b0c6-4601-997a-709914a1f2a7 PROJECT_MUSE https://muse.jhu.edu/book/84176 https://muse.jhu.edu/book/84176/pdf/download None None', 'b6882e77-fb16-4333-baa2-5d51520186d4 PROJECT_MUSE https://muse.jhu.edu/book/84177 https://muse.jhu.edu/book/84177/pdf/download None None', '18dddf1b-8ea9-4f9d-84f0-119bf2e93ae2 PROJECT_MUSE https://muse.jhu.edu/book/84178 https://muse.jhu.edu/book/84178/pdf/download None None', '4b5ca8ad-bcf0-44e5-ac63-653c4a5cc06b PROJECT_MUSE https://muse.jhu.edu/book/84180 https://muse.jhu.edu/book/84180/pdf/download None None', 'f7663f23-cfe0-482a-b7fd-a94e4412e028 PROJECT_MUSE https://muse.jhu.edu/book/84181 https://muse.jhu.edu/book/84181/pdf/download None None', '35e95056-fbde-4711-8370-c269d7ca0bd4 PROJECT_MUSE https://muse.jhu.edu/book/84182 https://muse.jhu.edu/book/84182/pdf/download None None', 'e0e21eb4-dd93-4575-b6f8-a5597747b391 PROJECT_MUSE https://muse.jhu.edu/book/84183 https://muse.jhu.edu/book/84183/pdf/download None None', 'd5692737-5eff-4dfe-aa83-4339e6b838ee PROJECT_MUSE https://muse.jhu.edu/book/84184 https://muse.jhu.edu/book/84184/pdf/download None None', 'ae40a175-7ddd-4c1a-b20e-fddd1610fb80 PROJECT_MUSE https://muse.jhu.edu/book/84185 https://muse.jhu.edu/book/84185/pdf/download None None', 'aacd7b37-4735-44a5-a9a4-ded082fea251 PROJECT_MUSE https://muse.jhu.edu/book/84187 https://muse.jhu.edu/book/84187/pdf/download None None', 'b9c37a54-cebd-4185-8e5f-d4eaf77369b6 PROJECT_MUSE https://muse.jhu.edu/book/84188 https://muse.jhu.edu/book/84188/pdf/download None None', 'bc7ad1d6-8de4-457f-b71b-ec534fd17c38 PROJECT_MUSE https://muse.jhu.edu/book/84189 https://muse.jhu.edu/book/84189/pdf/download None None', '30ecb7f2-76c4-4633-b7ff-d244c59379e6 PROJECT_MUSE https://muse.jhu.edu/book/84190 https://muse.jhu.edu/book/84190/pdf/download None None', 'b783ac22-7c62-4e28-8d8d-c65fa302b074 PROJECT_MUSE https://muse.jhu.edu/book/84191 https://muse.jhu.edu/book/84191/pdf/download None None', 'f32ebd93-978c-4ee5-8296-5ea05fdf5879 PROJECT_MUSE https://muse.jhu.edu/book/84192 https://muse.jhu.edu/book/84192/pdf/download None None', '6ef995bf-0b54-4e0a-85e4-445bb57ebadd PROJECT_MUSE https://muse.jhu.edu/book/84193 https://muse.jhu.edu/book/84193/pdf/download None None', '62206e45-abdc-4f7c-a2a9-de69056be3c0 PROJECT_MUSE https://muse.jhu.edu/book/84194 https://muse.jhu.edu/book/84194/pdf/download None None', 'c0481494-c3dc-46ab-9598-2edec6539378 PROJECT_MUSE https://muse.jhu.edu/book/84195 https://muse.jhu.edu/book/84195/pdf/download None None', '9e8f7e32-02d6-4def-ac14-b485eadf6138 PROJECT_MUSE https://muse.jhu.edu/book/84196 https://muse.jhu.edu/book/84196/pdf/download None None', '8a4e85d8-a1b1-4de9-b5c1-8caa6404b59d PROJECT_MUSE https://muse.jhu.edu/book/84197 https://muse.jhu.edu/book/84197/pdf/download None None', '84b6b3e1-2811-4b6e-864a-ddb866ea1cf0 PROJECT_MUSE https://muse.jhu.edu/book/84303 https://muse.jhu.edu/book/84303/pdf/download None None', 'b2a7d1f4-adac-497b-b8f3-d12ed98ef46f PROJECT_MUSE https://muse.jhu.edu/book/84309 https://muse.jhu.edu/book/84309/pdf/download None None', 'e23c4c5e-5b4f-4482-989f-f610709fd9d7 PROJECT_MUSE https://muse.jhu.edu/book/84310 https://muse.jhu.edu/book/84310/pdf/download None None', '0ac0df2f-3a09-4c13-b480-a4043454856a PROJECT_MUSE https://muse.jhu.edu/book/84311 https://muse.jhu.edu/book/84311/pdf/download None None', '72fe1c58-76a2-4de9-9d37-81009c653a72 PROJECT_MUSE https://muse.jhu.edu/book/84312 https://muse.jhu.edu/book/84312/pdf/download None None', 'f6a1c015-3d73-4bf6-be77-dd6cc74b9c8a PROJECT_MUSE https://muse.jhu.edu/book/84313 https://muse.jhu.edu/book/84313/pdf/download None None', 'b7f32b2d-53a6-482a-b243-7b2bdf2f3fc2 PROJECT_MUSE https://muse.jhu.edu/book/84314 https://muse.jhu.edu/book/84314/pdf/download None None', '05877527-cff8-4d8c-82e8-65a03959405e PROJECT_MUSE https://muse.jhu.edu/book/84502 https://muse.jhu.edu/book/84502/pdf/download None None']

logging.info('List of locations found: {}'.format(locations))
print(json.dumps(locations))

# Prompt the user to review any errors. Some locations may still have been returned and need to
# be processed by subsequent GitHub Actions jobs, so they need to run regardless of failure here
if not success:
    logging.warning("Not all data could be processed. Please review errors logged above.")
    sys.exit(1)
