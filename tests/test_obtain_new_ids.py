import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace


def make_location(platform):
    return SimpleNamespace(locationPlatform=platform)


def make_publication(pub_id, pub_type, locations=None):
    return SimpleNamespace(
        publicationId=pub_id,
        publicationType=pub_type,
        locations=locations or [],
    )


def make_work(work_id, doi, publications):
    return SimpleNamespace(
        workId=work_id,
        doi=doi,
        publications=publications,
    )


class TestOapenLocationsPostProcess(unittest.TestCase):

    def setUp(self):
        patcher = patch('obtain_new_ids.get_thoth_client')
        self.mock_get_thoth = patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_thoth = MagicMock()
        self.mock_get_thoth.return_value = self.mock_thoth

        from obtain_new_ids import OapenLocationsIDFinder
        self.finder = OapenLocationsIDFinder()

    def test_missing_both_platforms(self):
        """Test case 1: PDF with neither OAPEN nor DOAB produces ["OAPEN", "DOAB"]."""
        pdf_pub = make_publication("pub-1", "PDF", [])
        work = make_work("work-1", "https://doi.org/10.1234/test", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-1"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-1")
        self.assertEqual(doi, "10.1234/test")
        self.assertEqual(missing, ["OAPEN", "DOAB"])

    def test_missing_doab_only(self):
        """Test case 3: PDF with OAPEN but no DOAB produces ["DOAB"] only."""
        locations = [make_location("OAPEN")]
        pdf_pub = make_publication("pub-2", "PDF", locations)
        work = make_work("work-2", "https://doi.org/10.1234/test2", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-2"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-2")
        self.assertEqual(missing, ["DOAB"])

    def test_missing_oapen_only(self):
        """Test case 2: PDF with DOAB but no OAPEN produces ["OAPEN"] only."""
        locations = [make_location("DOAB")]
        pdf_pub = make_publication("pub-3", "PDF", locations)
        work = make_work("work-3", "https://doi.org/10.1234/test3", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-3"]

        self.finder.post_process()

        self.assertEqual(len(self.finder.thoth_ids), 1)
        pub_id, doi, missing = self.finder.thoth_ids[0]
        self.assertEqual(pub_id, "pub-3")
        self.assertEqual(missing, ["OAPEN"])

    def test_both_present_excluded(self):
        """Test case 4: PDF with both locations is excluded."""
        locations = [make_location("OAPEN"), make_location("DOAB")]
        pdf_pub = make_publication("pub-4", "PDF", locations)
        work = make_work("work-4", "https://doi.org/10.1234/test4", [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-4"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])

    def test_no_pdf_publication_skipped(self):
        """Non-PDF publications are skipped."""
        epub_pub = make_publication("pub-5", "EPUB", [])
        work = make_work("work-5", "https://doi.org/10.1234/test5", [epub_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-5"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])

    def test_no_doi_skipped(self):
        """Works without DOI are skipped even if locations missing."""
        pdf_pub = make_publication("pub-6", "PDF", [])
        work = make_work("work-6", None, [pdf_pub])
        self.mock_thoth.work_by_id.return_value = work
        self.finder.thoth_ids = ["work-6"]

        self.finder.post_process()

        self.assertEqual(self.finder.thoth_ids, [])


if __name__ == '__main__':
    unittest.main()
