import unittest
from unittest.mock import patch, MagicMock
from thothlibrary import ThothError
from types import SimpleNamespace


def _make_thoth_error(text):
    req = SimpleNamespace(method='POST', url='http://example.com/graphql')
    rsp = SimpleNamespace(status_code=400, text=text)
    return ThothError(req, rsp)


class TestWriteLocations(unittest.TestCase):

    @patch('write_locations.get_thoth_client')
    @patch('write_locations.environ')
    def test_duplicate_location_is_skipped(self, mock_environ, mock_get_thoth):
        """Test case 9: duplicate-platform ThothError is treated as successful skip."""
        mock_environ.__getitem__.return_value = 'fake-token'
        mock_thoth = MagicMock()
        mock_get_thoth.return_value = mock_thoth
        mock_thoth.create_location.side_effect = _make_thoth_error(
            "A location on the selected platform already exists."
        )

        from write_locations import write_thoth_location

        with patch('write_locations.logging') as mock_logging:
            result = write_thoth_location(
                "pub-1", "OAPEN",
                "https://library.oapen.org/handle/123",
                "https://library.oapen.org/bitstream/handle/123/file.pdf",
                None, None
            )

        self.assertIsNone(result)
        mock_logging.info.assert_called_once()
        self.assertIn("already exists", mock_logging.info.call_args[0][0])

    @patch('write_locations.get_thoth_client')
    @patch('write_locations.environ')
    def test_unrelated_thoth_error_raises(self, mock_environ, mock_get_thoth):
        """Test case 10: non-duplicate ThothError still fails."""
        mock_environ.__getitem__.return_value = 'fake-token'
        mock_thoth = MagicMock()
        mock_get_thoth.return_value = mock_thoth
        mock_thoth.create_location.side_effect = _make_thoth_error(
            "Some other error occurred"
        )

        from write_locations import write_thoth_location

        with self.assertRaises(ThothError):
            write_thoth_location(
                "pub-1", "OAPEN",
                "https://library.oapen.org/handle/123",
                "https://library.oapen.org/bitstream/handle/123/file.pdf",
                None, None
            )

    @patch('write_locations.get_thoth_client')
    @patch('write_locations.environ')
    def test_successful_write_prints_location_id(self, mock_environ, mock_get_thoth):
        """Successful location creation prints the location ID."""
        mock_environ.__getitem__.return_value = 'fake-token'
        mock_thoth = MagicMock()
        mock_get_thoth.return_value = mock_thoth
        mock_thoth.create_location.return_value = "loc-123"

        from write_locations import write_thoth_location

        with patch('builtins.print') as mock_print:
            write_thoth_location(
                "pub-1", "OAPEN",
                "https://library.oapen.org/handle/123",
                "https://library.oapen.org/bitstream/handle/123/file.pdf",
                None, None
            )

        mock_print.assert_called_once_with("loc-123")


if __name__ == '__main__':
    unittest.main()
