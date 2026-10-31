"""
Download a PDF source in one bounded request and check that it is a sound PDF
before any platform receives it
"""

import io
import re
from urllib.parse import urlparse

import pikepdf
import requests

from errors import DisseminationError


# A PDF source that is larger, or stalls for longer, is held rather than truncated
PDF_MAX_BYTES = 512 * 1024 * 1024
PDF_TIMEOUT = (10, 90)
PDF_CHUNK_BYTES = 1024 * 1024
# The browser-like agent the shared downloader sends, which some publisher hosts require
PDF_SOURCE_USER_AGENT = ('Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36')


class PDFCheckError(DisseminationError):
    """The PDF source could not be downloaded completely, or is not a sound PDF."""


def describe_pdf_problem(problem):
    """Shorten a parser message, naming the file rather than its in-memory buffer."""
    text = re.sub(r'stream <_io\.BytesIO object at 0x[0-9a-f]+>', 'PDF', str(problem))
    text = ' '.join(text.split())
    return text if len(text) <= 200 else text[:197] + '...'


def check_pdf(data):
    """
    Return the page count of a complete, unencrypted PDF whose structure,
    streams and page contents all parse without problems, or raise PDFCheckError.
    """
    if not data:
        raise PDFCheckError('the file is empty')
    # PDF readers accept a header anywhere in the first 1024 bytes
    if b'%PDF-' not in data[:1024]:
        raise PDFCheckError('the file has no PDF header')
    try:
        # Without recovery, a damaged cross-reference table fails instead of being rebuilt
        with pikepdf.open(io.BytesIO(data), attempt_recovery=False) as pdf:
            if pdf.is_encrypted:
                raise PDFCheckError('the file is encrypted')
            pages = len(pdf.pages)
            if pages == 0:
                raise PDFCheckError('the file has no pages')
            # Decodes every stream, parses every page's content and reports parser warnings
            problems = pdf.check_pdf_syntax()
    except PDFCheckError:
        raise
    except pikepdf.PasswordError:
        raise PDFCheckError('the file is encrypted with a password') from None
    except pikepdf.PdfError as error:
        raise PDFCheckError('the file could not be read as a PDF: {}'.format(
            describe_pdf_problem(error))) from None
    except Exception as error:
        raise PDFCheckError('the file could not be checked ({})'.format(
            type(error).__name__)) from None
    if problems:
        raise PDFCheckError('the file has {} structural problem(s), first: {}'.format(
            len(problems), describe_pdf_problem(problems[0])))
    return pages


class PDFSource:
    """Download a PDF source in one bounded request, keeping its exact bytes."""

    def __init__(self, http_get=requests.get):
        self.http_get = http_get

    def fetch(self, url):
        """Return the complete body of a PDF source, or raise PDFCheckError."""
        # Errors name the host only: source URLs may carry signed tokens
        host = urlparse(url).hostname or 'the source host'
        try:
            # An uncompressed transfer keeps the size limit a limit on memory
            # and lets the received length be compared with Content-Length
            response = self.http_get(url, headers={'User-Agent': PDF_SOURCE_USER_AGENT,
                                                   'Accept-Encoding': 'identity'},
                                     timeout=PDF_TIMEOUT, stream=True)
        except requests.RequestException as error:
            raise PDFCheckError('the request to {} failed ({})'.format(
                host, type(error).__name__)) from None
        try:
            # Redirects are followed, so report where the body came from
            host = urlparse(response.url).hostname or host
            return self.read_body(response, host)
        except requests.RequestException as error:
            raise PDFCheckError('the download from {} failed ({})'.format(
                host, type(error).__name__)) from None
        finally:
            response.close()

    @staticmethod
    def read_body(response, host):
        if response.status_code != 200:
            raise PDFCheckError('{} answered HTTP status {}'.format(host, response.status_code))
        content_type = (response.headers.get('Content-Type') or '').split(';')[0].strip().lower()
        if content_type != 'application/pdf':
            raise PDFCheckError('{} served {} instead of application/pdf'.format(
                host, repr(content_type[:60]) if content_type else 'no content type'))
        declared = response.headers.get('Content-Length')
        if declared is not None:
            if not re.fullmatch(r'[0-9]+', declared.strip()):
                raise PDFCheckError('{} sent an invalid Content-Length'.format(host))
            declared = int(declared)
            if declared > PDF_MAX_BYTES:
                raise PDFCheckError('{} declared {} bytes, more than the {}-byte limit'.format(
                    host, declared, PDF_MAX_BYTES))
        chunks = []
        received = 0
        for chunk in response.iter_content(chunk_size=PDF_CHUNK_BYTES):
            chunks.append(chunk)
            received += len(chunk)
            if received > PDF_MAX_BYTES:
                raise PDFCheckError('{} sent more than the {}-byte limit'.format(
                    host, PDF_MAX_BYTES))
        # A compressed transfer arrives decoded, so only an unencoded body has a comparable length
        encoding = (response.headers.get('Content-Encoding') or 'identity').strip().lower()
        if declared is not None and encoding == 'identity' and received != declared:
            raise PDFCheckError('{} declared {} bytes but sent {}'.format(host, declared, received))
        return b''.join(chunks)
