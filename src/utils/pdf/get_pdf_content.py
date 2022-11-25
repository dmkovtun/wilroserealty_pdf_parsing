import logging
from typing import Iterator, Tuple

from PyPDF2 import PdfReader
# from PyPDF2 import PdfReadError
import pypdfium2 as pdfium

logger = logging.getLogger(__name__.split(".")[-1])




def get_pdf_content_fitz(filename: str) -> Iterator[str]:
    """Returns all text available in PDF file.

    Args:
        filename (str): Path to PDF file.

    Returns:
        Tuple[str, str]: Tuple of (content, error)
    """
    import fitz
    doc = fitz.open(filename)
    for page in doc:
        # do something with 'page'
        if page.search_for("55"):
            # "text": (default) plain text with line breaks. No formatting, no text position details, no images.
            # "blocks": generate a list of text blocks (= paragraphs).
            # "words": generate a list of words (strings not containing spaces).
            # "html": creates a full visual version of the page including any images. This can be displayed with your internet browser.
            # "dict" / "json": same information level as HTML, but provided as a Python dictionary or resp. JSON string. See TextPage.extractDICT() for details of its structure.
            # "rawdict" / "rawjson": a super-set of "dict" / "json". It additionally provides character detail information like XML. See TextPage.extractRAWDICT() for details of its structure.
            # "xhtml": text information level as the TEXT version but includes images. Can also be displayed by internet browsers.
            # "xml": contains no images, but full position and font information down to each single text character. Use an XML module to interpret.
            import json
            # logger.info(json.dumps(page.get_text('blocks'), indent=4))
            yield from [p[4] for p in page.get_text('blocks')]


def get_pdf_content_pdfium(filename: str, start_from: int = 0, boundaries: dict={}) -> Iterator[str]:
    """Returns all text available in PDF file.

    Args:
        filename (str): Path to PDF file.

    Returns:
        Tuple[str, str]: Tuple of (content, error)
    """
    try:
        pdf = pdfium.PdfDocument(filename)
        for index in range(start_from, len(pdf)):
            page = pdf[index]
            # width, height = page.get_size()
            # print(f'height {height}')
            textpage = page.get_textpage()
            #print(boundaries)
            text_part = textpage.get_text_bounded(**boundaries)
            yieldable_data = text_part.splitlines()
            # Attention: objects must be closed in correct order!
            for garbage in (textpage, page):
                garbage.close()
            yield from yieldable_data

        pdf.close()
    except IOError:
        error = f"File '{filename}' does not exist"
        logger.error(error)