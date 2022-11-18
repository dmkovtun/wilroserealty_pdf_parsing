import logging
from typing import Iterator, Tuple

from PyPDF2 import PdfReader
# from PyPDF2 import PdfReadError
import pypdfium2 as pdfium

logger = logging.getLogger(__name__.split(".")[-1])




def get_pdf_content(filename: str, start_from: int = 0, boundaries: dict={}) -> Iterator[str]:
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
            width, height = page.get_size()
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

