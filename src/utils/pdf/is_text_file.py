import logging
import pypdfium2 as pdfium

logger = logging.getLogger(__name__)


def is_text_file(filename: str, threshold: int = 128) -> bool:
    try:
        pdf = pdfium.PdfDocument(filename)
        page = pdf[0]
        textpage = page.get_textpage()
        all_page_text = textpage.get_text_range()
        has_text = len(all_page_text) > threshold
        for garbage in (textpage, page, pdf):
            garbage.close()

        return has_text

    except IOError:
        error = f"File '{filename}' does not exist"
        logger.warning(error)
    return False
