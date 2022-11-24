import logging
import pypdfium2 as pdfium

logger = logging.getLogger(__name__.split(".")[-1])


def get_pages_where_found(filename, search_text: list = []):
    found_pages = []
    try:
        pdf = pdfium.PdfDocument(filename)
        for index in range(0, len(pdf)):
            page = pdf[index]
            textpage = page.get_textpage()
            for part in search_text:
                searcher = textpage.search(part, match_case=False, match_whole_word=False)
                first_occurrence = searcher.get_next()
                if first_occurrence:
                    found_pages.append(index)
                searcher.close()
            for garbage in (textpage, page):
                garbage.close()

        pdf.close()
    except IOError:
        error = f"File '{filename}' does not exist"
        logger.error(error)
    return list(set(found_pages))
