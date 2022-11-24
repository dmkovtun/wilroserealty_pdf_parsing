from typing import Iterator, Optional

from pikepdf import Pdf, PdfImage
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
from os.path import join, dirname, exists
from os import getcwd, remove, makedirs
from scrapy.utils.project import get_project_settings
from os.path import basename
from PIL import Image

from utils.pdf.get_pages_where_found import get_pages_where_found

settings = get_project_settings()
PDF_TEMP_DIR_PATH = str(settings.get("PDF_TEMP_DIR_PATH"))
POPPLER_PATH = str(settings.get("POPPLER_PATH"))

if not exists(PDF_TEMP_DIR_PATH):
    makedirs(PDF_TEMP_DIR_PATH)


def get_pdf_content_from_text_ocr(filename: str, bounding_func=None, search_text: list = []) -> Iterator[str]:
    images = convert_from_path(filename, poppler_path=POPPLER_PATH)

    if search_text:
        processable_pages = get_pages_where_found(filename, search_text)
    else:
        processable_pages = [i for i in range(len(images))]

    for i in range(len(images)):
        if i not in processable_pages:
            print(f"Skipping page {i} processing")
            continue
        _orig_name = basename(filename)
        _full_name = join(PDF_TEMP_DIR_PATH, f"{_orig_name}_page_{str(i)}.jpg")
        images[i].save(_full_name, "JPEG")

        if bounding_func:
            image = Image.open(_full_name)
            title_image = bounding_func(image)
            title_image.save(_full_name, "JPEG")

        data = pytesseract.image_to_string(_full_name, lang="eng", config="-c preserve_interword_spaces=1")
        remove(_full_name)
        yield data
    return
