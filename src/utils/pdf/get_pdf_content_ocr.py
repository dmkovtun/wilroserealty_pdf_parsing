import logging
import os
from os import getcwd
from os.path import basename, dirname, join
from typing import Iterator, List

import pytesseract
from pdf2image import convert_from_path
from pikepdf import Pdf, PdfImage
from PIL import Image
from pytesseract import pytesseract
from scrapy.utils.project import get_project_settings
from pikepdf.models.image import HifiPrintImageNotTranscodableError
# from utils.pdf.denoise_image import denoise_image

# https://github.com/UB-Mannheim/tesseract/wiki

logger = logging.getLogger(__name__)

settings = get_project_settings()

POPPLER_PATH = settings.get("POPPLER_PATH")
TESSERACT_PATH = settings.get("TESSERACT_PATH")
PDF_TEMP_DIR_PATH = str(settings.get("PDF_TEMP_DIR_PATH"))

#pytesseract.tesseract_cmd = TESSERACT_PATH


def convert_file_to_images_pike(filename: str) -> List[str]:
    pdf_file = Pdf.open(filename)
    image_files = []
    for i, page in enumerate(pdf_file.pages):

        for j, (name, raw_image) in enumerate(page.images.items()):
            image = PdfImage(raw_image)
            fname = f"{filename}-page{i:03}-img{j:03}"
            file_path = join(PDF_TEMP_DIR_PATH, fname)


            try:
                file_path = image.extract_to(fileprefix=file_path)
                image_files.append(file_path)
            except HifiPrintImageNotTranscodableError:
                return []
    return image_files


def convert_file_to_images_poppler(
    filename: str, bounding_func=None, dpi=200
) -> List[str]:
    image_files = []
    images = convert_from_path(filename, dpi=dpi, poppler_path=POPPLER_PATH)
    for i in range(len(images)):
        _orig_name = basename(filename)
        _full_name = join(PDF_TEMP_DIR_PATH, f"{_orig_name}_page_{str(i)}.jpg")
        images[i].save(_full_name, "JPEG")
        if bounding_func:
            image = Image.open(_full_name)
            title_image = bounding_func(image)
            title_image.save(_full_name, "JPEG")

        image_files.append(_full_name)

    return image_files


def get_pdf_content_ocr(
    filename: str, bounding_func, is_denoisable=False, dpi=200
) -> Iterator[str]:
    """
        Reads images inside PDF file. Returns text from them.

    Args:
        filename: Name of PDF file which contains images.

    Returns:
        Iterator[str]: Yields str text for each PDF page.
    """
    image_files = convert_file_to_images_poppler(filename, bounding_func, dpi=dpi)
    for file_path in image_files:
        # if is_denoisable:
        #    file_path = denoise_image(file_path)
        yield from _generate_text_from_images(file_path)


def _generate_text_from_images(file_path: str) -> Iterator[str]:
    # TODO PROFILE 1 PDF processing with threaded solution (multiple instances)
    data = pytesseract.image_to_string(
        file_path, lang="eng", config="-c preserve_interword_spaces=1"
        # TODO tessedit_do_invert=0
        #  engine="fast"
    )
    os.remove(file_path)
    yield data
