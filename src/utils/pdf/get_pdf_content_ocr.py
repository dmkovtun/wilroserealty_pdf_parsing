from typing import Iterator

from pikepdf import Pdf, PdfImage
from PIL import Image
import pytesseract
from pdf2image import convert_from_path

# Store Pdf with convert_from_path function
from os.path import join, dirname
from os import getcwd

# TODO poetry add pytesseract
# TODO poetry add pikepdf
# TODO sudo apt-get install tesseract-ocr

# pytesseract.pytesseract.tesseract_cmd = "C:/Program Files/Tesseract-OCR/tesseract.exe"


# https://github.com/UB-Mannheim/tesseract/wiki
import os

from pytesseract import pytesseract
from utils.pdf.get_pdf_content import get_pdf_content

from utils.pdf.is_text_file import is_text_file
import logging
from scrapy.utils.project import get_project_settings

# from .remove_empty_lines import remove_empty_lines

# TODO make this in a more usable way

logger = logging.getLogger(__name__)

settings = get_project_settings()

POPPLER_PATH = settings.get("POPPLER_PATH")
TESSERACT_PATH = settings.get("TESSERACT_PATH")
PDF_TEMP_DIR_PATH = str(settings.get("PDF_TEMP_DIR_PATH"))


if os.name == "nt":
    pytesseract.tesseract_cmd = TESSERACT_PATH
else:
    raise RuntimeError("Your OS is currently not supported")


def get_pdf_content_ocr(filename: str, boundaries: dict={}) -> Iterator[str]:
    """
        Reads images inside PDF file. Returns text from them.

    Args:
        filename: Name of PDF file which contains images.

    Returns:
        Iterator[str]: Yields str text for each PDF page.
    """
    logger.info(f'is_text_file(filename) {is_text_file(filename)}')
    if is_text_file(filename):
        yield from get_pdf_content(filename,boundaries=boundaries)
    else:
        yield from _process_scanned_pdf(filename)

def _process_scanned_pdf(filename: str) -> Iterator[str]:
    pdf_file = Pdf.open(filename)
    for i, page in enumerate(pdf_file.pages):
        for j, (name, raw_image) in enumerate(page.images.items()):
            image = PdfImage(raw_image)
            fname = f"{filename}-page{i:03}-img{j:03}"
            file_path = join(PDF_TEMP_DIR_PATH, fname)
            file_path = image.extract_to(fileprefix=file_path)

            denoised_image = _denoise_image(file_path)

            # file = Image.open(output_filename)
            yield from _generate_text_from_images(denoised_image)

def _denoise_image(file_path: str):
    import cv2
    image_bw = cv2.imread(file_path)
    # dst_name = file_path + '_noiseless.tif'
    # cv2.imwrite(dst_name, image_bw)
    # kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1,2))
    # morphology_img = cv2.morphologyEx(image_bw, cv2.MORPH_OPEN, kernel,iterations=1)
    # # plt.imshow(,'Greys_r')
    # cv2.imwrite(dst_name , morphology_img)

    # convert to grayscale
    gray = cv2.cvtColor(image_bw, cv2.COLOR_BGR2GRAY)

    # blur
    blur = cv2.GaussianBlur(gray, (0,0), sigmaX=5, sigmaY=5)
    #blur = cv2.GaussianBlur(gray, (0,0), sigmaX=5, sigmaY=5)

    # divide
    divide = cv2.divide(gray, blur, scale=255)

    # otsu threshold
    thresh = cv2.threshold(divide, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)[1]

    # apply morphology
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3,3))
    morph = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)

    # write result to disk
    cv2.imwrite(file_path +"_morph.jpg", morph)
    updated_file = file_path +"_morph.jpg"
    os.remove(file_path)

    return updated_file


    # noiseless_image_bw

def _generate_text_from_images(file_path: str) -> Iterator[str]:
    data = pytesseract.image_to_string(file_path, lang="eng", config="-c preserve_interword_spaces=1")
    # TODO UNCOMMENT
    # os.remove(file_path)
    yield from data.splitlines()
