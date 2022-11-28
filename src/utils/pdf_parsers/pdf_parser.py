import json
import logging
import re
from collections import OrderedDict, defaultdict
from itertools import groupby, islice, zip_longest

import pdfplumber
from scrapy.utils.project import get_project_settings

from temp_pdf_sampler_d import get_pages_where_found
from utils.get_parsed_address import get_parsed_address
from utils.pdf.get_pdf_content import get_pdf_content_fitz, get_pdf_content_pdfium
from utils.pdf.get_pdf_content_from_text_ocr import get_pdf_content_from_text_ocr
from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
from utils.pdf.is_text_file import is_text_file


class PdfParser:
    cases_by_file_type = defaultdict(list)
    cases_by_file_type_ab = defaultdict(list)

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__.split(".")[-1])

    def parse_pdf_file(self, filename, processing_funcs, type_discover_func, **kwargs):
        file_type = type_discover_func(filename)
        self.logger.debug(f"Processing file_type {file_type}")
        extracted_rows = {}
        try:
            _processing_func = processing_funcs[file_type]
            try:
                extracted_rows = _processing_func(filename)
            except Exception as err:
                self.logger.error(
                    f"Processing failed for file_type: {file_type}, file {filename}, error: {str(err)}"
                )
                import traceback

                self.logger.error(traceback.format_exc())

        except KeyError as ke:
            self.logger.error(
                f"Missing processing function for file_type: {file_type}, file {filename}, error: {ke}"
            )

        if not extracted_rows:
            self.logger.info("Will try to get data with another parsing method")
            try:
                funcs = [v for k, v in processing_funcs.items() if k != file_type]
                # Overcome for empty result
                for func in funcs:
                    self.logger.debug(f"Running func: {func}")
                    extracted_rows = func(filename)
                    if extracted_rows:
                        break
            except Exception:
                # This file was not expected to be parsed correctly with random func
                pass

        return extracted_rows

    def _convert_to_string(self, value):
        if isinstance(value, list):
            return " ".join(value)
        return str(value)
