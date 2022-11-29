import logging
from collections import defaultdict


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

        is_valid_result = self.is_valid_result(extracted_rows)
        if not extracted_rows or not is_valid_result:
            self.logger.info("Will try to get data with another parsing method")

            funcs = [v for k, v in processing_funcs.items() if k != file_type]
            # Overcome for empty result
            for func in funcs:
                self.logger.debug(f"Running func: {func.__name__}")
                try:
                    extracted_rows = func(filename)
                    is_valid_result = self.is_valid_result(extracted_rows)
                    if not is_valid_result:
                        continue

                    if extracted_rows:
                        break
                except Exception:
                    pass

        return extracted_rows

    def _convert_to_string(self, value):
        if isinstance(value, list):
            return " ".join(value)
        return str(value)

    def is_valid_result(self, value_dict: dict) -> bool:
        return True
