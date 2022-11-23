from typing import Iterator, List
from scrapy import Spider, Request

from utils.case import Case
from scrapy.http import Response

from os.path import exists, join
from os import makedirs

from utils.get_url_hash import get_url_hash


class CaseStatusSpider(Spider):

    name = "case_status_spider"
    cases: List[Case] = []

    file_field_type_mapping = {
        "url_attorney": "csv",
        # TODO url_petition
        "url_schedule_a_b": "pdf",
        "url_schedule_d": "pdf",
        # TODO (not required right now)
        # "url_schedule_e_f": "pdf",
        # "url_top_twenty": "pdf",
    }

    def __init__(self, cases: List[Case]):
        self.cases: List[Case] = cases

    def start_requests(self):
        for case in self.cases:
            yield from self.process_case(case)

    def process_case(self, case: Case):
        """Sets case status, downloads required files"""
        yield from self.download_pdf_files(case)

    def download_pdf_files(self, case: Case) -> Iterator[Request]:
        """Download PDF files, which do not require browser emulation"""
        # NOTE: url_attorney will be downloaded via playwright
        file_urls = ["url_schedule_a_b", "url_schedule_d"]

        for name in file_urls:
            url = getattr(case, name)
            if url:
                yield Request(
                    url,
                    self.parse_downloaded_file,
                    cb_kwargs={"case": case, "file_field": name},
                    meta={"dont_merge_cookies": True},
                )
            else:
                self.logger.warning(f"Case '{case.case_number}': Case is missing '{name}' file url")
        else:
            return []

    def parse_downloaded_file(self, response: Response, case: Case, file_field: str):
        filename = self.get_full_filename(case, file_field)
        with open(filename, "wb") as outp:
            outp.write(response.body)
        case.files[file_field] = filename
        self.logger.debug(f"Case '{case.case_number}': Received file '{file_field}'")

    def get_full_filename(self, case, file_field):
        file_storage = self.settings.get("TEMP_DIR_PATH")

        full_file_type_dirname = join(file_storage, file_field)
        if not exists(full_file_type_dirname):
            makedirs(full_file_type_dirname)

        _filename = "".join(letter for letter in getattr(case, file_field) if letter.isalnum())
        filename = _filename.split(" ", maxsplit=1)[-1]
        filename = get_url_hash(filename)
        full_path = join(full_file_type_dirname, filename)
        file_type = self.file_field_type_mapping[file_field]
        return f"{full_path}.{file_type}"
