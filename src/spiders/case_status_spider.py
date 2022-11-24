from typing import Iterator, List
from scrapy import Spider, Request

from utils.case import Case
from scrapy.http import Response

from utils.misc.get_full_filename import get_full_filename


class CaseStatusSpider(Spider):

    name = "case_status_spider"
    cases: List[Case] = []

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
        filename = get_full_filename(case, file_field)
        with open(filename, "wb") as outp:
            outp.write(response.body)
        case.files[file_field] = filename
        self.logger.debug(f"Case '{case.case_number}': Received file '{file_field}'")
