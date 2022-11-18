from typing import Any, Dict, Iterator, List
from scrapy import Spider, Request

from utils.case import Case
from scrapy.http import Response
from playwright.sync_api import sync_playwright
from playwright._impl._api_types import Error as PWError
from random import randint
from os.path import exists, join
from os import makedirs

from utils.case_status import CaseStatus
from utils.get_url_hash import get_url_hash


class CaseStatusSpider(Spider):

    name = "case_status"
    cases: List[Case] = []

    file_field_type_mapping = {
        "url_attorney": "csv",
        # TODO url_petition
        "url_schedule_a_b": "pdf",
        "url_schedule_d": "pdf",
        "url_schedule_e_f": "pdf",
        "url_top_twenty": "pdf",
    }

    def __init__(self, cases: List[Case]):
        self.cases: List[Case] = cases
        pass

    def start_requests(self):
        for case in self.cases:
            yield from self.process_case(case)

    def get_page_html_playwright(self, url: str, selectors_to_wait_for: list = []):
        with sync_playwright() as p:
            browser_type = p.chromium
            # for browser_type in [p.chromium, p.firefox, p.webkit]:
            browser = browser_type.launch(headless=False)
            context = browser.new_context(**self.get_new_context_params())
            page = context.new_page()

            page.goto(url)
            # try:
            #     page.wait_for_selector('button[value="Verify you are human"]')
            # except:
            #     self.logger.debug('Did not receive robot check page')

            for selector in selectors_to_wait_for:
                try:
                    page.wait_for_selector(selector)
                except Exception as err:
                    self.logger.error(f"Failed to await for selector {selector}")
                    page_title = page.title()
                    if "just a moment" in page_title.lower():
                        raise RuntimeError(f"Failed to bypass website security") from err

            page.wait_for_load_state("domcontentloaded")
            html = page.content()
            # cookies = context.cookies()
            browser.close()
            return html

    def get_new_context_params(self) -> Dict[str, Any]:
        return {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "viewport": {"width": 1920, "height": 1080},
            "locale": "en-US",
            "timezone_id": "America/New_York",  # TODO rotate?
            "geolocation": {
                "longitude": 34.022003 + (randint(-50, 50) / 10),
                "latitude": -84.361549 + (randint(-50, 50) / 10),
            },
        }

    def download_file_pw(self, case: Case, field_name: str):
        filename = self.get_full_filename(case, field_name)
        with sync_playwright() as p:
            browser_type = p.chromium
            browser = browser_type.launch(headless=False)

            context = browser.new_context(**self.get_new_context_params())
            page = context.new_page()
            page = context.new_page()  # Required

            with page.expect_download() as download_info:
                try:
                    page.goto(getattr(case, field_name))
                except PWError:
                    # NOTE: This is correct, error will be always raised
                    pass

                download = download_info.value
                self.logger.info(download.path())
                download.save_as(filename)

            context.close()
            browser.close()
        return filename

    def process_case(self, case: Case):
        for case in self.cases:
            try:
                full_html = self.get_page_html_playwright(case.url_case_link, [".card-header"])
                is_dismissed = self.check_is_dismissed(full_html)
                case.case_status = CaseStatus.dismissed if is_dismissed else CaseStatus.active
                filename = self.download_file_pw(case, "url_attorney")
                case.files["url_attorney"] = filename
                # case.additional_files = [filepath]
            except Exception as err:
                self.logger.error("Got an exception while enriching case")
                case.case_status = CaseStatus.processing_failed
                # No need to process more
                return []

        yield from self.download_pdf_files(case)

    def download_pdf_files(self, case: Case) -> Iterator[Request]:
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
                self.logger.warning(f"Case is missing '{name}' file url")
        else:
            return []

    def parse_downloaded_file(self, response: Response, case: Case, file_field: str):
        filename = self.get_full_filename(case, file_field)
        with open(filename, "wb") as outp:
            outp.write(response.body)
        case.files[file_field] = filename
        self.logger.info(f"Case N: Received file '{file_field}'")

    def get_full_filename(self, case, file_field):
        file_storage = self.settings.get("STORAGE_DIR")

        full_file_type_dirname = join(file_storage, file_field)
        if not exists(full_file_type_dirname):
            makedirs(full_file_type_dirname)

        _filename = "".join(letter for letter in getattr(case, file_field) if letter.isalnum())
        filename = _filename.split(" ", maxsplit=1)[-1]
        filename = get_url_hash(filename)
        full_path = join(full_file_type_dirname, filename)
        file_type = self.file_field_type_mapping[file_field]
        return f"{full_path}.{file_type}"

    def check_is_dismissed(self, full_html):
        reasons = ["DISMISSED", "Dismissed for Other Reason", "Debtor dismissed"]
        for reason in reasons:
            if reason in full_html:
                return True
        return False
