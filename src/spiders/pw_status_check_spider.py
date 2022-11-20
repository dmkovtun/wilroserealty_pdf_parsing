import logging
from typing import Any, Dict
from utils.case import Case
from playwright.sync_api import sync_playwright

from playwright._impl._api_types import Error as PWError
from random import randint
from os.path import exists, join
from os import makedirs

from utils.get_url_hash import get_url_hash

from scrapy.utils.project import get_project_settings


class PWStatusCheckSpider:
    def __init__(self) -> None:
        self.settings = get_project_settings()
        self.logger = logging.getLogger(__name__.split(".")[-1])

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

    def get_page_html_playwright(self, url: str, selectors_to_wait_for: list = []) -> str:
        """Gets full html of page"""
        with sync_playwright() as p:
            browser_type = p.chromium
            browser = browser_type.launch(headless=self.settings.get("PLAYWRIGHT_HEADLESS"))
            context = browser.new_context(**self.get_new_context_params())
            page = context.new_page()
            page.goto(url)
            # TODO more reliable way to bypass captcha|security page
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
            browser.close()
            return html

    def download_file_pw(self, case: Case, field_name: str) -> str:
        """Download logic for csv file which has closable tab"""
        filename = self.get_full_filename(case, field_name)
        with sync_playwright() as p:
            browser_type = p.chromium
            browser = browser_type.launch(headless=self.settings.get("PLAYWRIGHT_HEADLESS"))

            context = browser.new_context(**self.get_new_context_params())
            page = context.new_page()
            page = context.new_page()  # Required due to closable download page

            with page.expect_download() as download_info:
                try:
                    page.goto(getattr(case, field_name))
                except PWError:
                    # NOTE: This is correct, error will be always raised
                    pass

                download = download_info.value
                download.save_as(filename)

            context.close()
            browser.close()
        return filename

    def check_is_dismissed(self, full_html):
        reasons = ["DISMISSED", "Dismissed for Other Reason", "Debtor dismissed"]
        for reason in reasons:
            if reason in full_html:
                return True
        return False

    # TODO REMOVE AS DUPLICATE
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

    file_field_type_mapping = {
        "url_attorney": "csv",
        # TODO url_petition
        "url_schedule_a_b": "pdf",
        "url_schedule_d": "pdf",
        # TODO (not required right now)
        # "url_schedule_e_f": "pdf",
        # "url_top_twenty": "pdf",
    }
