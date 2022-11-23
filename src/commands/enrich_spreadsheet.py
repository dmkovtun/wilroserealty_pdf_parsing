from itertools import zip_longest
import logging
import pickle
from typing import Iterator, List
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor, threads
from twisted.internet.defer import inlineCallbacks
from commands.base.base_command import BaseCommand
from spiders.pw_status_check_spider import PWStatusCheckSpider
from spiders.case_status_spider import CaseStatusSpider
from utils.case import Case
from utils.case_status import CaseStatus
import pandas as pd
from utils.get_parsed_address import get_parsed_address

from utils.google_sheets.google_sheets_client import GoogleSheetsClient
from twisted.internet.error import ReactorNotRunning
from scrapy.utils.project import get_project_settings
from os import remove
import json

from utils.pdf_parser.pdf_parser import PdfParser


class EnrichSpreadsheet(BaseCommand):
    """ """

    def __init__(self):
        super().__init__()
        self.project_settings = get_project_settings()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sheets_process: GoogleSheetsClient
        self.pdf_parser = PdfParser()

    def init(self):
        """Init method for all resource-consuming things"""
        settings = self.settings
        self.sheets_process = GoogleSheetsClient(settings.get("TOKEN_PATH"), settings.get("CREDENTIALS_PATH"))

    def _errback(self, failure):
        self.logger.error(failure)

    def add_options(self, parser) -> None:
        super().add_options(parser)
        # parser.add_option(
        #     "-f",
        #     "--file",
        #     dest="filepath",
        #     help="File with accounts. Should be json.",
        #     type="str",
        #     default="",
        # )

    def load_cases(self):
        sheets_process = self.sheets_process

        cases: List[Case] = []

        # TODO MAKE THIS NOT HARDCODED???
        case_numbers_list = sheets_process.load_all_rows_from_name("Case No")
        case_statuses_list = sheets_process.load_all_rows_from_name("Status")
        case_link_list = sheets_process.load_all_rows_from_name("[URL] Case Link")
        attorney_list = sheets_process.load_all_rows_from_name("[URL] Attorneys")
        petition_list = sheets_process.load_all_rows_from_name("[URL] Petition")
        schedule_a_b_list = sheets_process.load_all_rows_from_name("[URL] Schedule A/B")
        schedule_d_list = sheets_process.load_all_rows_from_name("[URL] Schedule D")
        schedule_e_f_list = sheets_process.load_all_rows_from_name("[URL] Schedule E/F")
        top_twenty_list = sheets_process.load_all_rows_from_name("[URL] Top Twenty")

        row_number = 2  # Header skipped
        for (
            case_number,
            case_status_original,
            case_link,
            attorney,
            petition,
            schedule_a_b,
            schedule_d,
            schedule_e_f,
            top_twenty,
        ) in zip_longest(
            case_numbers_list,
            case_statuses_list,
            case_link_list,
            attorney_list,
            petition_list,
            schedule_a_b_list,
            schedule_d_list,
            schedule_e_f_list,
            top_twenty_list,
        ):
            cases.append(
                Case(
                    row_number,
                    case_status_original,
                    case_number,
                    case_link,
                    attorney,
                    petition,
                    schedule_a_b,
                    schedule_d,
                    schedule_e_f,
                    top_twenty,
                )
            )
            row_number += 1
        return cases

    @inlineCallbacks
    def check_status_pw(self, case):
        # Google Sheet enrichment process in steps:
        # 1. Status value checking:
        # ~ 1.1 Check file from 'Case Link' (column A):
        # - If pdf contains 'dismissed' in top right corner - status should be set as 'Dismissed'. CONTINUE processing this row.
        # - Else: continue processing, status 'Active'
        self.logger.info(f"Case '{case.case_number}': Starting PW case status checking")
        spider = PWStatusCheckSpider()
        try:
            d = threads.deferToThread(spider.get_page_html_playwright, case.url_case_link, [".card-header"])
            full_html = yield d

            is_dismissed = spider.check_is_dismissed(full_html)
            case.case_status = CaseStatus.dismissed if is_dismissed else CaseStatus.active
            fd = threads.deferToThread(spider.download_file_pw, case, "url_attorney")
            filename = yield fd
            case.files["url_attorney"] = filename
        except Exception as err:
            self.logger.error(f"Case '{case.case_number}': Got an error while enriching case: {str(err)}")
            case.case_status = CaseStatus.processing_failed
            # No need to process more

        return case

    def filter_cases_by_orig_status(self, cases: List[Case]) -> List[Case]:
        skip_statuses_list = [CaseStatus.dismissed.value, CaseStatus.active.value]
        # TODO REMOVE??
        #skip_statuses_list.append(CaseStatus.processing_failed.value)
        # skip_statuses_list.append(CaseStatus.possible_failure.value)

        logging.getLogger("case_status_spider").setLevel("INFO")
        logging.getLogger("scrapy.core.engine").setLevel("INFO")

        new_cases = []
        for case in cases:
            if case.case_status_original in skip_statuses_list:
                continue
            new_cases.append(case)
        return new_cases

    def run(self, args, opts):
        self.args = args
        self.opts = opts

        is_debug_run = False
        # TODO THIS IS FOR DEBUG ONLY
        if is_debug_run:
            with open("debug_cases.pickle", "rb") as handle:
                cases = pickle.load(handle)

            processable_cases = ["9:22-bk-90090"]
            # processable_cases = [c.case_number for c in cases]

            required_cases = []
            for c in cases:
                if c.case_number in processable_cases:
                    required_cases.append(c)

            cases = required_cases

            for index, case in enumerate(cases):
                self.logger.info(f"Processing case {index+1} of {len(cases)}")
                if case.case_status == CaseStatus.processing_failed:
                    self.logger.debug("Skipped case processing, but will update status")
                # NOTE: This is not usual way
                self.process_files(case)
                # self.logger.debug(json.dumps(case.__dict__, indent=4, default=str))
                if case.case_status != CaseStatus.processing_failed:
                    self.update_case(case)
                else:
                    self.update_case_status(case)

                self.logger.debug(json.dumps(case.__dict__, indent=4, default=str))

        if not is_debug_run:
            cases: List[Case] = self.load_cases()
            # TODO REMOVE

            self.logger.info(f"Received {len(cases)} cases from Google Sheet")
            self.logger.info("Starting processing case statuses")
            # cases = self.filter_cases_by_orig_status(cases)
            # cases = cases[:1]

            # processable_cases = ["4:22-bk-31579"]
            processable_cases = [c.case_number for c in cases]

            required_cases = []
            for c in cases:
                if c.case_number in processable_cases:
                    required_cases.append(c)

            cases = required_cases

            self.logger.info(f"Will process {len(cases)} cases after filtering by status")
            self.process_cases(cases)

            if not cases:
                self.logger.info("Will skip next steps, as cases were filtered out")
                return

            reactor.run()

            with open("debug_cases.pickle", "wb") as handle:
                pickle.dump(cases, handle)

    def clear_cached_files(self, case: Case) -> None:
        for _, file_path in case.files.items():
            remove(file_path)
            self.logger.debug(f"Removed file {file_path}")

    def process_files(self, case: Case):
        # 2. Attorney emails fillup (columns AJ, AK)
        # Save only emails. First person - column AJ, others go to AK
        # Get data from column K (Attorneys): it has a csv file inside and contains same emails
        error_msgs = []
        try:

            try:
                attorney_email, other_attorney_emails = self.attorney_csv_parsing(case)
                case.enrichable_values["attorney_email"] = attorney_email
                case.enrichable_values["other_attorney_emails"] = other_attorney_emails
            except Exception as err:
                error_msgs.append(f"Attorneys.csv file parsing: {str(err)}")

            try:
                schedule_a_b_data = self.pdf_parser.schedule_a_b_parsing(case.files["url_schedule_a_b"])
                case.enrichable_values["schedule_a_b_rows"] = schedule_a_b_data
            except Exception as err:
                error_msgs.append(f"Schedule A/B parsing: {str(err)}")

            try:
                case.enrichable_values["addresses"] = self._process_addresses(case)
            except Exception as err:
                error_msgs.append(f"Address parsing: {str(err)}")

            try:
                schedule_d_data = self.pdf_parser.schedule_d_parsing(case.files["url_schedule_d"])
                case.enrichable_values["schedule_d_rows"] = schedule_d_data
            except Exception as err:
                error_msgs.append(f"Schedule D parsing: {str(err)}")

        except Exception as err:
            error_msgs.append(f"Case data parsing: {str(err)}")
        if error_msgs:
            all_msgs = "\n".join(error_msgs)
            self.logger.error(f"Case '{case.case_number}': Failed to process case: {all_msgs}")
            case.case_status = CaseStatus.processing_failed

    def _get_dict_formatted(self, case: Case, field_name: str) -> str:
        try:
            dict_data = case.enrichable_values[field_name]
            return "\n".join([f"{k}: {v}" for k, v in dict_data.items()])
        except KeyError as ke:
            return f"Failed to parse due to missing value: {field_name}"

    def _process_addresses(self, case: Case):
        if case.enrichable_values.get("schedule_a_b_rows"):
            addresses = [get_parsed_address(v[0]) for k, v in case.enrichable_values["schedule_a_b_rows"].items()]
            return "\n".join(addresses)
        return ""

    def _check_possible_failure(self, case: Case):
        addr = self._process_addresses(case)
        ab_data = self._get_dict_formatted(case, "schedule_a_b_rows")
        d_data = self._get_dict_formatted(case, "schedule_d_rows")
        if not all((addr, ab_data, d_data)):
            case.case_status = CaseStatus.possible_failure

    def _prepare_case_data(self, case: Case) -> List[str]:
        # "Status","Creditor Notes","Borrower Notes","Property Notes","ADDRESS","Attorney Email","Other Attorney Emails"
        self._check_possible_failure(case)

        _mapping = {
            "Status": case.case_status.value,
            "Creditor Notes": self._get_dict_formatted(case, "schedule_d_rows"),
            "Borrower Notes": "UNSUPPORTED",
            "Property Notes": self._get_dict_formatted(case, "schedule_a_b_rows"),
            "ADDRESS": case.enrichable_values["addresses"],
            "Attorney Email": case.enrichable_values["attorney_email"],
            "Other Attorney Emails": case.enrichable_values["other_attorney_emails"],
        }

        return [v for k, v in _mapping.items()]

    def update_case(self, case: Case):
        # Status","Creditor Notes","Borrower Notes","Property Notes","ADDRESS","Attorney Email","Other Attorney Emails
        self.logger.info(f"Case '{case.case_number}': Updating case rows")
        try:
            prepared_values = self._prepare_case_data(case)
            # TODO Ideally somehow define these values
            start_column = "Status"
            end_column = "Other Attorney Emails"

            self.sheets_process.update_values(case.case_row_number, start_column, end_column, [prepared_values])
        except Exception as err:
            self.logger.error(f"Case '{case.case_number}': Failed to prepare case for update: {str(err)}")
            case.case_status = CaseStatus.processing_failed
            self.update_case_status(case)

    def update_case_status(self, case: Case):
        self.logger.info(f"Case '{case.case_number}': Updating case status")
        _mapping = {
            "Status": case.case_status.value,
        }
        prepared_values = [v for k, v in _mapping.items()]
        start_column = "Status"
        self.sheets_process.update_values(case.case_row_number, start_column, start_column, [prepared_values])

    def _return_empty_if_nan(self, line: str) -> str:
        line = str(line)
        return "" if line == "nan" else line

    def attorney_csv_parsing(self, case: Case) -> tuple:
        import numpy as np

        data = pd.read_csv(case.files["url_attorney"])
        df = pd.DataFrame(data, columns=["Name", "Email"])
        try:
            attorney_email = str()
            attorney_email = self._return_empty_if_nan(df["Email"][0])
        except IndexError:
            self.logger.debug(f"No value for field 'Attorney Email'")
            attorney_email = ""
        try:
            fixed_list = [str(f).strip() for f in df["Email"][1:] if self._return_empty_if_nan(f)]
            other_attorney_emails = "\n".join(fixed_list)
        except IndexError:
            self.logger.debug(f"No value for field 'Other Attorney Emails'")
            other_attorney_emails = ""
        return attorney_email, other_attorney_emails

    @defer.inlineCallbacks  # type: ignore
    def process_cases(self, cases: List[Case]) -> Iterator[defer.Deferred]:
        yield self.crawler_process.crawl(CaseStatusSpider, cases=cases)  # type: ignore

        len_cases = len(cases)

        def update_case_fields(result: Case, case: Case, case_index):
            # TODO DEBUG THIS
            self.logger.info(f"Processing case {case_index} of {len_cases}")
            case.case_status = result.case_status
            case.files["url_attorney"] = result.files["url_attorney"]
            if case.case_status == CaseStatus.processing_failed:
                self.logger.debug("Skipped case processing, but will update status")
                self.update_case_status(case)
                # continue
                return
            self.process_files(case)
            # TODO CHECK IF IT WAS INVOKED
            # self.process_files(case)
            if case.case_status != CaseStatus.processing_failed:
                self.update_case(case)
            else:
                self.update_case_status(case)

            self.logger.debug(json.dumps(case.__dict__, indent=4, default=str))

            # TODO REMOVE ALL CASE FILES BEFORE AND AFTER RUNS
            # self.clear_cached_files(case)

        for index, case in enumerate(cases):
            case_check_defer = self.check_status_pw(case)
            case_check_defer.addCallback(update_case_fields, case, index + 1)
            final_res = yield case_check_defer

        # yield self.crawler_process.join()  # type: ignore
        try:
            reactor.callFromThread(reactor.stop)  # type: ignore
        except ReactorNotRunning:
            pass
