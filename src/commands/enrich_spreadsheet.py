import json
import logging
import pickle
from itertools import zip_longest
from os import remove
from typing import Iterator, List

import pandas as pd
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor, threads
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ReactorNotRunning

from commands.base.base_command import BaseCommand
from spiders.case_status_spider import CaseStatusSpider
from spiders.pw_status_check_spider import PWStatusCheckSpider
from utils.case import Case
from utils.case_enrichment_status import CaseEnrichmentStatus
from utils.case_status import CaseStatus
from utils.get_parsed_address import get_parsed_address
from utils.google_sheets.google_sheets_client import GoogleSheetsClient
from utils.pdf_parsers.pdf_parser_ab import PdfParserAB
from utils.pdf_parsers.pdf_parser_d import PdfParserD


class EnrichSpreadsheet(BaseCommand):
    """ """

    def __init__(self):
        super().__init__()
        self.project_settings = get_project_settings()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sheets_process: GoogleSheetsClient
        self.pdf_parser_ab = PdfParserAB()
        self.pdf_parser_d = PdfParserD()

    def init(self):
        """Init method for all resource-consuming things"""
        settings = self.settings
        self.sheets_process = GoogleSheetsClient(
            settings.get("TOKEN_PATH"), settings.get("CREDENTIALS_PATH"), settings
        )

    def _errback(self, failure):
        self.logger.error(failure)

    def add_options(self, parser) -> None:
        super().add_options(parser)
        group = parser.add_argument_group(title="Custom settings")
        group.add_argument(
            "--debug",
            metavar="debug_mode",
            help="Debug mode",
            default=False,
        )

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
            d = threads.deferToThread(
                spider.get_page_html_playwright, case.url_case_link, [".card-header"]
            )
            full_html = yield d

            is_dismissed = spider.check_is_dismissed(full_html)
            case.case_status = CaseStatus.dismissed if is_dismissed else CaseStatus.active
            fd = threads.deferToThread(spider.download_file_pw, case, "url_attorney")
            filename = yield fd
            case.files["url_attorney"] = filename
        except Exception as err:
            self.logger.error(
                f"Case '{case.case_number}': Got an error while enriching case via PW: {str(err)}"
            )
            case.enrichment_status = CaseEnrichmentStatus.processing_failed
            # No need to process more

        return case

    def filter_cases_by_orig_status(self, cases: List[Case]) -> List[Case]:
        skip_statuses_list = [CaseStatus.dismissed.value, CaseStatus.active.value]

        logging.getLogger("case_status_spider").setLevel("INFO")
        logging.getLogger("scrapy.core.engine").setLevel("INFO")

        new_cases = []
        for case in cases:
            if case.case_status_original in skip_statuses_list:
                continue
            new_cases.append(case)
        return new_cases

    def _run_debug_processing(self):
        with open("debug_cases_schedule_d.pickle", "rb") as handle:
            cases = pickle.load(handle)
            processable_cases = ["2:22-bk-20632"]

            required_cases = []
            for c in cases:
                if c.case_number in processable_cases:
                    required_cases.append(c)

            cases = required_cases
            for index, case in enumerate(cases):
                self.logger.info(f"Processing case {index+1} of {len(cases)}")
                if case.enrichment_status == CaseEnrichmentStatus.processing_failed:
                    self.logger.debug("Skipped case processing, but will update status")
                # NOTE: This is not usual way
                self.process_files(case)

                if case.enrichment_status != CaseEnrichmentStatus.processing_failed:
                    self.update_case(case)
                else:
                    self.update_case_status(case)

                self.logger.debug(json.dumps(case.__dict__, indent=4, default=str))

    def run(self, args, opts):
        self.args = args
        self.opts = opts

        if opts.debug:
            self._run_debug_processing()
        else:
            self.regular_run()

        self.logger.debug("self.pdf_parser_ab.cases_by_file_type")
        self.logger.debug(
            json.dumps(dict(self.pdf_parser_ab.cases_by_file_type, indent=4, default=str))
        )
        self.logger.debug("\n\nself.pdf_parser_d.cases_by_file_type")
        self.logger.debug(
            json.dumps(dict(self.pdf_parser_d.cases_by_file_type, indent=4, default=str))
        )

    def regular_run(self):
        cases: List[Case] = self.load_cases()

        self.logger.info(f"Received {len(cases)} cases from Google Sheet")
        self.logger.info("Starting processing case statuses")
        # TODO uncomment this
        # cases = self.filter_cases_by_orig_status(cases)
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

        reactor.run()  # type: ignore
        # TODO REMOVE
        # with open("debug_cases_schedule_d______.pickle", "wb") as handle:
        #     pickle.dump(cases, handle)

    def clear_cached_files(self, case: Case) -> None:
        for _, file_path in case.files.items():
            remove(file_path)
            self.logger.debug(f"Removed file {file_path}")

    def _process_pdf(self, case, func, file_field_name: str, result_field_name: str):
        try:
            if file_field_name in case.files:
                pdf_file_data = func(case.files[file_field_name])
                case.enrichable_values[result_field_name] = pdf_file_data
            else:
                self.logger.debug(f"Case {file_field_name}: missing file {file_field_name}")
        except Exception as err:
            return f"Case {file_field_name}: {file_field_name} parsing: {str(err)}"

    def process_files(self, case: Case):
        # 2. Attorney emails fillup (columns AJ, AK)
        # Save only emails. First person - column AJ, others go to AK
        # Get data from column K (Attorneys): it has a csv file inside and contains same emails
        error_msgs = []
        try:
            try:
                (
                    attorney_email,
                    other_attorney_emails,
                    gov_emails,
                ) = self.attorney_csv_parsing(case)
                case.enrichable_values["attorney_email"] = attorney_email
                case.enrichable_values["other_attorney_emails"] = other_attorney_emails
                case.enrichable_values["gov_attorney_emails"] = gov_emails
            except Exception as err:
                error_msgs.append(f"Attorneys.csv file parsing: {str(err)}")

            r = self._process_pdf(
                case,
                self.pdf_parser_ab.schedule_a_b_parsing,
                "url_schedule_a_b",
                "schedule_a_b_rows",
            )
            error_msgs.append(r)

            try:
                case.enrichable_values["addresses"] = self._process_addresses(case)
            except Exception as err:
                error_msgs.append(f"Address parsing: {str(err)}")

            r = self._process_pdf(
                case,
                self.pdf_parser_d.schedule_d_parsing,
                "url_schedule_d",
                "schedule_d_rows",
            )
            error_msgs.append(r)

        except Exception as err:
            error_msgs.append(f"Case data parsing: {str(err)}")

        error_msgs = [p for p in error_msgs if p]
        if error_msgs:
            all_msgs = "\n".join(error_msgs)
            self.logger.error(f"Case '{case.case_number}': Failed to process case: {all_msgs}")
            case.enrichment_status = CaseEnrichmentStatus.processing_failed

    def _get_dict_formatted(self, case: Case, field_name: str, file_field_name: str) -> str:
        if not case.enrichable_values.get(field_name):
            if not case.files.get(file_field_name):
                return ""
            self.logger.error(
                f"Case '{case.case_number}': Failed to parse due to empty value: {field_name}"
            )
            return ""
        try:
            dict_data = case.enrichable_values[field_name]
            return "\n".join(
                ["; ".join([val for _, val in v.items()]) for k, v in dict_data.items()]
            )
        except KeyError as ke:
            raise RuntimeError(
                f"Case '{case.case_number}': Failed to parse due to missing value: {field_name}"
            ) from ke

    def _process_addresses(self, case: Case):
        if case.enrichable_values.get("schedule_a_b_rows"):
            addresses = [
                get_parsed_address(str(v.get("address", "")))
                for _, v in case.enrichable_values["schedule_a_b_rows"].items()
            ]
            return "\n".join(addresses)
        return ""

    def _check_possible_failure(self, case: Case):
        addr = self._process_addresses(case)
        ab_data = self._get_dict_formatted(case, "schedule_a_b_rows", "url_schedule_a_b")
        d_data = self._get_dict_formatted(case, "schedule_d_rows", "url_schedule_d")
        if not all((addr, ab_data, d_data)):
            if not d_data and not case.url_schedule_d:
                # OK
                return
            if not ab_data and not case.url_schedule_a_b:
                # OK
                return

            case.enrichment_status = CaseEnrichmentStatus.possible_failure
            return
        case.enrichment_status = CaseEnrichmentStatus.success

    def _prepare_case_data(self, case: Case) -> List[str]:
        # Status","Creditor Notes","Borrower Notes","Property Notes","ADDRESS","Attorney Email","Other Attorney Emails
        self._check_possible_failure(case)
        _mapping = {
            "Status": case.case_status.value,
            "Creditor Notes": self._get_dict_formatted(case, "schedule_d_rows", "url_schedule_d"),
            "Borrower Notes": "",
            "Property Notes": self._get_dict_formatted(
                case, "schedule_a_b_rows", "url_schedule_a_b"
            ),
            "ADDRESS": case.enrichable_values["addresses"],
            "Attorney Email": case.enrichable_values["attorney_email"],
            "Other Attorney Emails": case.enrichable_values["other_attorney_emails"],
            "Gov Attorney Emails": case.enrichable_values["gov_attorney_emails"],
            "Enrichment Status": case.enrichment_status.value,
        }
        return [v for k, v in _mapping.items()]

    def update_case(self, case: Case):
        self.logger.info(f"Case '{case.case_number}': Updating case rows")
        try:
            prepared_values = self._prepare_case_data(case)
            # TODO Ideally somehow define these values
            start_column = "Status"
            end_column = "Gov Attorney Emails"

            self.sheets_process.update_values(
                case.case_row_number, start_column, end_column, [prepared_values]
            )
        except Exception as err:
            self.logger.error(
                f"Case '{case.case_number}': Failed to prepare case for update: {str(err)}"
            )
            case.enrichment_status = CaseEnrichmentStatus.processing_failed
            self.update_case_status(case)

    def update_case_status(self, case: Case):
        self.logger.info(f"Case '{case.case_number}': Updating case status")
        _mapping = {
            "Status": case.case_status.value,
            "Enrichment Status": case.enrichment_status.value,
        }
        prepared_values = [v for k, v in _mapping.items()]
        start_column = "Status"
        self.sheets_process.update_values(
            case.case_row_number, start_column, start_column, [prepared_values]
        )

    def _return_empty_if_nan(self, line: str) -> str:
        line = str(line)
        return "" if line == "nan" else line

    def attorney_csv_parsing(self, case: Case) -> tuple:
        data = pd.read_csv(case.files["url_attorney"])
        df = pd.DataFrame(data, columns=["Name", "Email"])
        all_emails = [str(f).strip() for f in df["Email"] if self._return_empty_if_nan(f)]

        # Other attorney email - make 'usdoj.gov' to another column
        # If Attorney email is empty - fillup with second one
        gov_emails = [f for f in all_emails if "usdoj.gov" in f]
        non_gov_emails = [f for f in all_emails if "usdoj.gov" not in f]
        try:
            attorney_email = non_gov_emails[0]
        except IndexError:
            self.logger.debug(f"No value for field 'Attorney Email'")
            attorney_email = ""
        try:
            other_attorney_emails = "\n".join(non_gov_emails[1:])
        except IndexError:
            self.logger.debug(f"No value for field 'Other Attorney Emails'")
            other_attorney_emails = ""

        return attorney_email, other_attorney_emails, "\n".join(gov_emails)

    @defer.inlineCallbacks  # type: ignore
    def process_cases(self, cases: List[Case]) -> Iterator[defer.Deferred]:
        yield self.crawler_process.crawl(CaseStatusSpider, cases=cases)  # type: ignore

        len_cases = len(cases)

        def update_case_fields(result: Case, case: Case, case_index):
            self.logger.info(f"Processing case {case_index} of {len_cases}")
            try:
                case.case_status = result.case_status
                case.files["url_attorney"] = result.files["url_attorney"]
                if case.enrichment_status == CaseEnrichmentStatus.processing_failed:
                    self.logger.debug("Skipped case processing, but will update status")
                    self.update_case_status(case)
                    # continue
                    return
                self.process_files(case)
            except Exception as err:
                self.logger.error(
                    f"Case '{case.case_number}': Failed to fetch data from PW: {str(err)}"
                )

            self.process_files(case)
            if case.enrichment_status != CaseEnrichmentStatus.processing_failed:
                self.update_case(case)
            else:
                self.update_case_status(case)

            self.logger.debug(json.dumps(case.__dict__, indent=4, default=str))

            # NOTE REMOVE ALL CASE FILES BEFORE AND AFTER RUNS
            self.clear_cached_files(case)

        for index, case in enumerate(cases):
            case_check_defer = self.check_status_pw(case)
            case_check_defer.addCallback(update_case_fields, case, index + 1)
            final_res = yield case_check_defer

        # yield self.crawler_process.join()  # type: ignore
        try:
            reactor.callFromThread(reactor.stop)  # type: ignore
        except ReactorNotRunning:
            pass
