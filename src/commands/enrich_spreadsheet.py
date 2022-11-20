import json
import logging
from typing import Iterator, List

from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor, threads
from twisted.internet.defer import inlineCallbacks, returnValue
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


from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
import re


class EnrichSpreadsheet(BaseCommand):
    """ """

    def __init__(self):
        super().__init__()
        self.project_settings = get_project_settings()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sheets_process: GoogleSheetsClient

    def init(self):
        """Init method for all resource-consuming things"""
        settings = self.settings
        self.sheets_process = GoogleSheetsClient(settings.get("TOKEN_PATH"), settings.get("CREDENTIALS_PATH"))

    def _exit(self, _result=None):
        print("Name Combination search queries generation has been completed")
        reactor.stop()

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
        case_link_list = sheets_process.load_all_rows_from_name("[URL] Case Link")
        attorney_list = sheets_process.load_all_rows_from_name("[URL] Attorneys")
        petition_list = sheets_process.load_all_rows_from_name("[URL] Petition")
        schedule_a_b_list = sheets_process.load_all_rows_from_name("[URL] Schedule A/B")
        schedule_d_list = sheets_process.load_all_rows_from_name("[URL] Schedule D")
        schedule_e_f_list = sheets_process.load_all_rows_from_name("[URL] Schedule E/F")
        top_twenty_list = sheets_process.load_all_rows_from_name("[URL] Top Twenty")

        row_number = 2  # Header skipped
        for (case_number, case_link, attorney, petition, schedule_a_b, schedule_d, schedule_e_f, top_twenty) in zip(
            case_numbers_list,
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
        self.logger.info(f"Case {case.case_number}: Starting PW case status checking")
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
            self.logger.error(f"Case {case.case_number}: Got an error while enriching case: {str(err)}")
            case.case_status = CaseStatus.processing_failed
            # No need to process more

        return case

    def run(self, args, opts):
        self.args = args
        self.opts = opts

        cases: List[Case] = self.load_cases()

        self.logger.info(f"Received {len(cases)} cases from Google Sheet")
        # TODO REMOVE, DEBUG ONLY
        # cases = cases[:5]

        self.logger.info("Starting processing case statuses")

        # TODO UNCOMMENT
        self.run_status_checks(cases)
        reactor.run()

        for c in cases:
            self.logger.info(f"Case {c.case_number}: value {c.case_status} file {c.files['url_attorney']}")
        # return
        # self.save_cases(cases)

        # NOTE: DEBUG ONLY
        # cases = [
        #     Case(
        #         2,
        #         "https://www.inforuptcy.com/filings/cacbke_1942604",
        #         "https://www.inforuptcy.com/attorneys/export-csv/cacbke_1942604",
        #         "https://www.inforuptcy.com/ir-documentselect/download_pdf/docket/cacbke_1942604/1/1",
        #         "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/ab-0B9EC878-A4FA-11EC-9453-2161FAA6289E?filename=schedule_ab.pdf",
        #         "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/d-0B40C41C-A4FA-11EC-AF6D-23267CB7F93E?filename=schedule_d.pdf",
        #         "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/ef-0B1CA190-A4FA-11EC-A9E4-51F6E80E6D45?filename=schedule_ef.pdf",
        #         "",
        #     )
        # ]
        # cases[0].files = {
        #     "url_attorney": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_attorney\\httpswwwinforuptcycomattorneysexportcsvcacbke1942604.csv",
        #     "url_schedule_d": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_d\\httpspdfinforuptcycompacercacbke1942604scheduleformsd0B40C41CA4FA11ECAF6D23267CB7F93Efilenamescheduledpdf.pdf",
        #     "url_schedule_a_b": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_a_b\\httpspdfinforuptcycompacercacbke1942604scheduleformsab0B9EC878A4FA11EC94532161FAA6289Efilenamescheduleabpdf.pdf",
        #     # "url_schedule_a_b": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_a_b\\schedule_ab.pdf",
        # }
        # cases[0].case_status = CaseStatus.dismissed

        for case in cases:
            if case.case_status == CaseStatus.processing_failed:
                self.logger.debug("Skipped case processing, but will update status")
                self.update_case_status(case)
                continue
            self.process_files(case)
            # NOTE: For now, cases will not have status updated when error occured
            self.update_case(case)

        # TODO REMOVE THIS
        # self.logger.info(json.dumps([c.__dict__ for c in cases], indent=4, default=str))
        # Required modules:
        # - Google Sheets usage (login, read, edit, save)
        # - Status webpage scraping
        # - (possibly) CSV parsing
        # - PDF file download
        # - PDF file content reading and parsing

        # reactor.callLater(0, self.execute, args, opts)
        # reactor.run()

    def process_files(self, case: Case):
        # file_field_type_mapping = {
        #     "url_attorney": "csv",
        #     # TODO url_petition
        #     "url_schedule_a_b": "pdf",
        #     "url_schedule_d": "pdf",
        #     "url_schedule_e_f": "pdf",
        #     "url_top_twenty": "pdf",
        # }

        # 2. Attorney emails fillup (columns AJ, AK)
        # Save only emails. First person - column AJ, others go to AK
        # Get data from column K (Attorneys): it has a csv file inside and contains same emails
        try:
            attorney_email, other_attorney_emails = self.attorney_csv_parsing(case)
            case.enrichable_values["attorney_email"] = attorney_email
            case.enrichable_values["other_attorney_emails"] = other_attorney_emails

            schedule_a_b_data = schedule_a_b_parsing(case.files["url_schedule_a_b"])
            case.enrichable_values["schedule_a_b_rows"] = schedule_a_b_data

            schedule_d_data = schedule_d_parsing(case.files["url_schedule_d"])
            case.enrichable_values["schedule_d_rows"] = schedule_d_data
        except Exception as err:
            self.logger.error(f"Case {case.case_number}: Failed to process case: {str(err)}")
            case.case_status = CaseStatus.processing_failed

    def _get_dict_formatted(self, case: Case, field_name: str) -> str:
        try:
            dict_data = case.enrichable_values[field_name]
            return "\n".join([f"{k}: {v}" for k, v in dict_data.items()])
        except KeyError as ke:
            return f"Failed to parse due to missing value: {field_name}"

    def _process_addresses(self, case: Case):
        addresses = [get_parsed_address(v[0]) for k, v in case.enrichable_values["schedule_a_b_rows"].items()]
        return "\n".join(addresses)

    def _prepare_case_data(self, case: Case) -> List[str]:
        # "Status","Creditor Notes","Borrower Notes","Property Notes","ADDRESS","Attorney Email","Other Attorney Emails"
        _mapping = {
            "Status": case.case_status.value,
            "Creditor Notes": self._get_dict_formatted(case, "schedule_d_rows"),
            "Borrower Notes": "UNSUPPORTED",
            "Property Notes": self._get_dict_formatted(case, "schedule_a_b_rows"),
            "ADDRESS": self._process_addresses(case),
            "Attorney Email": case.enrichable_values["attorney_email"],
            "Other Attorney Emails": case.enrichable_values["other_attorney_emails"],
        }
        return [v for k, v in _mapping.items()]

    def update_case(self, case: Case):
        # Status","Creditor Notes","Borrower Notes","Property Notes","ADDRESS","Attorney Email","Other Attorney Emails
        self.logger.info(f"Case {case.case_number}: Updating case rows")
        try:
            prepared_values = self._prepare_case_data(case)
            # TODO Ideally somehow define these values
            start_column = "Status"
            end_column = "Other Attorney Emails"

            self.sheets_process.update_values(case.case_row_number, start_column, end_column, [prepared_values])
        except Exception as err:
            self.logger.error(f"Case {case.case_number}: Failed to prepare case for update: {str(err)}")
            case.case_status = CaseStatus.processing_failed
            self.update_case_status(case)

    def update_case_status(self, case: Case):
        self.logger.info(f"Case {case.case_number}: Updating case status")
        _mapping = {
            "Status": case.case_status.value,
        }
        prepared_values = [v for k, v in _mapping.items()]
        start_column = "Status"
        self.sheets_process.update_values(case.case_row_number, start_column, start_column, [prepared_values])

    def attorney_csv_parsing(self, case: Case) -> tuple:
        data = pd.read_csv(case.files["url_attorney"])
        df = pd.DataFrame(data, columns=["Name", "Email"])
        try:
            attorney_email = df["Email"][0]
        except IndexError:
            self.logger.debug(f"No value for field 'Attorney Email'")
            attorney_email = ""
        try:
            other_attorney_emails = "\n".join(df["Email"][1:])
        except IndexError:
            self.logger.debug(f"No value for field 'Other Attorney Emails'")
            other_attorney_emails = ""
        return attorney_email, other_attorney_emails

    @defer.inlineCallbacks  # type: ignore
    def run_status_checks(self, cases: List[Case]) -> Iterator[defer.Deferred]:
        self.crawler_process.crawl(CaseStatusSpider, cases=cases)  # type: ignore

        case_statuses = []

        def update_case_fields(result: Case, case: Case):
            # TODO CHECK THIS
            case.case_status = result.case_status
            case.files["url_attorney"] = result.files["url_attorney"]

        for case in cases:
            yield self.check_status_pw(case).addCallback(update_case_fields, case)

        yield self.crawler_process.join()  # type: ignore
        try:
            reactor.callFromThread(reactor.stop)  # type: ignore
        except ReactorNotRunning:
            pass

    def save_cases(self, cases):
        with open("output.json", "w", encoding="utf-8") as outp:
            outp.write(json.dumps([c.__dict__ for c in cases], indent=4, default=str))


from subprocess import CalledProcessError


logger = logging.getLogger(__name__)


def schedule_a_b_parsing(filename):
    # 3. 'Schedule A/B' parsing
    # Fill 'Notes' column with data from section 9, rows '55.{X}' (where X will change)
    # Take only first two columns.
    # TODO
    # ~ 3.1 (optional) Try to get addresses from data collected in 'Step 3'

    extracted_rows = {}
    current_group_lines = []
    is_savable = False
    boundaries = {"left": 40, "bottom": 80, "right": 320, "top": 792 - 80}
    for part in get_pdf_content_ocr(filename, boundaries):
        if not part.strip():
            continue
        if part.startswith("56"):
            break

        if is_savable and not part.startswith("55"):
            if "   " in part:
                # Was an image PDF file
                logger.debug(f"Splitting line as it is from image: {part}")
                current_group_lines.extend([p.strip() for p in part.split("   ") if p.strip()][:2])
            else:
                current_group_lines.append(part)
        import re

        match = re.search(r"55\.(\d+)", part)
        if match:
            group_name = match.group(0)
            current_group_lines = []
            extracted_rows[group_name] = current_group_lines
            is_savable = True

    # Will clear some values now
    for row, values in extracted_rows.items():
        if len(values) > 2:
            # Test whether was a text PDF file
            row_values = [p.strip() for p in values]
            last_row = row_values[-1]
            partial = last_row.strip().split()
            values[-1] = " ".join(partial[:-1])
            values.append(partial[-1])
            extracted_rows[row] = [" ".join(values[:-1]), values[-1]]
        else:
            # Was an image PDF file
            pass

    # is_save_line = False
    # for line in get_pdf_content_ocr(filename):
    #     # logger.info(line)
    #     if line.startswith("56"):
    #         current_row_id = None
    #         is_save_line = False
    #         break

    #     if is_save_line and not line.startswith("55."):
    #         parts = [p.strip() for p in line.split("  ") if p]
    #         #logger.info(parts)
    #         for index, value in enumerate(parts):
    #             try:
    #                 curr_cell = extracted_rows[current_row_id][index]
    #                 extracted_rows[current_row_id][index] = (curr_cell + " " + value).strip()
    #             except IndexError:
    #                 print("Failed to enrich row element")

    #     if line.startswith("55.") and "55. " not in line:
    #         parts = [p.strip() for p in line.split("   ") if p]
    #         parts = parts[0].split(" ", maxsplit=1) + parts[1:]
    #         if not parts:
    #             raise RuntimeError(f"Empty values parsed from line {line}")
    #         current_row_id = parts[0]
    #         parts = parts[1:]
    #         try:
    #             temp_value = parts[:-2] + parts[-2].split(" ", maxsplit=1) + [parts[-1]]
    #             current_row = temp_value
    #         except IndexError:
    #             missing_columns_count = 5 - len(parts)
    #             current_row = parts + missing_columns_count * [""]

    #         extracted_rows[current_row_id] = current_row
    #         is_save_line = True

    # for ind, row in extracted_rows.items():
    #     print(f'ROW NAME: "{ind}"')
    #     print(f'{row[:2]}\n')

    return extracted_rows


def schedule_d_parsing(filename):
    # 4. 'Schedule D' parsing (may be tricky)
    # In section 'List Creditors Who Have Secured Claims'
    # Get "Creditor's Name" from rows '2.{X}'
    # Also get "Creditor's mailing address"

    # (not decided yet): Save to 'Creditors Info' column. Maybe will need two columns

    extracted_rows = {}
    current_group_lines = []
    is_savable = False
    boundaries = {"left": 40, "bottom": 80, "right": 180, "top": 792 - 40}
    for part in get_pdf_content_ocr(filename, boundaries):
        if is_savable and not part.startswith("2."):
            current_group_lines.append(part)

        match = re.search(r"2\.(\d+)(.*)", part)
        if match:
            group_name = match.group(1)

            current_group_lines = [match.group(2).strip()]
            extracted_rows[group_name] = current_group_lines
            is_savable = True

    clear_data = {}

    for ind, row in extracted_rows.items():
        full_row = " ".join(row)
        required_text_regex = r"(.*)Creditor's Name(.*)Creditor's mailing address"
        match = re.search(required_text_regex, full_row)
        if match:
            clear_data[ind] = {"name": match.group(1).strip(), "mailing_address": match.group(2).strip()}

    return clear_data
