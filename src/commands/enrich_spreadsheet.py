import json
import logging
from typing import Iterator, List

from scrapy.commands import ScrapyCommand
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor
from commands.base.base_command import BaseCommand
from commands.base.base_reactor_command import BaseReactorCommand
from spiders.case_status_spider import CaseStatusSpider
from utils.case import Case
from utils.case_status import CaseStatus

from utils.google_sheets.google_sheets_client import GoogleSheetsClient
from twisted.internet.error import ReactorNotRunning
from scrapy.utils.project import get_project_settings
from utils.pdf.get_pdf_content import get_pdf_content

from utils.pdf.get_pdf_content_ocr import _process_scanned_pdf, get_pdf_content_ocr
from utils.pdf.is_text_file import is_text_file
import re


class EnrichSpreadsheet(BaseCommand):
    """ """

    def __init__(self):
        super().__init__()
        self.project_settings = get_project_settings()
        self.logger = logging.getLogger(self.__class__.__name__)
        # self.db_connection_pool = None

    def set_logger(self, name: str = "COMMAND", level: str = "DEBUG"):
        self.logger = logging.getLogger(name=name)
        self.logger.setLevel(level)
        configure_logging()

    # def execute(self, _args, opts):
    # self.init_db_connection_pool()
    # d = self.db_connection_pool.runInteraction(self._add_accounts)
    # d.addCallback(self._after_import).addErrback(self._errback)

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

    def _after_import(self, transaction):
        self.logger.info("FINISHED")

    def load_cases(self):
        settings = self.settings
        sheets_process = GoogleSheetsClient(settings.get("TOKEN_PATH"), settings.get("CREDENTIALS_PATH"))

        cases: List[Case] = []

        # TODO MAKE THIS NOT HARDCODED???
        case_link_list = sheets_process.load_all_rows_from_name("[URL] Case Link")
        attorney_list = sheets_process.load_all_rows_from_name("[URL] Attorneys")
        petition_list = sheets_process.load_all_rows_from_name("[URL] Petition")
        schedule_a_b_list = sheets_process.load_all_rows_from_name("[URL] Schedule A/B")
        schedule_d_list = sheets_process.load_all_rows_from_name("[URL] Schedule D")
        schedule_e_f_list = sheets_process.load_all_rows_from_name("[URL] Schedule E/F")
        top_twenty_list = sheets_process.load_all_rows_from_name("[URL] Top Twenty")

        row_number = 2  # Header skipped
        for (case_link, attorney, petition, schedule_a_b, schedule_d, schedule_e_f, top_twenty) in zip(
            case_link_list,
            attorney_list,
            petition_list,
            schedule_a_b_list,
            schedule_d_list,
            schedule_e_f_list,
            top_twenty_list,
        ):
            cases.append(
                Case(row_number, case_link, attorney, petition, schedule_a_b, schedule_d, schedule_e_f, top_twenty)
            )
            row_number += 1
        return cases

    # @defer.inlineCallbacks
    def run(self, args, opts):
        self.args = args
        self.opts = opts

        is_load_cases = False
        if is_load_cases:
            cases = self.load_cases()

            self.logger.info(len(cases))
            # TODO REMOVE
            cases = cases[:1]

        self.logger.info("Starting processing case statuses")

        # .addCallback(self.save_cases,cases)
        # self.crawler_process.start(stop_after_crawl)
        #
        # self.crawler_process.join()  # type: ignore

        # self.cases.extend(chunked_cases)

        # Google Sheet enrichment process in steps:
        # 1. Status value checking:
        # ~ 1.1 Check file from 'Case Link' (column A):
        # - If pdf contains 'dismissed' in top right corner - status should be set as 'Dismissed'. CONTINUE processing this row.
        # - Else: continue processing, status 'Active'

        # for case in cases:

        # reactor.run()
        # runner.crawl(MySpider1)
        # runner.crawl(MySpider2)
        # d = self.crawler_process.join()

        # d.addErrback(lambda _: reactor.stop())
        # return d
        # reactor.run() # the script will block here until all crawling jobs are finished

        # reactor.callFromThread(self.execute, args, opts)
        # reactor.callFromThread(d)
        # reactor.callFromThread(reactor.stop)

        # TODO UNCOMMENT
        # self.run_status_checks(cases)
        # reactor.run()
        # self.logger.info("SAVED CASES TO FILE")
        # self.save_cases(cases)
        # self.logger.info(cases)
        #

        # cases = json.loads(''.join(inp.readlines()))
        # cases = [Case.from_dict(c) for c in cases]
        cases = [
            Case(
                2,
                "https://www.inforuptcy.com/filings/cacbke_1942604",
                "https://www.inforuptcy.com/attorneys/export-csv/cacbke_1942604",
                "https://www.inforuptcy.com/ir-documentselect/download_pdf/docket/cacbke_1942604/1/1",
                "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/ab-0B9EC878-A4FA-11EC-9453-2161FAA6289E?filename=schedule_ab.pdf",
                "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/d-0B40C41C-A4FA-11EC-AF6D-23267CB7F93E?filename=schedule_d.pdf",
                "https://pdf.inforuptcy.com/pacer/cacbke/1942604/schedule-forms/ef-0B1CA190-A4FA-11EC-A9E4-51F6E80E6D45?filename=schedule_ef.pdf",
                "",
            )
        ]
        cases[0].files = {
            "url_attorney": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_attorney\\httpswwwinforuptcycomattorneysexportcsvcacbke1942604.csv",
            "url_schedule_d": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_d\\httpspdfinforuptcycompacercacbke1942604scheduleformsd0B40C41CA4FA11ECAF6D23267CB7F93Efilenamescheduledpdf.pdf",
            "url_schedule_a_b": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_a_b\\httpspdfinforuptcycompacercacbke1942604scheduleformsab0B9EC878A4FA11EC94532161FAA6289Efilenamescheduleabpdf.pdf",
            # "url_schedule_a_b": "D:/upwork/wilroserealty_pdf_parsing/temp\\url_schedule_a_b\\schedule_ab.pdf",
        }
        cases[0].case_status = CaseStatus.dismissed

        for case in cases:
            if case.case_status == CaseStatus.processing_failed:
                self.logger.debug("Skipped case processing")
                continue
            self.process_files(case)

        # TODO REMOVE THIS
        self.logger.info(json.dumps([c.__dict__ for c in cases], indent=4, default=str))
        # Required modules:
        # - Google Sheets usage (login, read, edit, save)
        # - Status webpage scraping
        # - (possibly) CSV parsing
        # - PDF file download
        # - PDF file content reading and parsing

        # reactor.callLater(0, self.execute, args, opts)
        # reactor.run()

    def process_files(self, case: Case):

        file_field_type_mapping = {
            "url_attorney": "csv",
            # TODO url_petition
            "url_schedule_a_b": "pdf",
            "url_schedule_d": "pdf",
            "url_schedule_e_f": "pdf",
            "url_top_twenty": "pdf",
        }

        # 2. Attorney emails fillup (columns AJ, AK)
        # Save only emails. First person - column AJ, others go to AK
        # Get data from column K (Attorneys): it has a csv file inside and contains same emails
        # attorneys = self.
        import pandas as pd

        # Attorney Email
        # Other Attorney Emails
        data = pd.read_csv(case.files["url_attorney"])
        df = pd.DataFrame(data, columns=["Name", "Email"])
        # self.logger.info(df)
        self.logger.info(df["Email"][0])
        try:
            case.enrichable_values["attorney_email"] = df["Email"][0]
        except IndexError:
            self.logger.debug(f"No value for field 'Attorney Email'")
        try:
            case.enrichable_values["other_attorney_emails"] = ", ".join(df["Email"][1:])
        except IndexError:
            self.logger.debug(f"No value for field 'Other Attorney Emails'")
        # case.enrichable_values["Other Attorney Emails"] = ""

        schedule_a_b_data = schedule_a_b_parsing(case.files["url_schedule_a_b"])
        # self.logger.info(schedule_a_b_data)
        case.enrichable_values["schedule_a_b_rows"] = schedule_a_b_data

        schedule_d_data = schedule_d_parsing(case.files["url_schedule_d"])
        case.enrichable_values["schedule_d_rows"] = schedule_d_data

    @defer.inlineCallbacks
    def run_status_checks(self, cases: List[Case]) -> Iterator[defer.Deferred]:

        self.crawler_process.crawl(CaseStatusSpider, cases=cases)
        yield self.crawler_process.join()
        try:
            reactor.callFromThread(reactor.stop)
        except ReactorNotRunning:
            pass

    def save_cases(self, cases):
        with open("output.json", "w", encoding="utf-8") as outp:
            outp.write(json.dumps([c.__dict__ for c in cases], indent=4, default=str))

    def init(self):
        pass


from subprocess import CalledProcessError

# from pdf import *


# def extract_table_text(filename):
#     # !DEPRECATED
#     from tabula import read_pdf

#     # ,

#     table_pdf = read_pdf(
#         filename,
#         guess=False,
#         pandas_options={"columns": ["column_name", "text_value"]},
#         pages=[5],
#         stream=True,
#         encoding="utf-8",
#         # (top,left,bottom,right)
#         area=(96, 5, 558, 350),
#     )

#     for row in table_pdf:
#         print(f"TABLE ")

#         import pandas as pd

#         if type(row) is pd.DataFrame:

#             if "55." in str(row):
#                 # row.columns = ['part_name', 'text']
#                 # print(f'\n\n{row["name"]}')
#                 for index, data in row.iterrows():
#                     print(f"\nindex {index}")
#                     # v=str(data['text']).replace('\n', '')
#                     print(f"data:\n{data}")


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
