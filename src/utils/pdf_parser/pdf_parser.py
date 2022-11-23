import json
import logging
import re


from scrapy.utils.project import get_project_settings
from utils.get_parsed_address import get_parsed_address
from utils.pdf.get_pdf_content import get_pdf_content_fitz, get_pdf_content_pdfium
from utils.pdf.get_pdf_content_from_text_ocr import get_pdf_content_from_text_ocr

from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
import re

from utils.pdf.is_text_file import is_text_file


class PdfParser:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__.split(".")[-1])

    def parse_pdf_file(self, filename, processing_funcs, **kwargs):
        def get_pdf_file_type(filename):
            return "text" if is_text_file(filename) else "scan"

        file_type = get_pdf_file_type(filename)
        self.logger.debug(f"file_type {file_type}")
        extracted_rows = processing_funcs[file_type](filename)
        return extracted_rows

    def schedule_a_b_parsing_scan(self, filename: str):
        extracted_rows = {}
        current_group_lines = []
        is_savable = False
        boundaries = {"left": 40, "bottom": 80, "right": 320, "top": 792 - 80}
        for part in get_pdf_content_ocr(filename, boundaries):

            self.logger.info(part)
            if not part.strip():
                continue
            if part.startswith("56"):
                break

            if is_savable and not part.startswith("55"):
                self.logger.info(part)
                if "   " in part:
                    # Was an image PDF file
                    self.logger.debug(f"Splitting line as it is from image: {part}")
                    current_group_lines.extend([p.strip() for p in part.split("   ") if p.strip()][:2])
                else:
                    current_group_lines.append(part)
            import re

            match = re.search(r"55\.(\d+)\.(.*)", part)
            if match:
                group_name = match.group(1)

                current_group_lines = []
                if match.group(2):
                    current_group_lines.append(str(match.group(2)).strip())
                extracted_rows[group_name] = current_group_lines
                is_savable = True

    def schedule_a_b_parsing_text_type1(self, filename: str):
        self.logger.info("schedule_a_b_parsing_text_type1")
        extracted_rows = {}
        # boundaries = {"left": 0, "bottom": 60, "right": 400, "top": 792 - 40}

        def crop_image(image):
            w, h = image.size
            bounding_box = (50, 40, int(w / 2) + 40, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image):
            if any(("Part 8" in page_text, "Part 9" in page_text, "Part 10" in page_text)):
                all_text += page_text
            if "Part 10" in page_text:
                break

        all_text = all_text.replace("\n", " ")

        # self.logger.info("OCR")
        # all_text = " ".join([l for l in get_pdf_content_from_text_ocr(filename)])
        # with open("ocr.txt", "w", encoding="utf-8") as outp:
        #     outp.write(all_text)

        part_nine = all_text
        # part_nine = all_text.split("55. Any building, other improved real estate")[-1]
        with open("sample_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(part_nine)
        # part_nine_rows = part_nine.split("55")
        # for row_text in part_nine_rows:
        #     row_text = row_text.replace(' ~"', "")
        #     unique_ind = 1
        #     for p in row_text.split("  "):
        #         # self.logger.info(get_parsed_address(p))
        #         # self.logger.info(p)

        #         for prt in p.split("$"):
        #             try:
        #                 from usaddress import tag

        #                 parsed, _ = tag(prt)
        #                 if len(parsed) < 2:

        #                     continue
        #                 # Will not include these tags to final text
        #                 skip_list = ["Recipient", "SubaddressType", "OccupancyIdentifier"]
        #                 text_addr = " ".join([v for k, v in parsed.items() if k not in skip_list])
        #                 if ".00" not in text_addr:
        #                     extracted_rows[unique_ind] = [text_addr, ""]
        #                     unique_ind += 1
        #             except Exception as err:
        #                 self.logger.info(err)
        # TODO add 55.2 check
        pattern = r"land which the debtor owns or i?\s*(.*?)  ([^\s].*?)55\.(\d)(.*)Total of Part 9"
        for match in re.finditer(pattern, all_text):
            groups = list(match.groups())
            self.logger.info(f'"{groups}"')
            groups = [g.strip() for g in groups]
            # ['5755 Bayport Blvd.                          ', 'Fee Simple', '1', ' 5755 Bayport Blvd., Seabrook, TX 77586']
            group_name = groups[-2]
            extracted_rows[group_name] = [groups[0] + " " + groups[-1], groups[1]]
            # last_rows = groups[-4:]  # These are not needed
            # required_data = groups[:-4]
        # TODO
        # PARSE LINES LIKE
        # 55.1. LAND, BUILDINGS, AND  IMPROVEMENTS: 700 S KINGS  AVE, BRANDON, FL 33511 100% OWNER OF REAL PROPERTY Total of Part 9
        return extracted_rows

        pattern = re.compile(r"55\.(\d+)\.(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\nTotal|55")

        for match in re.finditer(pattern, all_text):
            groups = list(match.groups())
            self.logger.info(f'"{groups}"')
            last_rows = groups[-4:]  # These are not needed
            required_data = groups[:-4]

            row_name = groups[0]
            fee_type = last_rows[0]
            address = " ".join(required_data[1:])

            self.logger.info(f"required_data {required_data}")
            self.logger.info(f"row_name {row_name}")
            self.logger.info(f"address {address}")
            self.logger.info(f"fee_type {fee_type}")
            self.logger.info(f"last_rows {last_rows}")

            extracted_rows[row_name] = [address.strip().replace("\n", " "), fee_type.strip()]
        return extracted_rows

    def schedule_a_b_parsing_text(self, filename: str):
        # TODO Parse such situations
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: 55.
        # Any building, other improved real estate, or land which the debtor owns or in which the debtor has interest
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: 55.1
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: 5755 Bayport Blvd.
        # 5755 Bayport Blvd., Seabrook, TX 77586
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: Fee Simple
        # (Unknown)
        # Bank Appraisal -
        # December 23, 2019
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: $7,570,000.00
        # 2022-11-20 19:42:42 [commands.enrich_spreadsheet] INFO: line: 56.
        # Total of Part 9

        # TODO fix when no 'fee' value in PDF

        # TODO Fix for ["2:22-bk-11403"]

        line_regex = r"55\.(\d+)\.(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n"
        extracted_rows = {}
        for line in get_pdf_content_fitz(filename):
            # self.logger.info(line)
            match = re.search(line_regex, line, re.DOTALL)
            if match:
                groups = list(match.groups())
                self.logger.info(f'"{groups}"')
                last_rows = groups[-4:]  # These are not needed
                required_data = groups[:-4]

                row_name = groups[0]
                fee_type = last_rows[0]
                address = " ".join(required_data[1:])

                self.logger.info(f"required_data {required_data}")
                self.logger.info(f"row_name {row_name}")
                self.logger.info(f"address {address}")
                self.logger.info(f"fee_type {fee_type}")
                self.logger.info(f"last_rows {last_rows}")

                extracted_rows[row_name] = [address.strip().replace("\n", " "), fee_type.strip()]

        if not extracted_rows:
            self.logger.warning("Will try to parse pdf of another format")
            return self.schedule_a_b_parsing_text_type1(filename)

        return extracted_rows

    def schedule_a_b_parsing(self, filename):
        # 3. 'Schedule A/B' parsing
        # Fill 'Notes' column with data from section 9, rows '55.{X}' (where X will change)
        # Take only first two columns.
        # TODO
        # ~ 3.1 (optional) Try to get addresses from data collected in 'Step 3'
        processing_funcs = {"text": self.schedule_a_b_parsing_text, "scan": self.schedule_a_b_parsing_scan}
        extracted_rows = self.parse_pdf_file(filename, processing_funcs)

        # Will clear some values now
        # for row, values in extracted_rows.items():
        #     if len(values) > 2:
        #         # Test whether was a text PDF file
        #         row_values = [p.strip() for p in values]
        #         last_row = row_values[-1]
        #         partial = last_row.strip().split()
        #         values[-1] = " ".join(partial[:-1])
        #         values.append(partial[-1])
        #         extracted_rows[row] = [" ".join(values[:-1]), values[-1]]
        #     else:
        #         # Was an image PDF file
        #         pass

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

    def schedule_d_parsing_scan(self, filename: str):
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
            pattern = re.compile(r"(.*)Creditor's Name(.*)Creditor's mailing address")
            match = re.search(pattern, full_row)
            if match:
                clear_data[ind] = {"name": match.group(1).strip(), "mailing_address": match.group(2).strip()}
        return clear_data

    def schedule_d_parsing_text1(self, data: str):
        print(f"schedule_d_parsing_text1")
        extracted_rows = {}

        pattern = re.compile(
            r"Creditor(?:Æ|'|’)?s names?(.*?)Creditor(?:Æ|'|’)?s mailing address(.*?)Creditor(?:Æ|'|’)?s", re.IGNORECASE
        )
        # 2.9 | creditor's name                         1  CITIBANK, N.A.                                                  |  Creditor's mailing address  333 W 34TH ST 9TH FLOOR NEW YORK, NY 10001                                        L  Creditor's email address
        # pattern = re.compile(r"CreditorÆs name?(.*?)CreditorÆs mailing address(.*?)CreditorÆs email", re.DOTALL)

        all_text = data

        # TODO this one
        for index, match in enumerate(re.finditer(pattern, all_text)):

            def remove_non_ascii(string):
                return "".join(char for char in string if ord(char) < 128)

            def remove_atrifacts(string):
                parts = ["  L ", " I ", " | ", "  1 ", "\n", ". "]
                for p in parts:
                    string = string.replace(p, " ")
                while "  " in string:
                    string = string.replace("  ", " ")
                return remove_non_ascii(string)

            def clear_data(part: str):
                return remove_atrifacts(part).strip()

            # print(match.groups())
            if match.group(1) and match.group(2):
                extracted_rows[index] = {
                    "name": clear_data(match.group(1)),
                    "mailing_address": clear_data(match.group(2)),
                }
            else:
                print("Failed to parse data")
        # TODO STILL NOT ALL

        return extracted_rows

    def schedule_d_parsing_text(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 45, "bottom": 80, "right": 180, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")
        pattern = re.compile(r"2\.(\d+)(.*?)Creditor(?:'|Æ)s Name(.*?)Creditor(?:'|Æ)s mailing address")
        with open("debug.txt", "w", encoding="utf-8") as outp:
            outp.write(str([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]))

        for match in re.finditer(pattern, all_text):

            group_name = match.group(1)
            extracted_rows[group_name] = {"name": match.group(2).strip(), "mailing_address": match.group(3).strip()}
            # logger.info('\n\n\n')

        if not extracted_rows:
            return self.schedule_d_parsing_text1(all_text)

        return extracted_rows
        current_group_lines = []
        is_savable = False

        rows = " ".join(all_lines).split("Part 1")[-1].split("Part 2")[0]
        parsed_rows = rows.split("Creditor's email address")
        self.logger.info(parsed_rows)

        for part in parsed_rows:
            if is_savable and not part.startswith("2."):
                current_group_lines.append(part)
            self.logger.info(part)
            match = re.search(r"2\.(\d+)(.*)?", part, re.DOTALL)
            if match:
                group_name = match.group(1)

                current_group_lines = [match.group(2).strip()]
                extracted_rows[group_name] = current_group_lines
                is_savable = True
        self.logger.info(json.dumps(extracted_rows, indent=4))

        clear_data = {}
        for ind, row in extracted_rows.items():
            full_row = " ".join(row)
            required_text_regex = r"(.*)Creditor's Name(.*)Creditor's mailing address"
            match = re.search(required_text_regex, full_row)
            if match:
                clear_data[ind] = {"name": match.group(1).strip(), "mailing_address": match.group(2).strip()}
        return clear_data

    def schedule_d_parsing(self, filename: str):
        # 4. 'Schedule D' parsing (may be tricky)
        # In section 'List Creditors Who Have Secured Claims'
        # Get "Creditor's Name" from rows '2.{X}'
        # Also get "Creditor's mailing address"
        # (not decided yet): Save to 'Creditors Info' column. Maybe will need two columns

        processing_funcs = {"text": self.schedule_d_parsing_text, "scan": self.schedule_d_parsing_scan}
        # row_parsing_funcs = {"text": schedule_d_parsing_text, "scan": schedule_d_parsing_scan}
        extracted_rows = self.parse_pdf_file(filename, processing_funcs)

        return extracted_rows
