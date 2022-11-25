from collections import defaultdict
from itertools import zip_longest, islice
import json
import logging
import re
import pdfplumber
from itertools import groupby
from collections import defaultdict, OrderedDict


from scrapy.utils.project import get_project_settings
from temp_pdf_sampler_d import get_pages_where_found
from utils.get_parsed_address import get_parsed_address
from utils.pdf.get_pdf_content import get_pdf_content_fitz, get_pdf_content_pdfium
from utils.pdf.get_pdf_content_from_text_ocr import get_pdf_content_from_text_ocr

from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
import re

from utils.pdf.is_text_file import is_text_file


class PdfParser:
    cases_by_file_type = defaultdict(list)
    cases_by_file_type_ab = defaultdict(list)

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__.split(".")[-1])

    def parse_pdf_file(self, filename, processing_funcs, type_discover_func, **kwargs):
        file_type = type_discover_func(filename)
        self.logger.debug(f"file_type {file_type}")

        try:
            extracted_rows = processing_funcs[file_type](filename)

            funcs = [v for k, v in processing_funcs.items()]
            if not extracted_rows:
                # Overcome for empty result
                for func in funcs:
                    extracted_rows = func(filename)
                    if extracted_rows:
                        break

        except KeyError as ke:
            self.logger.error(f"Missing processing function for file_type: {file_type}, file {filename}")
            return {}
        return extracted_rows

    def schedule_a_b_parsing_scan(self, filename: str):
        # TODO REDO THIS
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

        part_nine = all_text
        # part_nine = all_text.split("55. Any building, other improved real estate")[-1]
        with open("sample_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(part_nine)

        # TODO add 55.2 check
        pattern = re.compile(r"land which the debtor owns or i?\s*(.*?)  ([^\s].*?)55\.(\d)(.*)Total of Part 9")
        for match in re.finditer(pattern, all_text):
            groups = list(match.groups())
            self.logger.info(f'"{groups}"')
            groups = [g.strip() for g in groups]
            # ['5755 Bayport Blvd.                          ', 'Fee Simple', '1', ' 5755 Bayport Blvd., Seabrook, TX 77586']
            group_name = groups[-2]
            extracted_rows[group_name] = {"address": groups[0] + " " + groups[-1], "fee_type": groups[1]}

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
        # TODO GET PAGE index FROM REQUIRED FILE FOR FASTER PROCESSING

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

        # line_regex =
        extracted_rows = {}
        boundaries = {"left": 35, "bottom": 40, "right": 300, "top": 792 - 40}
        all_text = " ".join([line for line in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", " ")

        with open("debug_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)

        # self.logger.info(line)
        pattern = re.compile(r"55\.(\d+)\.(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n(.*)\n")
        for match in re.finditer(pattern, all_text):
            # match = re.search(line_regex, line, re.DOTALL)
            if match:
                groups = list(match.groups())
                self.logger.info(f'"{groups}"')
                last_rows = groups[-4:]  # These are not needed
                required_data = groups[:-4]

                row_name = groups[0]
                fee_type = last_rows[0]
                address = " ".join(required_data[1:])

                # self.logger.info(f"required_data {required_data}")
                # self.logger.info(f"row_name {row_name}")
                # self.logger.info(f"address {address}")
                # self.logger.info(f"fee_type {fee_type}")
                # self.logger.info(f"last_rows {last_rows}")

                extracted_rows[row_name] = {"address": address.strip().replace("\n", " "), "fee_type": fee_type.strip()}

        if not extracted_rows or not all([v for k, v in extracted_rows.items()]):
            self.logger.warning("Will try to parse pdf of another format")
            return self.schedule_a_b_parsing_text_type1(filename)

        return extracted_rows

    def schedule_ab_parsing_text_type_4(self, filename: str):
        required_pages = get_pages_where_found(filename, ["55.", "Part 9", "Part 10"])
        extracted_rows = {}
        with pdfplumber.open(filename) as pdf:
            bounding_box = (30, 40, 280, 792 - 40)
            for page_index in required_pages:
                cropped_page = pdf.pages[page_index].within_bbox(bounding_box, relative=False, strict=True)
                pdf_str = cropped_page.extract_words(use_text_flow=True)

                page_text = [d["text"] for d in pdf_str]
                page_text = " ".join(page_text)
                if "55." not in page_text:
                    continue

                groups = groupby(pdf_str, key=lambda x: int(x["top"]))
                new_text = ""
                lines = defaultdict(str)
                for index, part in groups:
                    part = list(part)
                    # TODO
                    if index > 190 and index < 345:
                        lines[f'{int(part[0]["x0"])}:{index}'] = " ".join([d["text"] for d in part])

                    new_text += " ".join([d["text"] for d in part])
                    new_text += "\n"

                sorted_dict = OrderedDict(sorted(lines.items(), key=lambda x: int(x[0].split(":")[0])))

                lines = dict(sorted_dict)

                def yield_subgroups(group, subgroup_test):
                    subgroup = []
                    for i, j in zip_longest(group, islice(group, 1, None)):
                        if subgroup_test(i, j):
                            yield subgroup
                            subgroup = []
                        else:
                            subgroup.append(i)
                    yield subgroup

                def checker(i, j):
                    if not j:
                        return True
                    return abs(int(j.split(":")[1]) - int(i.split(":")[1])) not in [9, 10]

                subgrouped_data = list(yield_subgroups(lines.keys(), checker))
                formatted_rows = []
                for l in subgrouped_data:
                    if l:
                        formatted_rows.append([" ".join([lines[i] for i in l])])

                found_rows_len = len(formatted_rows)

                for k, row, fee in zip_longest(
                    list(lines.keys())[:found_rows_len], formatted_rows, list(lines.keys())[-found_rows_len:]
                ):
                    extracted_rows[lines[k]] = {"address": row, "fee_type": lines[fee]}

        return extracted_rows

    def schedule_ab_parsing_text_type_3(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 35, "bottom": 40, "right": 300, "top": 792 - 40}
        all_text = "   ".join([line for line in get_pdf_content_pdfium(filename, 0, boundaries)])
        # print(all_text)
        main_pattern = re.compile(r"55\.(.*)5?6?.*Total of Part 9")
        all_text = re.search(main_pattern, all_text).group(1)
        all_text = "55." + all_text
        with open("debug_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)

        std_pdf_text = "Official Form 206A/B Schedule A/B"
        new_page_pattern = re.compile(r"\s?(\d?\d?\d?)\.?Page.*if available\.(.*)")
        for part in all_text.split("55."):

            pattern = re.compile(r"\s?(\d?\d?\d?)\.?(.*)")
            for match in re.finditer(pattern, part):
                if match:
                    groups = list(match.groups())
                    if groups[0] == "":
                        continue

                    row_name = groups[0].strip()
                    fee_type = ""  # TODO
                    address = groups[1].strip()
                    if "Part 9: Real property - detail" in address:
                        continue

                    if "TOTAL OF ASSETS LISTED BELOW" in address:
                        # TODO? group index is -1 because of this
                        extracted_rows[row_name] = {"address": "TOTAL OF ASSETS LISTED BELOW", "fee_type": ""}
                        address = address.split("TOTAL OF ASSETS LISTED BELOW")[-1]
                        # row_name = str(int(row_name) + 1)
                    # else:
                    #     row_name = str(int(row_name) + 1)

                    contains_new_page = re.search(new_page_pattern, address)
                    if contains_new_page:
                        address = contains_new_page.groups()[1]

                    parsed_address = address.replace("   ", " ").replace("  ", " ")
                    for word in ["Fee", "FEE", "LEASED", "OWNED", "100%", "100%", "Equitable"]:
                        if word in parsed_address:
                            if word.upper() in parsed_address:
                                word = word.upper()

                            partial_addr = parsed_address.split(word)
                            fee_type = word + partial_addr[-1]
                            # fee_type = fee_type.split("56")[0]
                            fee_type = fee_type.split(std_pdf_text)[0].strip()
                            if fee_type.endswith("."):
                                fee_type = fee_type[:-1]
                            # fee_type = fee_type.split('$')[0]
                            parsed_address = partial_addr[0]
                            break
                    extracted_rows[row_name] = {"address": parsed_address.strip(), "fee_type": fee_type.strip()}
        return extracted_rows

    def schedule_ab_parsing_text_type_2(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (75, 40, int(w / 2) - 20, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image, search_text=["55.", "Part 9"]):
            all_text += page_text

        all_text = all_text.replace("\n", " ")
        with open("debug_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)
        main_pattern = re.compile(r"55\.(.*)56\.?.*Total of Part 9")
        all_text = re.search(main_pattern, all_text).group(1)

        for part in all_text.split("55."):
            pattern = re.compile(r"(\d)(\d)?(\d)?\.?(.*)")
            for match in re.finditer(pattern, part):
                if match:
                    groups = list(match.groups())
                    row_name = groups[0].strip()
                    fee_type = ""  # TODO
                    address = groups[1]
                    parsed_address = address.replace("   ", " ")
                    for word in ["Fee", "FEE", "LEASED", "OWNED", "100%", "Equitable"]:
                        if word.upper() in parsed_address:
                            word = word.upper()

                        if word.lower() in parsed_address.lower():
                            fee_type = word + parsed_address.split(word)[-1]
                            parsed_address = parsed_address.split(word)[0]
                    extracted_rows[row_name] = {"address": parsed_address.strip(), "fee_type": fee_type.strip()}
        return extracted_rows

    def schedule_ab_parsing_text_type_1(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 10, "bottom": 40, "right": 300, "top": 792 - 40}
        all_text = "   ".join([line for line in get_pdf_content_pdfium(filename, 0, boundaries)])

        main_pattern = re.compile(r"55\.(.*)5?6?.*Total of Part 9")
        all_text = re.search(main_pattern, all_text).group(1)
        # with open("debug_schedule_ab.txt", "w", encoding="utf-8") as outp:
        #     outp.write(all_text)

        std_pdf_text = "Official Form 206A/B Schedule A/B"

        for part in all_text.split("55."):
            pattern = re.compile(r"(\d\d?\d?)\.?(.*)")
            for match in re.finditer(pattern, part):
                if match:
                    groups = list(match.groups())
                    row_name = str(groups[0]).strip()
                    fee_type = ""  # TODO
                    address = str(groups[1]).strip()

                    if address.endswith("56.") or address.endswith("56"):
                        address = "".join(reversed(address)).split("65", maxsplit=1)[-1]
                        address = "".join(reversed(address))
                    if "Software Copyright (c)" in address or "Software Copyright (c)" in address:
                        print(f"Skipping row {address}")
                        continue
                    parsed_address = address.replace("   ", " ").replace("  ", " ")
                    for word in ["Fee", "FEE", "LEASED", "OWNED", "Owned", "100%", "100%", "Equitable"]:
                        if word.upper() in parsed_address:
                            word = word.upper()

                        if word in parsed_address:
                            partial_addr = parsed_address.split(word)
                            fee_type = word + partial_addr[-1]
                            if len(fee_type) > len(partial_addr[0]):
                                # Will cause data mess
                                break

                            fee_type = fee_type.split(std_pdf_text)[0].strip()
                            if fee_type.endswith("."):
                                fee_type = fee_type[:-1]

                            parsed_address = partial_addr[0]
                            break

                    extracted_rows[row_name] = {
                        "address": str(parsed_address).strip(),
                        "fee_type": str(fee_type).strip(),
                    }
        return extracted_rows

    def schedule_a_b_parsing(self, filename):
        # 3. 'Schedule A/B' parsing
        # Fill 'Notes' column with data from section 9, rows '55.{X}' (where X will change)
        # Take only first two columns.
        # TODO
        # ~ 3.1 (optional) Try to get addresses from data collected in 'Step 3'

        def discover_pdf_type_schedule_ab(filename: str) -> str:

            file_type = "text" if is_text_file(filename) else "scan"
            if file_type == "scan":
                return file_type

            boundaries = {"left": 40, "bottom": 80, "right": 180, "top": 792 - 40}
            all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")
            type_signature = all_text.split(":")[0][:48]
            self.logger.info(f"File type signarure '{type_signature}'")
            if type_signature == "":
                return "scan"

            type_1_files = [
                "his information to identify the",
                "in this information to identify the",
                "Fill in this information to",
                "Official Form 206A/B chedule A/B",
                "nited States Bankruptcy",
                "fficial Form 206A/B chedule A/B",
                "Fill in this information to iden ebtor name",
                " nited States Ba",
                "Fill in this information to identify",
                "this information to identify",
                "Cash and cash equivalents edule A/B",
            ]
            # NOTE, "Cash and cash equivalents edule A/B" has fee data messed up
            if any([t in type_signature for t in type_1_files]):
                return "type1"

            if type_signature.startswith("this information to identify the case"):
                # NOTE: This type looks like duplicated, but is not
                return "type1"

            type_2_files = [
                "Official Form 206A/B  Schedule A/B",
                "ficial Form 206A/B hedule A/B",
                " this information to identify the case",
                "South ase number (If known)",
                ". Go to Part 3.",
            ]
            if any([t in type_signature for t in type_2_files]):
                return "type2"

            type_3_files = ["Part 1"]
            if any([t in type_signature for t in type_3_files]):
                return "type3"

            if "Official Form 206A/B Schedule A/B" in type_signature:
                return "type4"

            return type_signature

        file_type = discover_pdf_type_schedule_ab(filename)
        self.cases_by_file_type_ab[file_type].append(filename)
        self.logger.info("schedule_a_b_parsing")
        self.logger.info(self.cases_by_file_type_ab)

        processing_funcs = {
            # "text": self.schedule_a_b_parsing_text,
            "type1": self.schedule_ab_parsing_text_type_1,  # text
            "type2": self.schedule_ab_parsing_text_type_2,  # OCR
            "type3": self.schedule_ab_parsing_text_type_3,  # text incorrect order
            "type4": self.schedule_ab_parsing_text_type_4,  # text, mess in ordering
            "scan": self.schedule_a_b_parsing_scan,
        }

        # self.logger.info(processing_funcs)
        try:
            extracted_rows = self.parse_pdf_file(filename, processing_funcs, discover_pdf_type_schedule_ab)
            # TODO remove 'A/B Schedule A/B Assets Debtor Whetstone Partners, LLP Name;'
            # TODO remove '. ' from addresses
            extracted_rows = self.fix_schedule_ab_data(extracted_rows)
            return extracted_rows
        except Exception as err:
            self.logger.error(f"schedule ab file {filename}: parsing failed due to {str(err)}")
        return {}

    def _convert_to_string(self, value):
        if isinstance(value, list):
            return " ".join(value)
        return str(value)

    def fix_schedule_ab_data(self, extracted_rows: dict) -> dict:
        new_dict = {}
        for key, value in extracted_rows.items():
            if not value.get("fee_type", ""):
                value["fee_type"] = ""

            value["fee_type"] = self._convert_to_string(value["fee_type"])
            if not value.get("address", ""):
                value["address"] = ""

            value["address"] = self._convert_to_string(value["address"])
            if value["address"].startswith("."):
                value["address"] = value["address"][1:].strip()
            while "_" in value["address"]:
                value["address"] = value["address"].replace("_", " ")

            while "  " in value["address"]:
                value["address"] = value["address"].replace("  ", " ")

            new_dict[key] = value
        return new_dict

    def schedule_d_parsing_scan(self, filename: str):
        def crop_image(image):
            w, h = image.size
            bounding_box = (300, 300, int(w / 3) - 80, h - 400)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = " ".join(get_pdf_content_ocr(filename, crop_image, dpi=400))
        clear_data = {}

        all_text = all_text.replace("\n", " ").replace("’", "'").replace("maiting", "mailing")
        ind = 1
        # print(all_text)
        # print("\n\n\n")

        def clear_value(string):
            if not string:
                return string
            string = string.replace(" i ", " ")
            string = string.replace("|", "")
            string = string.replace(";", "")
            string = string.replace("_", " ")
            string = string.replace(". ", " ")
            string = string.replace("E! Paso", "El Paso")
            string = string.replace(": :", " ")
            while "  " in string:
                string = string.replace("  ", " ")
            return string.strip()

        patterns = [
            r"2.*for each claim(.*?) Cre.?itor.?s Name(.*?)Cre|ad|cit|lor.?s? mail|ting address",
            r"2\.\d+(.*?) Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
            r"priority(.*?) Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
        ]
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE | re.MULTILINE)

            for match in re.finditer(regex, all_text):
                groups = match.groups()
                if not groups:
                    print("no groups found")
                    continue
                # print(groups)
                clear_data[ind] = {"name": clear_value(groups[0]), "mailing_address": clear_value(groups[1])}
                if not all([v for k, v in clear_data[ind].items()]):
                    clear_data.pop(ind)
                    continue
                ind += 1

        return clear_data

    def schedule_d_parsing_scan_old(self, filename: str):
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

    def schedule_d_parsing_text2(self, filename: str):
        self.logger.info(f"schedule_d_parsing_text2")
        extracted_rows = {}

        pattern = re.compile(
            r"Creditor(?:Æ|'|’)?s names?(.*?)Creditor(?:Æ|'|’)?s mailing address(.*?)Creditor(?:Æ|'|’)?s", re.IGNORECASE
        )
        # 2.9 | creditor's name                         1  CITIBANK, N.A.                                                  |  Creditor's mailing address  333 W 34TH ST 9TH FLOOR NEW YORK, NY 10001                                        L  Creditor's email address
        # pattern = re.compile(r"CreditorÆs name?(.*?)CreditorÆs mailing address(.*?)CreditorÆs email", re.DOTALL)
        # {"left": 45, "bottom": 80, "right": 180, "top": 792 - 40}
        def crop_image(image):
            w, h = image.size
            bounding_box = (70, 40, int(w / 3) + 60, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image):
            # if any(("Part 8" in page_text, "Part 9" in page_text, "Part 10" in page_text)):
            all_text += page_text
            #
        all_text = all_text.replace("\n", " ")

        # all_text = data
        with open("debug_schedule_d_alt1.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)

        # TODO this one
        for index, match in enumerate(re.finditer(pattern, all_text)):

            def remove_non_ascii(string):
                return "".join(char for char in string if ord(char) < 128)

            def remove_atrifacts(string):
                parts = ["  L ", " I ", " | ", "  1 ", "\n", ". "]
                for p in parts:
                    string = string.replace(p, " ")
                while "_" in string:
                    string = string.replace("_", " ")
                while "  " in string:
                    string = string.replace("  ", " ")

                def get_c_or_o(part):
                    return "c/o" if part == "clo" or part == "c1o" else part

                string = " ".join([get_c_or_o(p) for p in string.split(" ")])
                return remove_non_ascii(string)

            def clear_data(part: str):
                return remove_atrifacts(part).strip()

            if match.group(1) and match.group(2):
                extracted_rows[index] = {
                    "name": clear_data(match.group(1)),
                    "mailing_address": clear_data(match.group(2)),
                }
            else:
                self.logger.info("Failed to parse data")
        # TODO STILL NOT ALL

        return extracted_rows

    def schedule_d_parsing_text1(self, filename: str):
        # TODO
        raise NotImplementedError()
        self.logger.info(f"schedule_d_parsing_text1")
        extracted_rows = {}

        pattern = re.compile(
            r"Creditor(?:Æ|'|’)?s names?(.*?)Creditor(?:Æ|'|’)?s mailing address(.*?)Creditor(?:Æ|'|’)?s", re.IGNORECASE
        )
        # 2.9 | creditor's name                         1  CITIBANK, N.A.                                                  |  Creditor's mailing address  333 W 34TH ST 9TH FLOOR NEW YORK, NY 10001                                        L  Creditor's email address
        # pattern = re.compile(r"CreditorÆs name?(.*?)CreditorÆs mailing address(.*?)CreditorÆs email", re.DOTALL)

        all_text = data
        with open("debug_schedule_d_alt1.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)

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

            # self.logger.info(match.groups())
            if match.group(1) and match.group(2):
                extracted_rows[index] = {
                    "name": clear_data(match.group(1)),
                    "mailing_address": clear_data(match.group(2)),
                }
            else:
                self.logger.info("Failed to parse data")
        # TODO STILL NOT ALL

        return extracted_rows

    def schedule_d_parsing_text(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 45, "bottom": 80, "right": 180, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")
        pattern = re.compile(r"2\.(\d+)(.*?)Creditor(?:'|Æ)s Name(.*?)Creditor(?:'|Æ)s mailing address")
        with open("debug_schedule_d.txt", "w", encoding="utf-8") as outp:
            outp.write(str(all_text))

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {"name": match.group(2).strip(), "mailing_address": match.group(3).strip()}

        if not extracted_rows:
            return self.schedule_d_parsing_text2(filename)

        return extracted_rows

    def schedule_d_parsing_text_1(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 30, "bottom": 80, "right": 180, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")
        pattern = re.compile(r"2\.(\d+)(.*?)Creditor.?s Name(.*?)Creditor.?s mailing address", re.IGNORECASE)

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {"name": match.group(2).strip(), "mailing_address": match.group(3).strip()}

        return extracted_rows

    def schedule_d_parsing_text_2(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 30, "bottom": 80, "right": 190, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")
        pattern = re.compile(
            r"2\.(\d+).?.?Creditor.?s Name(.*?)Creditor.?s mailing address(.*?)Creditor.?s", re.IGNORECASE
        )

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {"name": match.group(2).strip(), "mailing_address": match.group(3).strip()}

        return extracted_rows

    def schedule_d_parsing_text_3(self, filename: str):
        print(f"schedule_d_parsing_text_3")
        extracted_rows = {}

        pattern = re.compile(
            r"(:?Creditor.?s names?|for each clair)(.*?)Creditor.?s mailing address(.*?)Creditor.?s", re.IGNORECASE
        )
        # 2.9 | creditor's name                         1  CITIBANK, N.A.                                                  |  Creditor's mailing address  333 W 34TH ST 9TH FLOOR NEW YORK, NY 10001                                        L  Creditor's email address
        # pattern = re.compile(r"CreditorÆs name?(.*?)CreditorÆs mailing address(.*?)CreditorÆs email", re.DOTALL)
        # {"left": 45, "bottom": 80, "right": 180, "top": 792 - 40}
        def crop_image(image):
            w, h = image.size
            bounding_box = (50, 20, int(w / 3) + 35, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image):
            # if any(("Part 8" in page_text, "Part 9" in page_text, "Part 10" in page_text)):
            all_text += page_text
            #
        all_text = all_text.replace("\n", " ")

        # all_text = data
        with open("debug_schedule_d_alt1.txt", "w", encoding="utf-8") as outp:
            outp.write(all_text)

        # TODO this one
        for index, match in enumerate(re.finditer(pattern, all_text)):

            def remove_non_ascii(string):
                return "".join(char for char in string if ord(char) < 128)

            def remove_atrifacts(string):
                parts = ["  L ", " I ", " | ", "  1 ", "\n", ". "]
                for p in parts:
                    string = string.replace(p, " ")
                while "_" in string:
                    string = string.replace("_", " ")
                while "  " in string:
                    string = string.replace("  ", " ")

                def get_c_or_o(part):
                    return "c/o" if part == "clo" or part == "c1o" else part

                string = " ".join([get_c_or_o(p) for p in string.split(" ")])
                return remove_non_ascii(string)

            def clear_data(part: str):
                return remove_atrifacts(part).strip()

            if match.group(1) and match.group(2):
                extracted_rows[index + 1] = {
                    "name": clear_data(match.group(1)),
                    "mailing_address": clear_data(match.group(2)),
                }
            else:
                print("Failed to parse data")
        # TODO STILL NOT ALL

        return extracted_rows

    def schedule_d_parsing_text_4(self, filename: str):
        import pdfplumber

        # File type signarure 'lem Harbor Power Developme Part 1'
        # file_type type4
        # TODO FIX ounding box (20, 40, 220, 752) is not fully within parent page bounding box (0, 0, 792.06, 612.04)

        extracted_rows = {}
        with pdfplumber.open(filename) as pdf:
            for page_index in range(len(pdf.pages)):
                curr_page = pdf.pages[page_index]
                bounding_box = (20, 40, 220, int(curr_page.height) - 40)
                cropped_page = curr_page.within_bbox(bounding_box, relative=False, strict=True)
                pdf_str = cropped_page.extract_text(use_text_flow=True)

                while "_" in pdf_str:
                    pdf_str = pdf_str.replace("_", "")
                pattern = re.compile(
                    r"2\.(\d+).?.?Creditor.?s Name(.*?)Creditor.?s mailing address(.*?)Creditor.?s",
                    re.IGNORECASE | re.MULTILINE,
                )
                all_text = pdf_str.replace("\n", " ")
                for match in re.finditer(pattern, all_text):
                    group_name = match.group(1)
                    extracted_rows[group_name] = {
                        "name": match.group(2).strip(),
                        "mailing_address": match.group(3).strip(),
                    }

        return extracted_rows

    def schedule_d_parsing(self, filename: str):
        # 4. 'Schedule D' parsing (may be tricky)
        # In section 'List Creditors Who Have Secured Claims'
        # Get "Creditor's Name" from rows '2.{X}'
        # Also get "Creditor's mailing address"
        # (not decided yet): Save to 'Creditors Info' column. Maybe will need two columns

        def discover_pdf_type_schedule_d(filename: str):
            def get_pdf_file_type(filename):
                return "text" if is_text_file(filename) else "scan"

            file_type = get_pdf_file_type(filename)
            if file_type == "scan":
                return file_type

            boundaries = {"left": 45, "bottom": 80, "right": 180, "top": 792 - 40}
            all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace("\n", "")

            # return all_text[:128].split('2.1')[0]
            type_signature = all_text.split(":")[0][:48]

            type_1_files = ["ill in this information to identify the c ebtor "]

            # if any([t in type_signature for t in type_1_files]):
            #     return "type1"

            # type_2_files = ["btor ted States Bankruptcy Court for the"]
            # if any([t in type_signature for t in type_2_files]):
            #     return "type2"

            # NOTE SHOULD NOT WORK
            # type_3_files = ["Official Form 206D  Schedule D"]
            # if any([t in type_signature for t in type_3_files]):
            #     return "type3"

            type_4_files = [
                "Official Form 206D  chedule D",
                "Official Form 206D Schedule D",
                "or name d States Bankruptcy Court for the",
            ]
            if any([t in type_signature for t in type_4_files]):
                return "type4"
            # TODO DEBUG PURPOSES
            return "type4"

            return type_signature

        part = discover_pdf_type_schedule_d(filename)
        self.cases_by_file_type[part].append(filename)
        self.logger.info("schedule_d_parsing")
        # self.logger.info(json.dumps(self.cases_by_file_type, indent=4))

        processing_funcs = {
            "text": self.schedule_d_parsing_text_4,
            # "type1": self.schedule_d_parsing_text_1,
            # "type2": self.schedule_d_parsing_text_2,
            # "type3": self.schedule_d_parsing_text_3,
            "type4": self.schedule_d_parsing_text_4,  # Suppose most general one
            "scan": self.schedule_d_parsing_scan,
        }

        try:
            extracted_rows = self.parse_pdf_file(filename, processing_funcs, discover_pdf_type_schedule_d)
            return extracted_rows
        except Exception as err:

            self.logger.error(f"schedule d file {filename}: parsing failed due to {str(err)}")
        return {}
