import logging
import re
from collections import defaultdict

import pdfplumber

from utils.pdf.get_pdf_content import get_pdf_content_pdfium
from utils.pdf.get_pdf_content_from_text_ocr import get_pdf_content_from_text_ocr
from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
from utils.pdf.is_text_file import is_text_file
from utils.pdf_parsers.pdf_parser import PdfParser


class PdfParserD(PdfParser):
    cases_by_file_type = defaultdict(list)

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__.split(".")[-1])

    # Schedule D parsing code

    def schedule_d_parsing_scan(self, filename: str):
        def crop_image(image):
            w, h = image.size
            bounding_box = (
                300,
                300,
                int(w / 3) - 80,
                h - 400,
            )  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = " ".join(get_pdf_content_ocr(filename, crop_image, dpi=400))
        clear_data = {}

        all_text = all_text.replace("\n", " ").replace("’", "'").replace("maiting", "mailing")
        ind = 1

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
            r"2.*for each claim(.*?) .?Cre.?itor.?s Name(.*?)Cre|ad|cit|lor.?s? mail|ting address",
            r"2\.\d+(.*?) .?Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
            r"priority(.*?) .?Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
        ]
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE | re.MULTILINE)

            for match in re.finditer(regex, all_text):
                groups = match.groups()
                if not groups:
                    print("no groups found")
                    continue
                # print(groups)
                clear_data[ind] = {
                    "name": clear_value(groups[0]),
                    "mailing_address": clear_value(groups[1]),
                }
                if not all([v for k, v in clear_data[ind].items()]):
                    clear_data.pop(ind)
                    continue
                ind += 1

        return clear_data

    def schedule_d_parsing_scan_type_2(self, filename: str):
        def crop_image(image):
            w, h = image.size
            bounding_box = (
                150,
                300,
                int(w / 3) - 20,
                h - 400,
            )  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = " ".join(get_pdf_content_ocr(filename, crop_image, dpi=400))
        clear_data = {}

        all_text = all_text.replace("\n", " ").replace("’", "'").replace("maiting", "mailing")
        ind = 1

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

        for sym in ["[", "]", "|"]:
            all_text = all_text.replace(sym, " ")

        patterns = [
            r"2.*for each claim(.*?)Cre.?itor.?s Name(.*?)Cre|ad|cit|lor.?s? mail|ting address",
            r"2\.\d+(.*?)Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
            r"priority(.*?)Cre.?itor.?s Name(.*?)Credi.?or.?s mail|ting address",
            r"2\.\d+\s+?Cre.?itor.?s Name(.+?)Credi.?or.?s mailing address(.+?)Credi.?or.?s email address",
        ]
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE | re.MULTILINE)

            for match in re.finditer(regex, all_text):
                groups = match.groups()
                if not groups:
                    print("no groups found")
                    continue
                print(groups)
                clear_data[ind] = {
                    "name": clear_value(groups[0]),
                    "mailing_address": clear_value(groups[1]),
                }
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
                clear_data[ind] = {
                    "name": match.group(1).strip(),
                    "mailing_address": match.group(2).strip(),
                }
        return clear_data

    def schedule_d_parsing_text2(self, filename: str):
        self.logger.info(f"schedule_d_parsing_text2")
        extracted_rows = {}

        pattern = re.compile(
            r"Creditor(?:Æ|'|’)?s names?(.*?)Creditor(?:Æ|'|’)?s mailing address(.*?)Creditor(?:Æ|'|’)?s",
            re.IGNORECASE,
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
            r"Creditor(?:Æ|'|’)?s names?(.*?)Creditor(?:Æ|'|’)?s mailing address(.*?)Creditor(?:Æ|'|’)?s",
            re.IGNORECASE,
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

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace(
            "\n", ""
        )
        pattern = re.compile(
            r"2\.(\d+)(.*?)Creditor(?:'|Æ)s Name(.*?)Creditor(?:'|Æ)s mailing address"
        )
        with open("debug_schedule_d.txt", "w", encoding="utf-8") as outp:
            outp.write(str(all_text))

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {
                "name": match.group(2).strip(),
                "mailing_address": match.group(3).strip(),
            }

        if not extracted_rows:
            return self.schedule_d_parsing_text2(filename)

        return extracted_rows

    def schedule_d_parsing_text_1(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 30, "bottom": 80, "right": 180, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace(
            "\n", ""
        )
        pattern = re.compile(
            r"2\.(\d+)(.*?)Creditor.?s Name(.*?)Creditor.?s mailing address",
            re.IGNORECASE,
        )

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {
                "name": match.group(2).strip(),
                "mailing_address": match.group(3).strip(),
            }

        return extracted_rows

    def schedule_d_parsing_text_2(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 30, "bottom": 80, "right": 190, "top": 792 - 40}

        all_text = " ".join([l for l in get_pdf_content_pdfium(filename, 0, boundaries)]).replace(
            "\n", ""
        )
        pattern = re.compile(
            r"2\.(\d+).?.?Creditor.?s Name(.*?)Creditor.?s mailing address(.*?)Creditor.?s",
            re.IGNORECASE,
        )

        for match in re.finditer(pattern, all_text):
            group_name = match.group(1)
            extracted_rows[group_name] = {
                "name": match.group(2).strip(),
                "mailing_address": match.group(3).strip(),
            }

        return extracted_rows

    def schedule_d_parsing_text_3(self, filename: str):
        print(f"schedule_d_parsing_text_3")
        extracted_rows = {}

        pattern = re.compile(
            r"(:?Creditor.?s names?|for each clair)(.*?)Creditor.?s mailing address(.*?)Creditor.?s",
            re.IGNORECASE,
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
        # File type signarure 'lem Harbor Power Developme Part 1'
        # file_type type4
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

    def schedule_d_parsing_text_type_mod(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (20, 40, int(w / 3) - 130, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image, dpi=600):
            all_text += page_text

        all_text = all_text.replace("\n", " ")
        if not all_text:
            raise RuntimeError(f"Failed to retrieve data from file: {filename}")

        pattern = re.compile(r" 2\.?(\d+) (.+?) Date debt was incurred")
        for match in re.finditer(pattern, all_text):
            if match:
                groups = list(match.groups())
                row_name = groups[0].strip()
                # NOTE: Works not always
                # p = groups[1].split("  ", maxsplit=1)
                # if len(p) < 2:
                #     p = (p[0], "")
                # name = p[0]
                # m_addr = p[1]
                name = groups[1]
                m_addr = ""
                extracted_rows[row_name] = {
                    "name": name.strip(),
                    "mailing_address": m_addr.strip(),
                }
        return extracted_rows

    def schedule_d_parsing_text_type_4_mod(self, filename: str):
        all_text = ""
        extracted_rows = {}
        with pdfplumber.open(filename) as pdf:
            bounding_box = (20, 40, 220, 612)
            for page_index in range(len(pdf.pages)):
                cropped_page = pdf.pages[page_index].within_bbox(
                    bounding_box, relative=False, strict=True
                )
                pdf_str = cropped_page.extract_text(use_text_flow=True)

                while "_" in pdf_str:
                    pdf_str = pdf_str.replace("_", "")

                all_text += " " + pdf_str.replace("\n", " ")

        glob_pattern = re.compile(
            r"(2\.1.*)Schedule D",
            re.IGNORECASE | re.MULTILINE,
        )
        found_text = re.search(glob_pattern, all_text).groups()[0]
        for part in found_text.split("2."):
            if not part.strip():
                continue
            pattern = re.compile(r"(\d+)(.*)")
            groups = re.search(pattern, part).groups()

            group_name = groups[0]
            extracted_rows[group_name] = {
                "name": groups[1].strip(),
                "mailing_address": "",
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
            all_text = " ".join(
                [l for l in get_pdf_content_pdfium(filename, 0, boundaries)]
            ).replace("\n", "")

            # return all_text[:128].split('2.1')[0]
            type_signature = all_text.split(":")[0][:48]

            # type_1_files = ["ill in this information to identify the c ebtor "]
            # if any([t in type_signature for t in type_1_files]):
            #     return "type1"

            # type_2_files = ["btor ted States Bankruptcy Court for the"]
            # if any([t in type_signature for t in type_2_files]):
            #     return "type2"

            # NOTE SHOULD NOT WORK
            # type_3_files = ["Official Form 206D  Schedule D"]
            # if any([t in type_signature for t in type_3_files]):
            #     return "type3"

            if "" in type_signature:
                return "type_mod"

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

        processing_funcs = {
            "text": self.schedule_d_parsing_text_4,
            # "type1": self.schedule_d_parsing_text_1,
            # "type2": self.schedule_d_parsing_text_2,
            # "type3": self.schedule_d_parsing_text_3,
            "type4": self.schedule_d_parsing_text_4,  # Suppose most general one
            "type_mod": self.schedule_d_parsing_text_type_mod,  # OCR for utf-16 files
            "type4_mod": self.schedule_d_parsing_text_type_4_mod,  # When no 'creditors' text in file
            "scan": self.schedule_d_parsing_scan,
            "scan2": self.schedule_d_parsing_scan_type_2,
        }

        try:
            extracted_rows = self.parse_pdf_file(
                filename, processing_funcs, discover_pdf_type_schedule_d
            )
            return extracted_rows
        except Exception as err:

            self.logger.error(f"schedule d file {filename}: parsing failed due to {str(err)}")
        return {}
