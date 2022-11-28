import re
from collections import OrderedDict, defaultdict
from itertools import groupby, islice, zip_longest

import pdfplumber

from temp_pdf_sampler_d import get_pages_where_found
from utils.pdf.get_pdf_content import get_pdf_content_pdfium
from utils.pdf.get_pdf_content_from_text_ocr import get_pdf_content_from_text_ocr
from utils.pdf.get_pdf_content_ocr import get_pdf_content_ocr
from utils.pdf.is_text_file import is_text_file
from utils.pdf_parsers.pdf_parser import PdfParser


class PdfParserAB(PdfParser):
    cases_by_file_type = defaultdict(list)
    cases_by_file_type_ab = defaultdict(list)

    def schedule_a_b_parsing_scan(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (20, 50, int(w / 2), h - 80)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_pages_text = []
        for l in get_pdf_content_ocr(filename, crop_image):
            if "55." in l:
                all_pages_text.append(l)
            if "Part 10:" in l:
                break
        all_text = " ".join(all_pages_text)
        all_text = all_text.replace("\n", " ")

        pattern = re.compile(r"55\.(\d+)(.*)(:?55|56)", re.MULTILINE | re.IGNORECASE)
        for match in re.finditer(pattern, all_text):
            groups = match.groups()
            group_name = groups[0]

            parts = [p.strip() for p in groups[1].split("   ") if p.strip()]
            address = parts[0]
            if len(parts) > 1:
                fee_type = parts[1]
                extracted_rows[group_name] = {"address": address, "fee_type": fee_type}
            else:
                extracted_rows[group_name] = {"address": address, "fee_type": ""}

        return extracted_rows

    def schedule_a_b_parsing_scan_type_2(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (20, 50, int(w / 2), h - 80)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_pages_text = []
        for l in get_pdf_content_ocr(filename, crop_image):
            if "55." in l:
                all_pages_text.append(l)
            if "Part 10:" in l:
                break
        all_text = " ".join(all_pages_text)
        all_text = all_text.replace("\n", " ")

        pattern = re.compile(
            r"55\.(\d+).+if available(.+?)(:?55|56)", re.MULTILINE | re.IGNORECASE
        )
        for match in re.finditer(pattern, all_text):
            groups = match.groups()
            group_name = groups[0]

            parts = [p.strip() for p in groups[1].split("   ") if p.strip()]

            address = parts[0]
            if len(parts) > 1:
                fee_type = parts[1]
                if "Total of Part 9" in fee_type:
                    fee_type = fee_type.split("Total of Part 9")[0]
                extracted_rows[group_name] = {"address": address, "fee_type": fee_type}
            else:
                extracted_rows[group_name] = {"address": address, "fee_type": ""}

        return extracted_rows

    def schedule_ab_parsing_text_type_4(self, filename: str):
        self.logger.debug("schedule_ab_parsing_text_type_4")
        required_pages = get_pages_where_found(filename, ["55.", "Part 9", "Part 10"])
        extracted_rows = {}
        with pdfplumber.open(filename) as pdf:
            bounding_box = (30, 40, 280, 792 - 40)
            for page_index in required_pages:
                cropped_page = pdf.pages[page_index].within_bbox(
                    bounding_box, relative=False, strict=True
                )
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
                        lines[f'{int(part[0]["x0"])}:{index}'] = " ".join(
                            [d["text"] for d in part]
                        )

                    new_text += " ".join([d["text"] for d in part])
                    new_text += "\n"

                sorted_dict = OrderedDict(
                    sorted(lines.items(), key=lambda x: int(x[0].split(":")[0]))
                )

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
                    return abs(int(j.split(":")[1]) - int(i.split(":")[1])) not in [
                        9,
                        10,
                    ]

                subgrouped_data = list(yield_subgroups(lines.keys(), checker))
                formatted_rows = []
                for l in subgrouped_data:
                    if l:
                        formatted_rows.append([" ".join([lines[i] for i in l])])

                found_rows_len = len(formatted_rows)

                for k, row, fee in zip_longest(
                    list(lines.keys())[:found_rows_len],
                    formatted_rows,
                    list(lines.keys())[-found_rows_len:],
                ):
                    extracted_rows[lines[k]] = {"address": row, "fee_type": lines[fee]}

        return extracted_rows

    def schedule_ab_parsing_text_type_3(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 35, "bottom": 40, "right": 300, "top": 792 - 40}
        all_text = "   ".join(
            [line for line in get_pdf_content_pdfium(filename, 0, boundaries)]
        )
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
                        extracted_rows[row_name] = {
                            "address": "TOTAL OF ASSETS LISTED BELOW",
                            "fee_type": "",
                        }
                        address = address.split("TOTAL OF ASSETS LISTED BELOW")[-1]
                        # row_name = str(int(row_name) + 1)
                    # else:
                    #     row_name = str(int(row_name) + 1)

                    contains_new_page = re.search(new_page_pattern, address)
                    if contains_new_page:
                        address = contains_new_page.groups()[1]

                    parsed_address = address.replace("   ", " ").replace("  ", " ")
                    for word in [
                        "Fee",
                        "FEE",
                        "LEASED",
                        "OWNED",
                        "100%",
                        "100%",
                        "Equitable",
                    ]:
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
                    extracted_rows[row_name] = {
                        "address": parsed_address.strip(),
                        "fee_type": fee_type.strip(),
                    }
        return extracted_rows

    def schedule_ab_parsing_text_type_2_mod(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (20, 40, int(w / 2) - 20, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image):
            all_text += page_text

        all_text = all_text.replace("\n", " ")
        if not all_text:
            raise RuntimeError(f"Failed to retrieve data from file: {filename}")

        main_pattern = re.compile(r"55\.(1.*)Total of Part 9")
        all_text = re.search(main_pattern, all_text).groups()[0]

        for part in all_text.split("55."):
            pattern = re.compile(r"(\d+)\.?(.*)")
            for match in re.finditer(pattern, part):
                if match:
                    groups = list(match.groups())
                    row_name = groups[0].strip()
                    address = groups[1] or ""

                    p = address.split("   ")
                    parsed_address = p[0]
                    fee_type = " ".join(p[1:])

                    if extracted_rows.get(row_name):
                        extracted_rows[row_name]["address"] += (
                            " " + parsed_address.strip()
                        )
                        extracted_rows[row_name]["fee_type"] += " " + fee_type.strip()
                    else:
                        extracted_rows[row_name] = {
                            "address": parsed_address.strip(),
                            "fee_type": fee_type.strip(),
                        }
        return extracted_rows

    def schedule_ab_parsing_text_type_2(self, filename: str):
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (65, 40, int(w / 2) - 20, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(
            filename, crop_image, search_text=["54.", "55.", "Part 9"]
        ):
            all_text += page_text

        all_text = all_text.replace("\n", " ")
        if not all_text:
            raise RuntimeError(f"Failed to retrieve data from file: {filename}")

        main_pattern = re.compile(r"55\.(1.*)Total of Part 9")
        all_text = re.search(main_pattern, all_text).groups()[0]
        for part in all_text.split("55."):
            pattern = re.compile(r"(\d+)\.?(.*)")
            for match in re.finditer(pattern, part):
                if match:
                    groups = list(match.groups())
                    print(f"groups {groups}")
                    row_name = groups[0].strip()
                    fee_type = ""  # TODO
                    address = groups[1] or ""
                    if not address and groups[-1]:
                        address = groups[-1]
                    parsed_address = address.replace("   ", " ")
                    for word in ["Fee", "FEE", "LEASED", "OWNED", "100%", "Equitable"]:
                        if word.upper() in parsed_address:
                            word = word.upper()

                        if word.lower() in parsed_address.lower():
                            fee_type = word + parsed_address.split(word)[-1]
                            parsed_address = parsed_address.split(word)[0]
                    if extracted_rows.get(row_name):
                        extracted_rows[row_name]["address"] += (
                            " " + parsed_address.strip()
                        )
                        extracted_rows[row_name]["fee_type"] += " " + fee_type.strip()
                    else:
                        extracted_rows[row_name] = {
                            "address": parsed_address.strip(),
                            "fee_type": fee_type.strip(),
                        }
        return extracted_rows

    def schedule_ab_parsing_text_type_1(self, filename: str):
        extracted_rows = {}
        boundaries = {"left": 10, "bottom": 40, "right": 300, "top": 792 - 40}
        all_text = "   ".join(
            [line for line in get_pdf_content_pdfium(filename, 0, boundaries)]
        )

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
                    if (
                        "Software Copyright (c)" in address
                        or "Software Copyright (c)" in address
                    ):
                        print(f"Skipping row {address}")
                        continue
                    parsed_address = address.replace("   ", " ").replace("  ", " ")
                    for word in [
                        "Fee",
                        "FEE",
                        "LEASED",
                        "OWNED",
                        "Owned",
                        "100%",
                        "100%",
                        "Equitable",
                    ]:
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

    # DEPRECATED

    def schedule_a_b_parsing_text_type1(self, filename: str):
        self.logger.info("schedule_a_b_parsing_text_type1")
        extracted_rows = {}

        def crop_image(image):
            w, h = image.size
            bounding_box = (50, 40, int(w / 2) + 40, h)  # left, top, right, bottom
            return image.crop(bounding_box)

        all_text = ""
        for page_text in get_pdf_content_from_text_ocr(filename, crop_image):
            if any(
                ("Part 8" in page_text, "Part 9" in page_text, "Part 10" in page_text)
            ):
                all_text += page_text
            if "Part 10" in page_text:
                break

        all_text = all_text.replace("\n", " ")

        part_nine = all_text
        # part_nine = all_text.split("55. Any building, other improved real estate")[-1]
        with open("sample_schedule_ab.txt", "w", encoding="utf-8") as outp:
            outp.write(part_nine)

        # TODO add 55.2 check
        pattern = re.compile(
            r"land which the debtor owns or i?\s*(.*?)  ([^\s].*?)55\.(\d)(.*)Total of Part 9"
        )
        for match in re.finditer(pattern, all_text):
            groups = list(match.groups())
            self.logger.info(f'"{groups}"')
            groups = [g.strip() for g in groups]
            # ['5755 Bayport Blvd.                          ', 'Fee Simple', '1', ' 5755 Bayport Blvd., Seabrook, TX 77586']
            group_name = groups[-2]
            extracted_rows[group_name] = {
                "address": groups[0] + " " + groups[-1],
                "fee_type": groups[1],
            }

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

            extracted_rows[row_name] = [
                address.strip().replace("\n", " "),
                fee_type.strip(),
            ]
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
        all_text = " ".join(
            [line for line in get_pdf_content_pdfium(filename, 0, boundaries)]
        ).replace("\n", " ")

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

                extracted_rows[row_name] = {
                    "address": address.strip().replace("\n", " "),
                    "fee_type": fee_type.strip(),
                }

        if not extracted_rows or not all([v for k, v in extracted_rows.items()]):
            self.logger.warning("Will try to parse pdf of another format")
            return self.schedule_a_b_parsing_text_type1(filename)

        return extracted_rows

    # END DEPRECATED

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
            all_text = " ".join(
                [l for l in get_pdf_content_pdfium(filename, 0, boundaries)]
            ).replace("\n", "")
            type_signature = all_text.split(":")[0][:48]
            self.logger.info(f"File type signarure '{type_signature}'")
            if type_signature == "":
                return "scan"

            if "" in type_signature:
                return "type2mod"

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
                "South Case number (If known)",
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
            "type2mod": self.schedule_ab_parsing_text_type_2_mod,  # OCR, utf-16 files
            "type3": self.schedule_ab_parsing_text_type_3,  # text incorrect order
            "type4": self.schedule_ab_parsing_text_type_4,  # text, mess in ordering
            "scan": self.schedule_a_b_parsing_scan,
            "scan2": self.schedule_a_b_parsing_scan_type_2,  # TODO check some time
        }

        try:
            extracted_rows = self.parse_pdf_file(
                filename, processing_funcs, discover_pdf_type_schedule_ab
            )

            # NOTE: Some type4 files have 'Is a depreciation schedule available for any of the property listed' text in output
            art_msg = (
                "Is a depreciation schedule available for any of the property listed"
            )
            type4_artifacts = [
                art_msg in v["address"] for k, v in extracted_rows.items()
            ]
            if any(type4_artifacts):
                extracted_rows = self.parse_pdf_file(
                    filename,
                    {"type4": self.schedule_ab_parsing_text_type_3},
                    discover_pdf_type_schedule_ab,
                )

            extracted_rows = self.fix_schedule_ab_data(extracted_rows)
            return extracted_rows
        except Exception as err:
            self.logger.error(
                f"schedule ab file {filename}: parsing failed due to {str(err)}"
            )
        return {}

    def fix_schedule_ab_data(self, extracted_rows: dict) -> dict:
        # TODO remove 'A/B Schedule A/B Assets Debtor Whetstone Partners, LLP Name;'
        # TODO remove '. ' from addresses
        new_dict = {}
        for key, value in extracted_rows.items():
            address = value.get("address", "")
            fee_type = value.get("fee_type", "")

            if not fee_type:
                fee_type = ""

            fee_type = self._convert_to_string(fee_type)

            _remove_list = ["56.", "56"]
            for r in _remove_list:
                if r in fee_type:
                    fee_type = fee_type.replace(r, "")

            if not address:
                address = ""

            address = self._convert_to_string(address)
            if address.startswith("."):
                address = address[1:].strip()
            while "_" in address:
                address = address.replace("_", " ")

            while "  " in address:
                address = address.replace("  ", " ")

            if "$e attached" in address:
                address = address.replace("$e attached", "see attached")

            value["address"] = address.strip()
            value["fee_type"] = fee_type.strip()
            new_dict[key] = value
        # TODO
        # "address": "727 W. Capitol Drive, San Pedro, CA 90731 Owner"
        # "fee_type": ""
        return new_dict
