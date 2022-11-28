from __future__ import print_function

import logging
import os.path
from os import remove
from time import sleep
from typing import Any, Dict, List

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# TODO

# The ID and range of a sample spreadsheet.
# https://docs.google.com/spreadsheets/d/1Or-w7VFKGRI-eZ9w26JgLeG6z1rJxhJ783nJ0TOEisk/edit#gid=2088411805


class GoogleSheetsClient:

    creds: Credentials
    token_path: str
    credentials_path: str
    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    spreadsheet_id = "1Or-w7VFKGRI-eZ9w26JgLeG6z1rJxhJ783nJ0TOEisk"
    sheet_name = " A/B $5MM+ through 11/3"
    range_name = f"{sheet_name}!A1:AM1"
    _loaded_sheet = None
    logger: logging.Logger

    def __init__(self, token_path, credentials_path):
        self.token_path = token_path
        self.credentials_path = credentials_path
        self.logger = logging.getLogger(__name__.split(".")[-1])
        self.authorize()
        self.load_header()
        self.service

    def authorize(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except RefreshError:
                    self.logger.warning(
                        "Google API OAuth2 token expired. Will create new one."
                    )
                    remove(self.token_path)
                    self.authorize()
                    return
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, "w") as token:
                token.write(creds.to_json())

        self.creds = creds
        self.service = build("sheets", "v4", credentials=self.creds)

    def load_header(self):
        self.sheet_header = self.read_rows(f"{self.sheet_name}!A1:AW1")[0]

    def read_rows(self, range_name, is_load_formulas=False) -> List[list]:
        try:

            if not self._loaded_sheet:
                self._loaded_sheet = self.service.spreadsheets()

            value_render_option = "FORMATTED_VALUE"
            if is_load_formulas:
                value_render_option = "FORMULA"

            # majorDimension='ROWS',
            result = self._loaded_sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueRenderOption=value_render_option,
            )
            # result.postproc = lambda x, y: y
            result = result.execute()
            rows_list = result.get("values", [])

            if not rows_list:
                print("No data found.")
                return []

            # TODO remove this
            # fixable_hyperlinks = ['View online', 'Download PDF', 'Download CSV']
            # header_row = 1
            # for column in rows_list:
            #     for index, row in enumerate(column):
            #         if row in fixable_hyperlinks:
            #             curr_cell_row = header_row + index
            #             # range_name
            #             range_regex = r'.*\!(\w+):(\w+)'
            #             import re
            #             re_found = re.search(range_regex, range_name)
            #             if re_found:
            #                 start_range = re_found.group(1)
            #                 end_range = re_found.group(2)
            #                 print(f'start_range {start_range}')
            #                 print(f'end_range {end_range}')
            #             # request = service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id, range='range_', valueInputOption=value_input_option, body=value_range_body)
            #             # response = request.execute()
            #             # print(response)

            return rows_list
        except HttpError as err:
            print(err)
        return []

    def load_all_rows(
        self, column_name_exc: str, is_load_formulas: bool = False
    ) -> List[Any]:
        found_rows = self.read_rows(
            f"{self.sheet_name}!{column_name_exc}2:{column_name_exc}", is_load_formulas
        )
        return found_rows

    __colnames_cache: Dict[str, str] = {}

    @staticmethod
    def get_colnum_string(n):
        string = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string

    def discover_column_from_name(self, column_name: str) -> str:
        try:
            return self.__colnames_cache[column_name]
        except KeyError:

            cached_value = self.get_colnum_string(
                int(self.sheet_header.index(column_name) + 1)
            )
            self.__colnames_cache[column_name] = cached_value
            return self.__colnames_cache[column_name]

    def load_all_rows_from_name(self, column_name):

        while retries_count := 0 < 100:
            data = self.load_all_rows(self.discover_column_from_name(column_name))
            # Check if all functions have resulted
            if not any(["Loading..." in p for p in data]):
                return [p[0] if p else "" for p in data]
            retries_count += 1

            seconds_to_sleep = 2
            self.logger.debug(
                f"Waiting for {seconds_to_sleep} seconds to get formulas loaded."
            )
            sleep(seconds_to_sleep)
        raise RuntimeError(f"Failed to fetch all links for column '{column_name}'")

    def _is_valid_range(self, start_col, end_col, values: List[list]):
        """Checks range length and values length.

        Args:
            start_col (_type_): _description_
            end_col (_type_): _description_
            values (List[list]): _description_

        Returns:
            _type_: _description_
        """
        _start_col_idx = self.sheet_header.index(start_col)
        _end_col_idx = self.sheet_header.index(end_col)
        range_len = _end_col_idx - _start_col_idx + 1
        if any([len(r) != range_len for r in values]):
            self.logger.critical(
                f"Cannot update values with provided range: ['{start_col}':'{end_col}'], values: {values}"
            )
            return False
        return True

    def _get_range_cell(self, row_idx, start_col, end_col):
        _range_start = f"{self.discover_column_from_name(start_col)}{row_idx}"
        _range_end = f"{self.discover_column_from_name(end_col)}{row_idx}"
        return f"{_range_start}:{_range_end}"

    def update_values(self, row_idx, start_col, end_col, values: List[list]):

        if self._is_valid_range(start_col, end_col, values):
            updatable_range = self._get_range_cell(row_idx, start_col, end_col)
            self.logger.debug(
                f"Updating range [{updatable_range}] with values {values}"
            )
            return self._update_values(updatable_range, values)
        return None

    def _update_values(self, range_name, values: List[list]):
        """_summary_

        Args:
            range_name (_type_): _description_
            values (_type_):
                    values = [
                        [
                            # Cell values ...
                        ],
                        # Additional rows ...
                    ]

        Returns:
            _type_: _description_
        """
        while True:
            try:
                body = {"values": values}
                value_input_option = "USER_ENTERED"
                result = (
                    self.service.spreadsheets()
                    .values()
                    .update(
                        spreadsheetId=self.spreadsheet_id,
                        range=range_name,
                        valueInputOption=value_input_option,
                        body=body,
                    )
                    .execute()
                )
                self.logger.info(f"{result.get('updatedCells')} cells updated.")
                return result

            except HttpError as error:
                self.logger.info(f"An error occurred during row update: {error}")

                if "Quota exceeded for quota metric" in str(error):
                    self.logger.warning("Write requests quota exceeded. Will wait")
                    from time import sleep

                    sleep(20)
                    continue
                else:
                    raise RuntimeError("Failed to update row values") from error


if __name__ == "__main__":
    from scrapy.utils.project import get_project_settings

    settings = get_project_settings()
    sheets_process = GoogleSheetsClient(
        settings.get("TOKEN_PATH"), settings.get("CREDENTIALS_PATH")
    )
    #
    cols = [
        "Status",
        "Creditor Notes",
        "Borrower Notes",
        "Property Notes",
        "ADDRESS",
        "Attorney Email",
        "Other Attorney Emails",
    ]
    print(len(cols))
    start_col = "Status"
    end_col = "Other Attorney Emails"
    values = ["TEMP", "TEMP1", "a", "b", "c", "temp2", "3"]

    sheets_process.update_values(2, start_col, end_col, [values])
    # # Pass: spreadsheet_id,  range_name, value_input_option and  _values
    # sheets_process.update_values( "A1:C2", [["A", "B"], ["C", "D"]])
    # TODO google docs
    # https://developers.google.com/sheets/api/quickstart/python
