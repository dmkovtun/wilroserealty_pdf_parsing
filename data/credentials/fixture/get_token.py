import logging
import os.path
from os import remove
from os.path import join
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


class GoogleSheetsClient:

    creds: Credentials
    token_path: str
    credentials_path: str
    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    _loaded_sheet = None
    logger: logging.Logger

    def __init__(self, token_path, credentials_path):
        self.token_path = token_path
        self.credentials_path = credentials_path
        #
        self.logger = logging.getLogger(__name__.split(".")[-1])
        self.authorize()
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
                    self.logger.warning("Google API OAuth2 token expired. Will create new one.")
                    remove(self.token_path)
                    self.authorize()
                    return
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, "w") as token:
                token.write(creds.to_json())

        self.creds = creds
        self.service = build("sheets", "v4", credentials=self.creds)



if __name__ == "__main__":

    TOKEN_PATH = join(os.getcwd(), '../', "token.json")
    CREDENTIALS_PATH = join(os.getcwd(), '../', "credentials.json")
    print(TOKEN_PATH)
    from os.path import exists
    import shutil
    import json

    path = CREDENTIALS_PATH
    print(exists(path))
    if not exists(path):
        with open(path, 'w') as outp:
            outp.write(json.dumps({}))
        # example_file = path.split('.json')[0] + '.example.json'
        # print(example_file)
        # shutil.copyfile(example_file, path)

    sheets_process = GoogleSheetsClient(
        TOKEN_PATH, CREDENTIALS_PATH
    )