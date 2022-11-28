# wilroserealty_pdf_parsing

This app is for enrichment of Google Sheets document with data from https://www.inforuptcy.com/ portal.

Functionality:
- Google Sheets usage (login, read, edit)
- Status webpage scraping
- CSV parsing
- PDF file download
- PDF file content reading and parsing

# Installation process (Mac-specific)
- Install homebrew:
> /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

- Install tesseract OCR
For Mac:
> brew install tesseract

- Install poppler
For Mac:
> brew install poppler

Moving apps to `./packages` directory is recommended.
Then you'll need to set .env variables for installed apps.

# Environment

Copy `.env.example` file and rename it to `.env`.

Then specify following settings:
- `POPPLER_PATH` - provide a path to installed poppler
- `TESSERACT_PATH` - provide a path to installed tesseract
- `SPREADSHEET_ID` - value in spreadsheet url after 'https://docs.google.com/spreadsheets/d/`{THIS ID}`/edit#gid='
- `SHEET_NAME` - name of sheet to be read and edited
- `HEADER_RANGE_NAME` - range of sheet header row, like `A1:AW1`

# Google Sheets credentials and app creation

Perform all steps from the [official instruction](https://developers.google.com/sheets/api/quickstart/python#enable_the_api)

Save received json file as `credentials.json` to './credentials/' directory.

Enable Google Sheets API usage for an app [on this page](https://console.cloud.google.com/apis/library/sheets.googleapis.com).

# Python installations

Install Python 3.8.1 or upper from [official portal](https://python.org)

Install poetry:
> pip install poetry

Create virtual environment (run following commands from inside `src` directory)
> poetry install

Install playwright browsers (required):
> poetry run playwright install


# Usage
From inside `src` directory, run script `enrich_spreadsheet.py` by double clicking it or running from terminal:
> python enrich_spreadsheet.py

Note, that first run will require google account authorization with your default browser.


# Business requirements

Description is below:

Google Sheet enrichment process in steps:
1. Status value checking:
~ 1.1 Check file from 'Case Link' (column A):
- If pdf contains 'dismissed' in top right corner - status should be set as 'Dismissed'. CONTINUE processing this row.
- Else: continue processing, status 'Active'
2. Attorney emails fillup (columns AJ, AK)
Save only emails. First person - column AJ, others go to AK
Get data from column K (Attorneys): it has a csv file inside and contains same emails
3. 'Schedule A/B' parsing
Fill 'Notes' column with data from section 9, rows '55.{X}' (where X will change)
Take only first two columns.
~ 3.1 Try to get addresses from data collected in 'Step 3'
4. 'Schedule D' parsing (may be tricky)
In section 'List Creditors Who Have Secured Claims'
Get "Creditor's Name" from rows '2.{X}'
Also get "Creditor's mailing address"
Save to 'Creditors Info' column.
