# wilroserealty_pdf_parsing

# Overview

This app is for enrichment of Google Sheets document with data from https://www.inforuptcy.com/ portal.

Functionality:
- Google Sheets usage (login, read, edit)
- Status webpage scraping
- CSV parsing
- PDF file download
- PDF file content reading and parsing

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

# Environment setup

Copy `.env.example` file and rename it to `.env`.

Then specify following settings:
- `SPREADSHEET_ID` - value in spreadsheet url after 'https://docs.google.com/spreadsheets/d/`{THIS_ID}`/edit'
- `SHEET_NAME` - name of sheet to be read and edited
- `HEADER_RANGE_NAME` - range of sheet header row, like `A1:AW1`

# Google Sheets credentials and app creation

1. Perform all steps from the [official Google instruction](https://developers.google.com/sheets/api/quickstart/python#enable_the_api)

2. Save received json file as `credentials.json` to `./data/credentials/` directory.

3. Enable Google Sheets API usage for an app [on this page](https://console.cloud.google.com/apis/library/sheets.googleapis.com).

# Google Sheet requirements

Kindly check [example spreadsheet with supported columns](https://docs.google.com/spreadsheets/d/1SgRASrGHVmsoAryeCOVn88wO54K7roJYPCjJb2rQkpc/edit#gid=2088411805).

Note, that column order does not affect script running. Column names affect it though.

If you need to change any column name, stop docker container and then modify spreadsheet.

You'll need to add a Script function and use it for getting hyperlink cells links:

>function getHyperlink(cellReference) {
>  let file = SpreadsheetApp.getActive();
>  let sheet = file.getSheetByName("`NAME OF REQUIRED SHEET`");
>  let range = sheet.getRange(cellReference);
>  let richText = range.getRichTextValue();
>  let link = richText.getLinkUrl();
>  return link;
>}

# Installation process

1. [Install Python 3.8.6 or upper](https://www.python.org/downloads/).

2. Install [Docker application](https://docs.docker.com/desktop/install/mac-install/)

3. From project directory run in terminal:

> python setup.py

# Usage

After previous steps, you can use Docker UI for starting/stopping the app.

# Runtime error solving

Sometimes Google authentication token expires.
If app is not running as expected, try resetting everything by running:

> python setup.py

If this doesn`t help, feel free to contact.
