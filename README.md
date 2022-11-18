# wilroserealty_pdf_parsing




# Installation process

> poetry install


> poetry run playwright install



# Google API enabling process

https://developers.google.com/sheets/api/quickstart/python#enable_the_api

https://developers.google.com/sheets/api/quickstart/python#authorize_credentials_for_a_desktop_application

TODO
Need to enable Google Sheets usage for an app.
https://console.cloud.google.com/apis/library/sheets.googleapis.com?project=wilroserealty-pdf-parsing

# How to use




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
~ 3.1 (optional) Try to get addresses from data collected in 'Step 3'
4. 'Schedule D' parsing (may be tricky)
In section 'List Creditors Who Have Secured Claims'
Get "Creditor's Name" from rows '2.{X}'
Also get "Creditor's mailing address"

(not decided yet): Save to 'Creditors Info' column. Maybe will need two columns



Required modules:
- Google Sheets usage (login, read, edit, save)
- Status webpage scraping
- (possibly) CSV parsing
- PDF file download
- PDF file content reading and parsing



Code requirements:
- Extensible
- (ideally) Processes & downloads data concurrently
- Runs as easy as possible
- Easy to setup required dependencies
- Easy readme file
- Runs locally on Mac for now



Future extensibility requirements:
- Code running on schedule in cloud
- CRM integration
- Third-party API integration with possibility to integrate several ones





