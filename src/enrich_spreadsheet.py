import os

print("Started processing. Do not close this window.")
stream = os.popen("poetry run scrapy enrich_spreadsheet > latest_processing.log 2>&1")
output = stream.read()
print("Processing completed successfully.")
