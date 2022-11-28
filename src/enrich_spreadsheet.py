import os

stream = os.popen("poetry run scrapy enrich_spreadsheet > latest_processing.log 2>&1")
output = stream.read()
