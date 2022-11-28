import os

stream = os.popen("poetry run scrapy enrich_spreadsheet")
output = stream.read()
