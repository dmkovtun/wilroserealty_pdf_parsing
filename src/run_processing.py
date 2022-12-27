import os
from time import sleep

TIME_TO_SLEEP = 30

print("Started processing. Do not close this window.")
while True:
    stream = os.popen("poetry run scrapy enrich_spreadsheet > latest_processing.log 2>&1")
    output = stream.read()
    sleep(TIME_TO_SLEEP)
