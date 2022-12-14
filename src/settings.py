# -*- coding: utf-8 -*-
import json
import logging
import os
from datetime import datetime, timedelta
from distutils.util import strtobool
from os import getcwd
from os.path import join
from typing import Dict

from dotenv import load_dotenv
from scrapy.utils.log import configure_logging

load_dotenv()

BOT_NAME = "pdf_excel_parser"

SPIDER_MODULES = ["spiders"]
NEWSPIDER_MODULE = "spiders"
COMMANDS_MODULE = "commands"

PROXY = os.getenv("PROXY", "")
PROXY_AUTH = os.getenv("PROXY_AUTH", "")
PROXY_ENABLED = strtobool(os.getenv("PROXY_ENABLED", "False"))

USER_AGENT_RELEASE_DATE = "2022-04-01"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "16"))
CONCURRENT_REQUESTS_PER_DOMAIN = int(os.getenv("CONCURRENT_REQUESTS_PER_DOMAIN", "8"))
DOWNLOAD_DELAY = int(os.getenv("DOWNLOAD_DELAY", "0"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "180"))

ROBOTSTXT_OBEY = False
COOKIES_ENABLED = True

TELNETCONSOLE_ENABLED = False
TELNETCONSOLE_PASSWORD = "password"

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "*/*",
}

ROTATING_PROXIES_DOWNLOADER_HANDLER_AUTO_CLOSE_CACHED_CONNECTIONS_ENABLED: bool = True

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": None,
    "middlewares.HttpProxyMiddleware": 543,
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE") if os.getenv("LOG_FILE", "") else None
logging.getLogger("rmq.utils.decorators.log_current_thread").setLevel(LOG_LEVEL)

ITEM_PIPELINES: Dict[str, int] = {}

# DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
# DB_PORT = int(os.getenv("DB_PORT", "3306"))
# DB_USERNAME = os.getenv("DB_USERNAME", "root")
# DB_PASSWORD = os.getenv("DB_PASSWORD", "")
# DB_DATABASE = os.getenv("DB_DATABASE", "db_name")

# PIKA_LOG_LEVEL = os.getenv("PIKA_LOG_LEVEL", "WARN")
# logging.getLogger("pika").setLevel(PIKA_LOG_LEVEL)
# logging.getLogger("rmq.connections.pika_select_connection").setLevel(PIKA_LOG_LEVEL)
# logging.getLogger("PIL.PngImagePlugin").setLevel(PIKA_LOG_LEVEL)


# COOKIES_ENABLED = True
# COOKIES_DEBUG = True

PLAYWRIGHT_HEADLESS = bool(strtobool(os.getenv("PLAYWRIGHT_HEADLESS", "True")))


try:
    HTTPCACHE_ENABLED = strtobool(os.getenv("HTTPCACHE_ENABLED", "False"))
except ValueError:
    HTTPCACHE_ENABLED = False

HTTPCACHE_IGNORE_HTTP_CODES = list(
    map(int, (s for s in os.getenv("HTTPCACHE_IGNORE_HTTP_CODES", "").split(",") if s))
)

EXTENSIONS = {}

# Send exceptions to Sentry
IS_SENTRY_ENABLED = os.getenv("IS_SENTRY_ENABLED", "false").lower() == "true"
if IS_SENTRY_ENABLED:
    SENTRY_DSN = os.getenv("SENTRY_DSN", None)
    # Optionally, additional configuration options can be provided
    SENTRY_CLIENT_OPTIONS = {
        # these correspond to the sentry_sdk.init kwargs
        "release": os.getenv("RELEASE", "0.0.0")
    }
    # Load SentryLogging extension before others
    EXTENSIONS["scrapy_sentry_sdk.extensions.SentryLogging"] = 1

configure_logging()
if (
    datetime(*[int(number) for number in USER_AGENT_RELEASE_DATE.split("-")]) + timedelta(days=180)
    < datetime.now()
):
    logging.warning("USER_AGENT is outdated")


def _process_relative_path(path: str):
    if "../" in path:
        return join(getcwd(), path)
    return path


# Google Sheets API
CREDENTIALS_PATH = _process_relative_path(os.getenv("CREDENTIALS_PATH", "../credentials"))
TOKEN_PATH = join(CREDENTIALS_PATH, "token.json")
CREDENTIALS_PATH = join(CREDENTIALS_PATH, "credentials.json")


logging.getLogger("googleapiclient.discovery_cache").setLevel("WARN")
logging.getLogger("googleapiclient.discovery").setLevel("INFO")
logging.getLogger("asyncio").setLevel("INFO")
logging.getLogger("scrapy.core.engine").setLevel("INFO")
logging.getLogger("pdfminer.psparser").setLevel("INFO")
logging.getLogger("pdfminer.pdfdocument").setLevel("INFO")
logging.getLogger("pdfminer.pdfparser").setLevel("INFO")
logging.getLogger("pdfminer.pdfinterp").setLevel("INFO")
logging.getLogger("pdfminer.cmapdb").setLevel("INFO")
logging.getLogger("pdfminer.pdfpage").setLevel("INFO")
logging.getLogger("pdfminer.encodingdb").setLevel("INFO")
logging.getLogger("pdfminer.converter").setLevel("INFO")
logging.getLogger("case_status_spider").setLevel("INFO")
logging.getLogger("scrapy.core.engine").setLevel("INFO")


# TODO README
# pdf2image.exceptions.PDFInfoNotInstalledError: Unable to get page count. Is poppler installed and in PATH?


POPPLER_PATH = _process_relative_path(os.getenv("POPPLER_PATH", ""))
TESSERACT_PATH = _process_relative_path(
    os.getenv("TESSERACT_PATH", "../packages/Tesseract-OCR/tesseract.exe")
)
TEMP_DIR_PATH = _process_relative_path(os.getenv("TEMP_DIR_PATH", "../temp"))
PDF_TEMP_DIR_PATH = join(TEMP_DIR_PATH, "pdf_parts")


SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "REQUIRED")
SHEET_NAME = os.getenv("SHEET_NAME", "REQUIRED")
HEADER_RANGE_NAME = os.getenv("HEADER_RANGE_NAME", "REQUIRED")
