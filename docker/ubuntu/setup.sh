#!/bin/bash
set -eu
chmod +x /var/app/src
cd /var/app/src

poetry install --no-interaction --no-ansi --only main
poetry run playwright install

python run_processing.py
bash
