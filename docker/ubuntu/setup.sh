#!/bin/bash
set -eu
# bash
chmod +x /var/app/src

apt install tesseract-ocr -y
apt install imagemagick -y
apt install poppler-utils -y


cd /var/app/src
poetry install --no-dev --no-interaction --no-ansi
poetry run playwright install

echo 'Everything was successfully installed'

cd /var/app/pm2
pm2 status
# uncomment following line if pm2.config.js file existing in python/src dir and contain instructions
# to run worker processes. Or replace with required bash terminal instructions
pm2 start pm2.config.js

bash
