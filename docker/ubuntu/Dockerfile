FROM ubuntu:latest
SHELL ["/bin/bash", "-l", "-c"]

#####################################
# Set Timezone
#####################################

ARG TZ=UTC
ENV TZ ${TZ}

RUN apt-get update && apt-get install -y curl wget
RUN apt-get update && apt-get install -y python3.9 python3-pip python-is-python3
RUN python3 -m pip install --upgrade pip && python3 -m pip install poetry

RUN apt install -y tesseract-ocr imagemagick poppler-utils
# Playwright requires this
RUN apt install -y libatk1.0-0 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 libatspi2.0-0 libwayland-client0


COPY setup.sh /usr/local/bin/
RUN set -eu && chmod +x /usr/local/bin/setup.sh

ENTRYPOINT ["/bin/bash", "-l", "-c", "setup.sh"]
