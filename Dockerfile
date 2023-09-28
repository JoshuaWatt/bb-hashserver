# Copyright (C) 2023 Garmin Ltd.
#
# SPDX-License-Identifier: GPL-2.0-only

FROM debian:bookworm

RUN apt update -y && \
    apt install -y \
        python3 \
        python3-pip \
        python3-venv \
    && apt clean -y

RUN mkdir /opt/hashserver
WORKDIR /opt/hashserver

ENV VIRTUAL_ENV=/opt/hashserver/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip install -U pip

COPY requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt

COPY hashserver.py .
COPY run .
COPY bitbake ./bitbake/

ENTRYPOINT ["./run"]
