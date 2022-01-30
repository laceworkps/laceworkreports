FROM python:3.10-slim

RUN groupadd --gid 5000 user && useradd --home-dir /home/user --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null user && chmod 755 /home/user
RUN apt-get update && apt-get install -y curl git
RUN curl https://raw.githubusercontent.com/lacework/go-sdk/main/cli/install.sh | bash

RUN python -m pip install --upgrade pip
RUN python -m pip install laceworkreports
# RUN python -m pip install laceworksdk

# Temp workaround for beta v2 API support (beta support)
WORKDIR /
RUN git clone https://github.com/lacework/python-sdk -b alannix-lw/pagination
RUN cd python-sdk \
    && pip install -r requirements.txt \
    && python setup.py install \
    && mkdir -p /app \
    && chown user:user /app \
    && mv laceworksdk /app

USER user

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PATH "$PATH:/home/user/.local/bin"

WORKDIR /app

ENTRYPOINT [ "python3", "-m", "laceworkreports" ]
