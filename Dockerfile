FROM python:3.10-slim

RUN groupadd --gid 5000 user \
    && useradd --home-dir /home/user --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null user \
    && chmod 755 /home/user
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://raw.githubusercontent.com/lacework/go-sdk/main/cli/install.sh | bash

RUN python -m pip install --upgrade pip 


ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PATH "$PATH:/home/user/.local/bin"


WORKDIR /app

COPY pyproject.toml ./
COPY poetry.lock ./

COPY laceworkreports ./laceworkreports
COPY README.md ./
RUN chown -R user:user .


USER user
RUN python -m pip install 'poetry==1.1.12'

# Install Poetry
RUN poetry config virtualenvs.create false \
    && poetry install

# Allow installing dev dependencies to run tests
ARG INSTALL_DEV=false
RUN bash -c "if [ $INSTALL_DEV == 'true' ] ; then poetry install --no-root ; else poetry install --no-root --no-dev ; fi"

ENTRYPOINT [ "python3", "-m", "laceworkreports" ]
