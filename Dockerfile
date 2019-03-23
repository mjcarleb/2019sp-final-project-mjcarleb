FROM python:3.5 AS base

ARG CI_USER_TOKEN
RUN echo "machine github.com\n  login $CI_USER_TOKEN\n" >~/.netrc

ENV \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PIPENV_HIDE_EMOJIS=true \
    PIPENV_COLORBLIND=true \
    PIPENV_NOSPIN=true \
    PYTHONPATH="/app/src:${PYTHONPATH}"

WORKDIR /root

RUN pip install pipenv

WORKDIR /app
COPY Pipfile .

ARG SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN pipenv install --dev

