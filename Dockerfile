FROM python:3.11-buster

WORKDIR app/

# update pip and install poetry
RUN pip install -U pip && \
    pip install --no-cache-dir poetry

# install poetry packages
COPY ./poetry.lock .
COPY ./pyproject.toml .
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

# copy code
COPY ./src .

ENTRYPOINT []
CMD ["python", "."]
