# --- Build dependencies --- #
FROM python:3.12 AS build-deps

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml

# Install only requirements
RUN mkdir pvliveconsumer && uv sync --no-dev --no-install-project --compile-bytecode --inexact

# --- Build the package --- #
FROM build-deps AS build-app

# Install the app
# * The .git folder is needed here for setuptools-git-versioning
COPY pvliveconsumer /app/pvliveconsumer
COPY README.md /app/README.md
COPY .git /app/.git
RUN uv sync --no-editable --no-dev --compile-bytecode --inexact

# --- Runtime image --- #
FROM python:3.12-slim

ARG TESTING=0

# make sure it doesnt fail if the docker file doesnt know the git commit
ARG GIT_PYTHON_REFRESH=quiet

RUN apt-get update && apt-get install -y git

# Copy required elements of the builder image
COPY --from=build-app /app/.venv /app/.venv
COPY --from=build-app /usr/bin/git /usr/bin/git
COPY --from=build-deps /usr/local/bin/uv /app/.venv/bin/uv

# Copy pyproject.toml for uv commands
COPY --from=build-app /app/pyproject.toml /app/pyproject.toml

# This is just a check to make sure it works
ENV PATH="/app/.venv/bin:${PATH}"

# copy source code and test files if needed
COPY --from=build-app /app/pvliveconsumer /app/pvliveconsumer
COPY tests/ /app/tests/

# change to app folder
WORKDIR /app

# Install the package in editable mode using uv sync (this will install the package based on pyproject.toml)
RUN /app/.venv/bin/uv sync --no-dev

# For testing, install additional packages using the virtual environment
RUN if [ "$TESTING" = 1 ]; then /app/.venv/bin/uv add --dev pytest pytest-cov coverage; fi

CMD ["/app/.venv/bin/python", "-u", "-m", "pvliveconsumer.app"]
