FROM python:3.12-slim

ARG TESTING=0

# make sure it doesnt fail if the docker file doesnt know the git commit
ARG GIT_PYTHON_REFRESH=quiet

RUN apt-get update && apt-get install -y git curl

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# copy files
COPY pyproject.toml app/pyproject.toml
COPY README.md app/README.md

# create virtual environment and install dependencies
RUN cd app && uv venv && uv sync

# copy library files
COPY pvliveconsumer/ app/pvliveconsumer/
COPY tests/ app/tests/

# change to app folder
WORKDIR /app

# install library in development mode
RUN uv sync --dev

# activate the virtual environment in the shell
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN if [ "$TESTING" = 1 ]; then uv add --dev pytest pytest-cov coverage; fi

CMD ["python", "-u","pvliveconsumer/app.py"]
