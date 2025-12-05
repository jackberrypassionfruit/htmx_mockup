# Use the official Python image
# FROM python:3.13-slim
FROM ghcr.io/astral-sh/uv:bookworm-slim

# # The installer requires curl (and certificates) to download the release archive
# RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# # Download the latest installer
# ADD https://astral.sh/uv/install.sh /uv-installer.sh

# # Run the installer then remove it
# RUN sh /uv-installer.sh && rm /uv-installer.sh


# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy the project into the image
ADD . /app

# Sync the project into a new environment, asserting the lockfile is up to date
WORKDIR /app

RUN uv sync --locked
# RUN uv init && uv add django django-htmx django-inline-svg gunicorn django-extensions django-livereload-server polars-lts-cpu werkzeug python-dateutil

# CMD ["uv", "run", "manage.py", "runserver", "0.0.0.0:8000"]

