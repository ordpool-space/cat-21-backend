#
# Stage 1: Building dependencies with Pipenv
#
FROM python:3.11-slim-bullseye AS build

# Set environment variables to avoid unnecessary cache busting
ENV PYTHONUNBUFFERED=1
# Create the virtual environment in the project directory
ENV PIPENV_VENV_IN_PROJECT=1
# Locale
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
# No to pyc files, yes to tracebacks on segfault
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

# Install Pipenv
RUN pip install --progress-bar off --no-cache-dir pipenv

# Set the working directory
WORKDIR /app

# Copy the Pipfile and Pipfile.lock to the working directory
COPY Pipfile Pipfile.lock ./

# Install the dependencies with Pipenv
RUN pipenv sync --bare

#
# Stage 2: Building the final image
#
FROM python:3.11-slim-bullseye as runtime

# Create and switch to a new user
RUN useradd --home-dir /app nonroot
USER nonroot

# Set the working directory
WORKDIR /app

# Copy the virtual environment from the build stage
COPY --from=build /app/.venv /app/.venv

# Set environment variable to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy code and launch app
COPY app/main.py .
CMD ["uvicorn", "main:app", "--port", "8000"]
