#
# Stage 1: Building dependencies with Pipenv
#
FROM python:3.11-slim AS build

# Set environment variables to avoid unnecessary cache busting
ENV PYTHONUNBUFFERED=1
# Create the virtual environment in the project directory
ENV PIPENV_VENV_IN_PROJECT=1

# Install Pipenv and other build dependencies
#RUN apt-get update && \
#    apt-get install -y --no-install-recommends gcc && \
RUN pip install --no-cache-dir pipenv

# Set the working directory
WORKDIR /app

# Copy the Pipfile and Pipfile.lock to the working directory
COPY Pipfile Pipfile.lock ./

# Install the dependencies with Pipenv
RUN pipenv install --deploy --ignore-pipfile

#
# Stage 2: Building the final image
#
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the virtual environment from the build stage
COPY --from=build /app/.venv /app/.venv

# Set environment variable to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy code and launch app
COPY . .
CMD ["uvicorn", "app.main:app", "--port", "8000"]
