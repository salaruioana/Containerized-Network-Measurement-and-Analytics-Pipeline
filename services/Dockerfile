# One base stage installs the dependencies and the shared package; each service
# is a thin target on top of it that adds its own code and its own entrypoint.
# Four targets, four images, one process each. §1.5.1 asks for exactly that, and a
# single image with four roles does not satisfy it.
#
#   docker build --target <api-gateway|storage-service|analytics-service|report-service> .
#
# The build context is this directory.
#
# The api-gateway target is filled in as a worked example. The other three are
# yours; they are consumers, so they run a module rather than a web server.

FROM python:3.12-slim AS base
WORKDIR /app
# Unbuffered, or the logs sit in a pipe buffer and `docker compose logs` shows
# nothing until the process exits. You will want this the first time something
# hangs.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
# Requirements first, so a source change does not reinstall the dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY aic_common aic_common
RUN useradd --create-home --uid 10001 aic
USER aic

FROM base AS api-gateway
COPY api_gateway api_gateway
EXPOSE 8080
CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8080"]

# TODO: storage-service. Copy `storage_service` and run it with
# `python -m storage_service.main`. No EXPOSE: nothing connects to a consumer.

# TODO: analytics-service.

# TODO: report-service.
