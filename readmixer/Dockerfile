# Build from the repository root: docker build -f readmixer/Dockerfile .
# This development image is not release-qualified.
FROM python:3.11-slim
COPY . /app
WORKDIR /app
RUN python -m pip install --no-cache-dir .
ENTRYPOINT ["readmixer-py"]
