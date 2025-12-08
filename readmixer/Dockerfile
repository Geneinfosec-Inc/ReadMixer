FROM python:3.11-slim-buster

# Note: This Dockerfile uses pip installation which requires GCC >= 9.3
# The python:3.11-slim-buster image includes a compatible GCC version
# For systems with older GCC, see Dockerfile.conda

RUN pip install --upgrade pip

# Copy the ReadMixer application files to /app in the container
COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "readmixer.py"]