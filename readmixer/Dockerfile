FROM python:3.11-slim-buster
RUN pip install --upgrade pip

# Copy the ReadMixer application files to /app in the container
COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "readmixer.py"]