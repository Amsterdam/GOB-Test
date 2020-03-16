FROM amsterdam/python:3.7-buster
MAINTAINER datapunt@amsterdam.nl

RUN apt-get update
RUN apt-get install -y git

# Install api service in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

COPY gobtest gobtest

# Copy tests
COPY tests tests
COPY .flake8 .flake8
COPY test.sh test.sh

COPY .jenkins /.jenkins

CMD ["python", "-m", "gobtest"]
