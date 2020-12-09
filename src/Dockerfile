FROM amsterdam/python:3.7-buster
MAINTAINER datapunt@amsterdam.nl

RUN apt-get update && apt-get install -y git libgdal-dev

# Install gobtest in /app folder
WORKDIR /app

# Copy the required db drivers
COPY drivers drivers

# Install Oracle driver
# https://oracle.github.io/odpi/doc/installation.html#linux
ENV ORACLE_DIR=/app/opt/oracle
RUN mkdir -p ${ORACLE_DIR}
RUN unzip drivers/instantclient-basic-linux.x64-18.3.0.0.0dbru.zip -d ${ORACLE_DIR}
ENV LD_LIBRARY_PATH=${ORACLE_DIR}/instantclient_18_3:$LD_LIBRARY_PATH
RUN apt-get -y install libaio1 unixodbc-dev apt-transport-https

# Install SQL Server driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

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
