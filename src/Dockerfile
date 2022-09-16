# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-buster as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-buster as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver, Oracle driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobtest in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Copy gobtest module
COPY gobtest gobtest

USER datapunt
CMD ["python", "-m", "gobtest"]

# Development.
FROM application as development
USER root
# Install GOB-Core and GOB-Config in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core and GOB-Config
# will be available immediately.
RUN rm -rf GOB-Core && git clone --depth=1 --branch master https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Config.git
RUN pip install -e /app/GOB-Config
USER datapunt

# Test.
FROM application as test
USER root

# Remove gobcore tests.
RUN rm -rf /app/src/gobcore/tests

# Copy test module and tests.
COPY test.sh .flake8 pytest.ini ./
COPY tests tests

# Copy Jenkins files.
COPY .jenkins /.jenkins

USER datapunt
