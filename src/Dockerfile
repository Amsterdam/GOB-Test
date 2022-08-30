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
