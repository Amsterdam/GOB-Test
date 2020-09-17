# GOB-Test

Contains the E2E-style tests for GOB.

Two types of end-2-end tests can be distinguished:

- e2e_test  
End-2-End tests for test cases based in test collections in the GOB test catalog

- data_e2e_test  
End-2-End data tests to compare data in source systems with data in the analysis database

Both tests may require configuration:

- The API_HOST to get data from GOB for the end-2-end tests in the GOB test catalog

- The access details for the source systems and the analysis database for the end-2-end data tests.
These details are equal to the configuration found in GOB-Import (source systems) and GOB-Export (analysis database) 

The configuration is kept in a .env file.
An example file is included with this project.

To initialise the configuration:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
```

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03

## Run

```bash
docker-compose build
docker-compose up &
```

## Tests

```bash
docker-compose -f src/.jenkins/test/docker-compose.yml build
docker-compose -f src/.jenkins/test/docker-compose.yml run test
```

# Local

## Requirements

* python >= 3.6

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment

```bash
source venv/bin/activate
```

# Run

Set environment:

```bash
export $(cat .env | xargs)  # Copy from .env.example if missing
```


Start the service:

```bash
cd src
python -m gobtest 
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```

# Remarks

## Trigger tests

Tests are triggered by the GOB-Workflow module.

See the [GOB-Workflow README](https://github.com/Amsterdam/GOB-Workflow/blob/develop/README.md)  for more details how to start workflows.

See [GOB-Core start_commands.json](https://github.com/Amsterdam/GOB-Core/blob/master/gobcore/workflow/start_commands.json)
for instructions how to start e2e_test and data_e2e_test.

## Data end-2-end tests

The warnings and errors that are reported are:

- Warning: Skip <<attribuut>> because no mapping is found.
The attribute is present in GOB but not mapped from the input.
No mapping exists for the given attribute in the import definition.  
Examples are: belast_kadastrale_objecten, feitelijk_gebruik

- Warning: Skip <<attribuut>> that is imported as non- or empty-JSON.
The JSON attribute cannot be compared because its value is not in the source.
The JSON attribute is calculated or derived during the import or enrichment.
The mapping however exists which is surprising.
Example: soort_cultuur_bebouwd.

- Warning: Skip <<attribuut>> because it is missing in the input.
The attribute cannot be compared because its value is not in the source.
The mapping however exists which is surprising.
The attribute is calculated or derived during the import or enrichment.  
Examples are: cbs_code or zakkingen van meetbouten

- Error: Have mismatching values.
The source value does not match the GOB value.
The comparison is case-insensitive and whitespace differences are ignored  

- Error: Counts don't match.  
The number of entities in the source does not match the number of entities in GOB.
