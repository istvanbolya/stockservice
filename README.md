# Stock Management Service
An application to import and manage stock.

* [Prerequisite](#Prerequisite)
* [Installation](#installation)
* [Usage](#usage)
* [Unittests](#unittests)

## Prerequisite
The application currently is prepared for local (dev) usage only. It is configured to default Kafka and PostgreSQL host/ports.

Kafka: 127.0.0.1:9092
PostGreSQL: localhost

## Installation
The application uses base Python3.x libraries only.

I suggest to use [virtualenv](https://www.pythonforbeginners.com/basics/how-to-use-python-virtualenv) with Python 3.x.
After installing virtualenv, create a new for the application:

`mkdir ~/stockservice`

`virtualenv ~/stockservice/venv`

Activate the env.:

`cd ~/stockservice`

`source venv/bin/activate`

Checkout the source:

`git clone https://github.com/istvanbolya/stockservice.git`

Install packages:

- Development:
`pip install -r stockservice/requirements/dev.txt`

- Production:
`pip install -r stockservice/requirements/prod.txt`

Initialize database

`python ./stockservice/scripts/init_db.py`

## Usage
The application contains two parts:
- Importer: It grabs all of the CSV files from a specified directory, parses them, validates the records, and sends into a Kafka topic as an event
- Service: It polls the Kafka topic for new events, and process them.
 
Importer script: `python stockservice/run_importer.py`
Service script: `python stockservice/run_service.py`
 
## Unittests
You can run the unittests by executing the unittest module under stockservice directory in virtualenv:
 
 `python -m unittest`
