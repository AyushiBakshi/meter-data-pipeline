# Meter Data Pipeline

## Overview
This pipeline ingests and stores **NEM12-formatted meter data** efficiently into a PostgreSQL database.


## Features
- Parse **NEM12** energy data files (CSV format)
- Insert meter readings to batch insert statements in a SQL file.
- Designed for high-throughput ingestion, batching, and resilience using asynchronous task queues.


## System Design

### Architecture
The system follows a **Layered Architecture** with the following components:

1. **Presentation Layer:**
   - User can run the program with the command
     ```bash
     python manage.py parse_data --file <absolute path to nem12 file.csv>

2. **Service Layer:**
   - Contains business logic for data parsing and generation Insert statements in SQL file.
   - `DataParser` manages data parsing operations.
   - `DDLScriptCreatorForMeterReadings` handles creating the SQL script for data insertion into database.
   - `process_data`, `read_file_in_chunks`, and `run`: Handle multiprocessing, streaming, and orchestration.

3. **Data Layer:**
   - Uses Django ORM for data persistence.
   - Models include `meter_readings`.
   

### Database Schema

#### **meter_readings Table**
| Field         | Type            | Description                              |
|--------------|----------------|------------------------------------------|
| id           | UUID field       | Unique identifier                 |
| nmi        | CharField (max length 10, not null)      | Record identifier                             |
| timestamp         | DateTimeField , not null   | Record timestamp                  |
| consumption | IntegerField , not null   | Avg Consumption                 |



## Installation

### Requirements
- Python 3.13+
- Postgres 14+ (reference guide https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database )

### Setup
To run the project, make sure you are inside the root folder "meter-data-pipeline" and then follow these steps:

1. **Create a virtual environment (optional but recommended)**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
2. **Install the requirements**:
   ```bash
   pip install -r requirements.txt
3. **Navigate to the main app folder**:
   ```bash
   cd src
4. **Add database details in settings.py**:
   ```bash
   vi src/settings.py
   ```
   Scroll down to find DATABASES and enter the details
   ```bash
   DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': '<db_name>',
        'USER': '<user>',
        'PASSWORD': '<password>',
        'HOST': 'localhost',
        'PORT': '5432',
    }
    }
   ```
   Save and close the file
 

5. **Setup the database**:
   ```bash
   python manage.py migrate

Your project setup is complete.

### Command Usages

Always make sure you are in the directory "meter-data-pipeline/src"

#### Running the App

To start the app, use the following command:
   ```bash
   python manage.py parse_data --file <absolute_path_to_nem12_data.csv>
   ```
#### Tests
To run the tests from command line, use the following command:
   ```bash
   python manage.py test
   ```
### Folder Structure
Here is the folder structure of the project:

```bash
meter_data_pipeline/src
├── data_ingestion_app             - App for NEM12 data ingestion
│   ├── admin.py                      
│   ├── apps.py
│   ├── constants.py                    - Configs specific to the app
│   ├── management
│   │   └── commands
│   │       └── parse_data.py.        - Custom command for initiating data parsing
│   ├── migrations                        - Versioned changed to database
│   │   ├── __init__.py
│   │   ├── 0001_initial.py
│   │   ├── 0002_alter_meterreadings_id.py
│   │   ├── 0003_alter_meterreadings_timestamp.py
│   │   ├── 0004_alter_meterreadings_timestamp.py
│   │   └── 0005_alter_meterreadings_nmi.py
│   ├── models.py                    - Database tables
│   ├── services.py                   - Logic for parsing and writing output file
│   ├── tests.py                         - Contains unit tests and integration tests
│   └── views.py                       - Logic for API (not used for now) 
├── manage.py
└── src                                      - Project settings module
    ├── asgi.py
    ├── settings.py
    ├── urls.py
    └── wsgi.py
```

