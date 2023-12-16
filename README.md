# GTDB Data Warehouse

Data used can be downloaded [here](https://www.start.umd.edu/gtd/contact/download).
From: START (National Consortium for the Study of Terrorism and Responses to Terrorism). (2022). Global Terrorism Database 1970 - 2020 [globalterrorismdb_0522dist.xlsx]. https://www.start.umd.edu/gtd


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Ensure that you have Docker and Docker Compose installed on your machine. If you haven't installed them yet, you can download Docker from [here](https://docs.docker.com/get-docker/) and Docker Compose from [here](https://docs.docker.com/compose/install/).

### Installing

A step by step series of examples that tell you how to get a development environment running:

1. Clone the repository to your local machine:

```bash
git clone https://github.com/vegarddale/ikt453.git
```

2. Navigate to the project directory:

```bash
cd ikt453
```

3. Add sql script file to create database and load data

If you have a script file put it in folder in the repo called 

```bash
docker-entrypoint-initdb.d
```
You can then exec into the mssql container when it is running

```bash
docker exec -it <docker_id> bash
```

and run the script file to build the database and load the data.

```bash
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P Password123. -i script.sql
```

4. Build the Docker image:

```bash
docker-compose build
```

5. Start the Docker containers:

```bash
docker-compose up
```

The application should now be running at `http://localhost:8000`.

## Built With

* [Python 3.8](https://www.python.org/) - The programming language used.
* [Docker](https://www.docker.com/) - Used for containerization.
* [Docker Compose](https://docs.docker.com/compose/) - Used for defining and running multi-container Docker applications.
