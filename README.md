Sure, here's a basic README file for your repository:

---

# Project Title

A brief description of what your project does and its purpose.

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

3. Build the Docker image:

```bash
docker-compose build
```

4. Start the Docker containers:

```bash
docker-compose up
```

The application should now be running at `http://localhost:8000`.

## Running the tests

Explain how to run the automated tests for this system.

## Built With

* [Python 3.8](https://www.python.org/) - The programming language used.
* [Docker](https://www.docker.com/) - Used for containerization.
* [Docker Compose](https://docs.docker.com/compose/) - Used for defining and running multi-container Docker applications.