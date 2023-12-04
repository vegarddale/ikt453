# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container to /app
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    g++ \
    curl \
    unixodbc-dev \
    apt-transport-https \
    gpg \
  && apt-get clean -y

# Download the Microsoft GPG key
RUN curl https://packages.microsoft.com/keys/microsoft.asc -o microsoft.gpg

# Import the key
RUN gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/microsoft-archive-keyring.gpg --import microsoft.gpg

# Change the permissions of the keyring file
RUN chmod 644 /etc/apt/trusted.gpg.d/microsoft-archive-keyring.gpg

# Add the Microsoft repository
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
# sudo su -c "curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" 
# if run locally

# Update the package list and install the necessary tools
RUN apt-get update && \
 ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools
# Locally: sudo su -c "apt-get update -o DPkg::options::='--force-overwrite' 
# && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools"
# maybe have to run: sudo su -c "apt --fix-broken -o DPkg::options::='--force-overwrite' install"
 

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run app.py when the container launches
CMD ["python", "app.py"]
