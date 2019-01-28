# Use an official Python runtime as a parent image
FROM python:latest

# Set the working directory to /app
WORKDIR /app

RUN mkdir /app/shared

# Copy the requirement.txt file
COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the whole project
COPY . /app/soweego

# Move to soweego folder
WORKDIR /app/soweego

# Defines the entry point of the image
ENTRYPOINT [ "python", "-m", "soweego", "run" ]
