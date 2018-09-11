# Use an official Python runtime as a parent image
FROM python:latest

# Set the working directory to /app
WORKDIR /app

# Copy the requirement.txt file
COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the whole project
COPY . /app

# Define environment variable
ENV ROOT_MODULE ${PWD}/soweego \
    PYTHONPATH ${ROOT_MODULE}:${ROOT_MODULE}/{commons,target_selection,wikidata}:${ROOT_MODULE}/target_selection/common:${ROOT_MODULE}/target_selection/bne:${ROOT_MODULE}/target_selection/discogs:${ROOT_MODULE}/target_selection/musicbrainz:${ROOT_MODULE}/target_selection/bibsys

# Run app.py when the container launches
CMD ["/bin/bash"]