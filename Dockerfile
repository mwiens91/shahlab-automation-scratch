# Use Ubuntu as base image
FROM ubuntu

# Get pip
RUN apt-get -yqq update && apt-get install -yqq python-pip

# Set the working directory to /app
WORKDIR /shahlab_automation

# Copy the current directory contents into the container at /app
ADD automate_me/* /shahlab_automation

# Install requirements
RUN pip install -r requirements.txt

# Logs can be collected here
RUN mkdir /logs
