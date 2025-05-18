# Start with an official Python base image (consistent with other services)
FROM python:3.10-slim-bookworm

# Set the working directory in the container
WORKDIR /service

# Copy the requirements file into the container
COPY ./requirements.txt /service/requirements.txt

# Install Python dependencies
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /service/requirements.txt

# Copy the rest of your application's code into the container
COPY ./app /service/app

# Expose the port the app runs on (e.g., 8002 for this service)
EXPOSE 8002

# Command to run the application using Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]