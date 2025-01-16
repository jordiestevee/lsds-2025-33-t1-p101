# Use an official Python image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Install FastAPI and Uvicorn
RUN pip install fastapi uvicorn

# Expose the default FastAPI port
EXPOSE 8000

# Command to run the server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
