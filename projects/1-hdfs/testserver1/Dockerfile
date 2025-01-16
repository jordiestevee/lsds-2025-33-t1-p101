# Use an official Python image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . /app

# Install dependencies
RUN pip install fastapi uvicorn

# Expose the API port
EXPOSE 8000

# Command to run the FastAPI service
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
