# Use the official Python image as base
FROM python:latest

# Set working directory inside the container
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install FastAPI with all extras and pandas
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI app file into the container
COPY main.py .
COPY data/ ./data

# Expose the port that FastAPI will run on
EXPOSE 8000

# Command to run the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]