FROM apache/beam_python3.8_sdk:latest

# Copy your pipeline code
COPY . /app
WORKDIR /app

# Install additional dependencies, if needed
RUN pip install -r requirements.txt

# Specify the entrypoint for your pipeline
ENTRYPOINT ["python", "your_pipeline_script.py"]
