FROM apache/beam_python3.8_sdk:latest

# Copy your pipeline code
COPY . /app
WORKDIR /app

# Install additional dependencies, if needed
RUN pip install -r requirements.txt

# Specify the entrypoint for your pipeline
ENTRYPOINT ["python", "twitter_silver_to_gold_v1.py"]
