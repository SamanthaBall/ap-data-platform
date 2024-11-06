import logging
import json

# Configure a structured logger
logging.basicConfig(level=logging.INFO)

def log_info(message, **kwargs):
    log_entry = {"message": message, "severity": "INFO", **kwargs}
    logging.info(json.dumps(log_entry))

def log_error(message, **kwargs):
    log_entry = {"message": message, "severity": "ERROR", **kwargs}
    logging.error(json.dumps(log_entry))
