#!/bin/bash

# Set the PYTHONPATH to include the parent directory
export PYTHONPATH=/home/ml/SmartForecasting

# Change to the directory where Celery should run
cd /home/ml/SmartForecasting/smartforecasting

# Get the path to the Poetry executable
POETRY=/home/ml/.pyenv/shims/poetry

# Run Celery worker within the Poetry environment
exec "$POETRY" run celery -A smartforecasting.run.celery worker --loglevel=info --logfile=/home/ml/SmartForecasting/logs/celery_worker.log --pidfile=/home/ml/SmartForecasting/logs/celery_worker.pid
