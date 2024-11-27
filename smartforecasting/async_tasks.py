# tasks.py
import base64
import io
import time

import pandas as pd
from celery import shared_task
from celery.contrib.abortable import AbortableTask

from database import DatabaseHandler
from forecasting.models import ForecastContext
from logging_config import logger
from redis_memory import RedisHandler
from structs.models import Training
from utility import find_data_source_by_id


@shared_task(bind=True)
def process_file(self, file_data, datasource_id: int, config: dict):
    try:
        start_time = time.perf_counter()

        # Connect to Databases
        redis_handler = RedisHandler()
        database = DatabaseHandler(config)
        database.connect()
        logger.info(f"config: {database.config}")
        logger.info(f"ds id: {datasource_id}")

        datasource_str, datasource_index = find_data_source_by_id(
            datasource_id, redis_handler.get_all_data_sources()
        )

        # Decode the file data from base64
        file_data = base64.b64decode(file_data)

        # Convert the file data to a StringIO object for pandas
        file_io = io.StringIO(file_data.decode("utf-8"))

        # Read the CSV data into a DataFrame
        df = pd.read_csv(file_io)
        database.insert_dataframe(df, datasource_id)
        # database.disconnect()
        end_time = time.perf_counter()

        redis_handler.set_item(datasource_index, "initialized", True)
        return f"Data insertion has been successfully completed in: {end_time - start_time} seconds"
    except Exception as e:
        raise Exception(e)
    finally:
        database.disconnect()  # Ensure the database connection is closed

    return "Something went wrong!"


@shared_task(bind=True, base=AbortableTask)
def process_training(
    self, training_data: str, datasource_id: int, config: dict, frequency: int
):
    start_time = time.perf_counter()

    # Initialize necessary handlers and models outside the try-except for cleaner resource handling
    redis_handler = RedisHandler()
    database = DatabaseHandler(config)
    try:
        # Parse the training data and connect to the database
        training_data_object: Training = Training.parse_raw(training_data)
        database.connect()
        logger.info(f"Config: {database.config}, Datasource ID: {datasource_id}")

        # Retrieve datasource info
        datasource_str, datasource_index = find_data_source_by_id(
            datasource_id, redis_handler.get_all_data_sources()
        )

        # Get all the data needed for training
        df = database.get_all_data_for_datasource(datasource_id)
        logger.info(f"Training data fetched: {df.head()}")

        total_models = len(training_data_object.models)

        # Iterate through all the models specified in training data
        for algorithm_index, algorithm in enumerate(training_data_object.models):
            model = ForecastContext(algorithm, datasource_id)
            forecast_data = model.train(df, frequency)

            # Inserting forecast data into the database and updating progress in a single loop
            # We gather all the rows in one step to avoid having an inner loop that just counts
            insert_result = database.insert_forecasting_dataframe(
                forecast_data, datasource_id, algorithm.value
            )

            # Only iterate once over the inserted rows, saving one iteration
            total_rows = len(insert_result)
            for index, _ in enumerate(insert_result):
                if self.is_aborted():  # Check for task abortion before continuing
                    return "TASK STOPPED!"

                # Update task progress
                self.update_state(
                    state="PROGRESS",
                    meta={
                        "current": index + 1,
                        "total": total_rows,
                        "current_model": algorithm_index,
                        "total_models": total_models,
                    },
                )

        # Mark the datasource as trained in Redis
        redis_handler.set_item(datasource_index, "trained", True)

        end_time = time.perf_counter()
        logger.info(f"Training completed in {end_time - start_time:.2f} seconds")
        return f"Training completed successfully in {end_time - start_time:.2f} seconds"
    except Exception as e:
        raise Exception(e)
    finally:
        database.disconnect()  # Ensure the database connection is closed

    return "Something went wrong!"
