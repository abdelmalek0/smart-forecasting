import base64
import time

from flask import Blueprint
from flask import jsonify
from flask import make_response
from flask import request
from pydantic import ValidationError

from async_tasks import process_file
from config import Config
from constants import BASE_PATH
from logging_config import logger
from structs.models import DataSource
from structs.models import DataSourceInfo
from structs.models import Training
from utility import find_data_source_by_id

bp = Blueprint("datasources", __name__)


@bp.route(f"{BASE_PATH}/datasources", methods=["POST"])
def create_datasource():
    """
    file: ../../docs/create_datasource.yaml
    """
    logger.info("Creating a new data source...")

    # Get JSON data from the request
    data = request.get_json()
    try:
        datasource_info = DataSourceInfo(**data)
    except ValidationError as e:
        logger.error(f"Invalid input data: {e}")
        return jsonify(error="Invalid input data"), 400

    # Create a new DataSource object
    datasource = DataSource(
        id=int(time.time()),
        datasource_info=datasource_info,
        training=Training(models=["auto-regression"]),
        initialized=False,
        trained=False,
    )

    try:
        # Save the data source to Redis
        Config.redis_handler.add_data_source(datasource.model_dump())
    except Exception as e:
        logger.error(f"Failed to save data source: {e}")
        return jsonify(error="Failed to create data source"), 500

    logger.info(f"Data source created successfully with ID: {datasource.id}")
    return jsonify({"id": datasource.id}), 201


@bp.route(f"{BASE_PATH}/datasources/all", methods=["GET"])
def get_all_datasources():
    """
    file: ../../docs/get_all_datasources.yaml
    """
    logger.info("Fetching all data sources...")

    try:
        datasources = Config.redis_handler.get_all_data_sources()
        logger.info(f"Successfully retrieved {len(datasources)} data sources")
        return jsonify(datasources), 200
    except Exception as e:
        logger.error(f"Failed to fetch data sources: {e}")
        return jsonify(error="Failed to retrieve data sources"), 500


@bp.route(f"{BASE_PATH}/datasources/<int:datasource_id>", methods=["DELETE"])
def delete_datasource(datasource_id: int):
    """
    file: ../../docs/delete_datasource.yaml
    """
    logger.info(f"Deleting data source with ID: {datasource_id}")

    # Check if the request has 'htmx' query parameter
    htmx = request.args.get("htmx")

    # Fetch all data sources from Redis
    datasources = Config.redis_handler.get_all_data_sources()

    # Validate the data source ID
    if find_data_source_by_id(datasource_id, datasources) is None:
        message = "Data source not found"
        logger.error(message)
        return jsonify(error=message), 404

    try:
        # Remove the data source from Redis and database
        Config.redis_handler.remove_data_source(datasource_id)
        Config.database.delete_datasource(datasource_id)

        logger.info(f"Data source with ID {datasource_id} deleted successfully")

        if not htmx:
            return jsonify(message=f"Data source with ID {datasource_id} deleted!"), 200
        else:
            return make_response("", 200)

    except Exception as e:
        logger.error(f"Failed to delete data source {datasource_id}: {e}")
        return jsonify(
            error=f"Failed to delete data source with ID {datasource_id}"
        ), 500


@bp.route(
    f"{BASE_PATH}/datasources/<int:datasource_id>/initialization", methods=["POST"]
)
def initialize_datasource(datasource_id: int):
    """
    file: ../../docs/initialize_datasource.yaml
    """
    logger.info(f"Initializing data source with ID: {datasource_id}")

    # Ensure the request contains a file
    if "file" not in request.files:
        message = "No file found in the request"
        logger.error(message)
        return jsonify(error=message), 400

    file = request.files["file"]

    # Ensure the file has a valid filename
    if file.filename == "":
        message = "No file selected"
        logger.error(message)
        return jsonify(error=message), 400

    # Fetch all data sources from Redis
    datasources = Config.redis_handler.get_all_data_sources()

    # Validate the data source ID
    data_source_match = find_data_source_by_id(datasource_id, datasources)
    if data_source_match is None:
        logger.error(f"Data source not found with ID: {datasource_id}")
        return jsonify(error=f"Data source not found with ID {datasource_id}"), 404

    try:
        # Find the data source by ID
        datasource_str, datasource_index = data_source_match
        datasource = DataSource(**datasource_str)

        if not datasource.initialized:
            # Read and encode the file
            file_data = file.read()
            file_data_base64 = base64.b64encode(file_data).decode("utf-8")

            # Asynchronously process the file
            process_task = process_file.apply_async(
                args=[file_data_base64, datasource_id, Config.db_config]
            )
            logger.info(f"File processing task started with ID: {process_task.id}")
            return jsonify({"task_id": process_task.id}), 202
        else:
            message = f"Data source with ID {datasource_id} is already initialized"
            logger.info(message)
            return jsonify(message=message), 200

    except Exception as e:
        message = f"Error initializing data source: {e}"
        logger.error(message)
        return jsonify(error=message), 500
