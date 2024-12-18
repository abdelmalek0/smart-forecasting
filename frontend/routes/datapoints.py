from datetime import datetime
from typing import List

import numpy as np
from fasthtml.common import Div
from fasthtml.common import H5
from fasthtml.common import Input
from fasthtml.common import Label
from fasthtml.common import Option
from fasthtml.common import Select
from fasthtml.common import Table
from fasthtml.common import Td
from fasthtml.common import Th
from fasthtml.common import Thead
from fasthtml.common import Title
from fasthtml.common import Tr
from starlette.requests import Request

from components.card import Card
from components.chart import Chart
from components.filter import DateFilter
from components.navbar import Navbar
from components.table import DataTable
from utils.constants import DATAPOINTS_HEADERS
from utils.helpers import fetch_datapoints
from utils.helpers import fetch_datasources


async def generate_options_ui(datasources: list, selected_datasource_id: int) -> list:
    """
    Generate UI options for selecting a datasource.

    :param datasources: List of data sources.
    :param selected_datasource_id: ID of the currently selected datasource.
    :return: List of Option elements.
    """
    options = []

    # Add default option if no datasource is selected
    if selected_datasource_id == -1:
        options.append(
            Option("Pick a datasource", selected="selected", disabled="disabled")
        )

    # Add options for each data source
    options.extend(
        [
            Option(
                f'{data_source["datasource_info"]["name"]}',
                selected="selected"
                if data_source["id"] == selected_datasource_id
                else None,
                value=f'{data_source["id"]}',
            )
            for data_source in datasources
        ]
    )

    return options


async def build_table_content(datapoints: List[dict]) -> list:
    """
    Build table rows for datapoints.

    :param datapoints: List of datapoint dictionaries.
    :return: List of Tr elements.
    """
    rows = [
        Tr(
            Th(f"{index}"),
            Td(f'{datapoint["ts"]}'),
            Td(f'{datapoint["value"]}'),
            Td(f'{datapoint["AutoReg"]}'),
            Td(f'{datapoint["ExpSmoothing"]}'),
        )
        for index, datapoint in enumerate(datapoints)
    ]
    return rows


def get_data_point(date_str, y):
    base_date = datetime.fromisoformat(date_str)
    x = base_date.timestamp() * 1000
    return [x, y]


def generate_chart(datapoints):
    main_data = [get_data_point(dp["ts"], dp["value"]) for dp in reversed(datapoints)]
    auto_data = [get_data_point(dp["ts"], dp["AutoReg"]) for dp in reversed(datapoints)]
    exp_data = [
        get_data_point(dp["ts"], dp["ExpSmoothing"]) for dp in reversed(datapoints)
    ]

    start_date = datapoints[len(auto_data) // 4]["ts"]
    end_date = datapoints[0]["ts"]

    main_data = list(map(lambda x: [x[0], None] if not x[1] else x, main_data))
    auto_data = list(map(lambda x: [x[0], None] if not x[1] else x, auto_data))
    exp_data = list(map(lambda x: [x[0], None] if not x[1] else x, exp_data))

    return Chart(main_data, auto_data, exp_data, start_date, end_date)


async def datapoints(request: Request):
    """
    Handle the datapoints request and render the page.

    :param request: The Starlette request object.
    :return: Div element containing the datapoints page layout.
    """
    datasource_id = int(request.path_params.get("datasource_id", -1))
    page = request.query_params.get("page", None)
    start_date = request.query_params.get("start_date", None)
    end_date = request.query_params.get("end_date", None)
    latest = request.query_params.get("latest", None)

    datasources = await fetch_datasources()
    datapoints = await fetch_datapoints(
        datasource_id, start_date, end_date, latest, page
    )

    if (
        datasource_id != -1
        and latest is not None
        and len(datapoints["data"])
        and start_date is None
        and end_date is None
    ):
        start_date = datapoints["data"][-1]["ts"]

    table_content = (
        DataTable(datapoints["data"])
        if datasource_id != -1 and len(datapoints["data"])
        else Div()
    )

    chart_content = (
        [
            generate_chart(datapoints["data"]),
            Div(id="chart-line2"),
            Div(id="chart-line"),
        ]
        if datasource_id != -1 and len(datapoints["data"])
        else []
    )
    minDate = datapoints["minDate"] if datasource_id != -1 else None
    maxDate = datapoints["maxDate"] if datasource_id != -1 else None

    cards_content = []
    if datasource_id != -1:
        y_actual_auto, y_auto = (
            zip(
                *[
                    (dp["value"], dp["AutoReg"])
                    for dp in reversed(datapoints["data"])
                    if dp["value"] and dp["AutoReg"]
                ]
            )
            if any(dp["value"] and dp["AutoReg"] for dp in datapoints["data"])
            else ([], [])
        )

        y_actual_exp, y_exp = (
            zip(
                *[
                    (dp["value"], dp["ExpSmoothing"])
                    for dp in reversed(datapoints["data"])
                    if dp["value"] and dp["ExpSmoothing"]
                ]
            )
            if any(dp["value"] and dp["ExpSmoothing"] for dp in datapoints["data"])
            else ([], [])
        )

        rmse_auto = np.sqrt(((np.array(y_auto) - y_actual_auto) ** 2).mean())
        rmse_exp = np.sqrt(((np.array(y_exp) - y_actual_exp) ** 2).mean())
        if len(y_auto):
            cards_content.append(Card("Auto Regression", rmse_auto))
        if len(y_exp):
            cards_content.append(Card("Exponential Smoothing", rmse_exp))

    return (
        Title("SmartForecasting - Datapoints"),
        Div(
            Div(
                Navbar(index=2),
                Div(
                    Div(
                        H5("Data source:", cls="text-xl font-bold"),
                        Select(
                            *(await generate_options_ui(datasources, datasource_id)),
                            cls="select select-primary max-w-xs bg-gray-50",
                            hx_on="change: this.value ? window.location.href = '/datapoints/' + encodeURIComponent(this.value) + '?latest=200' : ''",
                        ),
                        Label(
                            Input(type="checkbox"),
                            Div("📈", **{"@click": "table = false"}, cls="swap-off"),
                            Div("📅", **{"@click": "table = true"}, cls="swap-on"),
                            cls="swap swap-flip text-3xl",
                        ),
                        cls="flex flex-row items-center gap-4",
                    ),
                    DateFilter(start_date, end_date, minDate, maxDate),
                    cls="flex flex-row items-center w-screen justify-between px-6",
                ),
                Div(
                    Div(*cards_content, cls="flex flex-row px-4 space-x-4"),
                    Div(
                        *chart_content,
                    ),
                    x_show="!table",
                    cls="p-5 w-screen h-screen",
                ),
                Div(
                    Table(
                        Thead(
                            Tr(*[Th(header) for header in DATAPOINTS_HEADERS]),
                            cls="bg-neutral text-white",
                        ),
                        table_content,
                        cls="table bg-gray-50",
                        id="datatable",
                    ),
                    x_show="table",
                    cls="p-5 w-screen",
                ),
                cls="flex flex-col items-start gap-4",
            ),
            x_data="{table: true}",
        ),
    )
