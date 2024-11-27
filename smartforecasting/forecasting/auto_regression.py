import json
from typing import Final

import numpy as np
import pandas as pd
from statsmodels.tsa.ar_model import AutoReg

from forecasting.models import ForecastRegistry
from forecasting.models import ForecastStrategy
from forecasting.utility import add_time
from forecasting.utility import auto_stationary
from forecasting.utility import find_best_lag_pvalues
from forecasting.utility import generate_range_datetime
from forecasting.utility import make_stationary
from forecasting.utility import reconstruct_series_from_stationary
from logging_config import logger
from structs.enums import ForecastModel


@ForecastRegistry.register(ForecastModel.AUTO_REGRESSION)
class AutoRegression(ForecastStrategy):
    MAX_LAGS: Final[int] = 15
    stationary: bool = False
    model_params = None

    def train(self, data, frequency="1D"):
        logger.info("Training Data with Auto Regression ...")
        original_data = data.copy()

        data.index = data["ts"]
        data.index = pd.to_datetime(data.index)

        full_index = pd.date_range(
            start=data.index.min(), end=data.index.max(), freq=frequency
        )

        # Step 2: Reindex your DataFrame to this complete datetime index
        data = data.reindex(full_index)
        data["ts"] = data.index
        data.index.freq = frequency
        # Step 3: Interpolate or impute missing values
        data["value"] = data["value"].fillna(0.0)
        data = data.reset_index(drop=True)

        if self.stationary:
            stationary_data, nb_diffs = auto_stationary(data["value"])
            stationary_data = pd.concat(
                [pd.Series([data.iloc[0, -1]], index=[data.index[0]]), stationary_data]
            )
        else:
            stationary_data, nb_diffs = data["value"], 0

        optimal_lag = find_best_lag_pvalues(stationary_data, self.MAX_LAGS)
        logger.info(optimal_lag)

        model = AutoReg(stationary_data, lags=optimal_lag).fit()
        self.model_params = [param for param in model.params]
        self.model_params.append(0)

        self.vector_db.set(self.vector_id, json.dumps(self.model_params))

        start_index = len(model.params)  # The index in df where the forecast starts
        end_index = len(data) - 1  # The index in df where the forecast ends

        data.reset_index(inplace=True)
        forecast = model.predict(start=0, end=end_index)[start_index:]
        logger.info(f"forecast_data: {forecast}")

        # Create a new DataFrame for the forecasted values
        forecast_data = pd.DataFrame(
            {"ts": data[start_index:]["ts"], "value": forecast}
        )

        forecast_data.loc[:, "value"] = forecast_data["value"].fillna(0)

        forecast_data = forecast_data[
            forecast_data["ts"].isin(original_data[start_index:]["ts"])
        ]

        if self.stationary:
            prepend_value = pd.Series([original_data.iloc[start_index - 1, 1]])
            reconstructed_series = reconstruct_series_from_stationary(
                pd.concat([prepend_value, forecast_data["value"]], ignore_index=True),
                nb_diffs,
            )[1:]

            forecast_data["value"] = reconstructed_series.values

        logger.info(len(forecast_data))
        logger.info(f"forecast_data: {forecast_data}")
        return forecast_data

    def __forecast_next_value(self, stationary_data: np.ndarray) -> float:
        """
        Forecast the next value based on stationary data using model parameters.
        """
        if not self.model_params:
            return -1

        # Ensure model parameters and stationary data are aligned
        assert len(self.model_params) - 2 == len(
            stationary_data
        ), "Mismatch between model parameters and stationary data length"

        # Calculate the forecasted value
        y_t = self.model_params[0] + sum(
            param * value
            for param, value in zip(self.model_params[1:], stationary_data)
        )
        return y_t

    def forecast(
        self, data: pd.DataFrame, date: str, steps: int = 1, frequency: str = "1D"
    ) -> pd.DataFrame | None:
        """
        Forecast future values up to a given date with specified steps and frequency.
        """
        if not self.model_params or data is None:
            return None

        # Ensure the date is not in the past
        last_date = data["ts"].iloc[-1]
        if pd.Timestamp(date) < last_date:
            return None

        # Initialize result and range for forecasting
        result = []
        end_range = add_time(date, frequency, steps)

        for index, timestamp in enumerate(
            generate_range_datetime(last_date, end_range, frequency)
        ):
            # Prepare stationary data
            if self.stationary:
                stationary_data = make_stationary(
                    data.iloc[index:, -1], lag=self.model_params[-1]
                )
                forecast_value = self.__forecast_next_value(stationary_data) + float(
                    data.iloc[-1, -1]
                )
            else:
                stationary_data = data.iloc[index:, -1].values
                forecast_value = self.__forecast_next_value(stationary_data)

            # Append the forecasted value and update the dataset
            result.append(forecast_value)
            new_row = pd.DataFrame({"ts": [timestamp], "value": [forecast_value]})
            data = pd.concat([data, new_row], ignore_index=True)

        return data

    def get_nb_lags_needed(self) -> int:
        """
        Calculate the number of lags required by the model.
        """
        if not self.model_params:
            return -1

        # Number of lags is determined by the length of parameters and the lag indicator
        return len(self.model_params) - 2 + self.model_params[-1]
