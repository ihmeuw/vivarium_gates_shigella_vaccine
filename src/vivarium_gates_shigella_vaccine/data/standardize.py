"""All generic functionality for standardizing raw data."""
import math
from numbers import Real
from typing import List

import pandas as pd
from vivarium_inputs import globals as vi_globals
from vivarium_inputs import utilities

from vivarium_gates_shigella_vaccine import globals as project_globals


FERTILE_AGE_GROUP_IDS = list(range(7, 15 + 1))  # need for calc live births by sex


###############################################################
# Functions to normalize GBD data over a standard demography. #
###############################################################

def normalize(data: pd.DataFrame, fill_value: Real = None,
              cols_to_fill: List[str] = vi_globals.DRAW_COLUMNS) -> pd.DataFrame:
    data = utilities.normalize_sex(data, fill_value, cols_to_fill)
    data = normalize_year(data)
    data = utilities.normalize_age(data, fill_value, cols_to_fill)
    return data


def normalize_year(data: pd.DataFrame) -> pd.DataFrame:
    years = range(project_globals.MIN_YEAR, project_globals.MAX_YEAR + 1)

    if 'year_id' not in data:
        # Data doesn't vary by year, so copy for each year.
        df = []
        for year in years:
            fill_data = data.copy()
            fill_data['year_id'] = year
            df.append(fill_data)
        data = pd.concat(df, ignore_index=True)
    elif set(data.year_id) >= set(years):
        # We have at least the required years, so filter down
        data = data.loc[data.year_id.isin(years)]
    else:  # We don't have a strategy.
        raise ValueError('Years in data are incompatible with normalization strategies.')

    return data
