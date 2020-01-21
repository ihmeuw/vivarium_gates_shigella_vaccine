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



def get_age_group_bins_from_age_group_id(df):
    """Creates "age_group_start" and "age_group_end" columns from the "age_group_id" column
    Parameters
    ----------
    df: df for which you want an age column that has an age_group_id column
    Returns
    -------
    df with "age_group_start" and "age_group_end" columns
    """
    if df.empty:
        df['age_group_start'] = 0
        df['age_group_end'] = 0
        return df

    df = df.copy()
    idx = df.index
    mapping = get_age_bins()
    mapping = mapping.set_index('age_group_id')

    df = df.set_index('age_group_id')
    df[['age_group_start', 'age_group_end']] = mapping[['age_group_years_start', 'age_group_years_end']]

    df = df.set_index(idx)

    return df


def rename_value_columns(data: pd.DataFrame, value_column: str = 'value') -> pd.DataFrame:
    """Standardize the value column name."""
    if not ('value' in data or 'mean_value' in data):
        # we need to rename the value column to match vivarium_inputs convention
        col = set(data.columns) - {'year_id', 'sex_id', 'age_group_id', 'draw', 'scenario', 'location_id'}
        if len(col) > 1:
            raise ValueError(f"You have multiple value columns in your data.")
        data = data.rename(columns={col.pop(): value_column})
    return data


def replicate_data(data):
    """If data has fewer than NUM_DRAWS draws, duplicate to have the full set.
    Assumes draws in data are sequential and start at 0
    """
    if 'draw' not in data:  # for things with only 1 draw, draw isn't included as a col
        data['draw'] = 0

    full_data = data.copy()
    og_draws = data.draw.unique()
    n_draws = len(og_draws)

    if n_draws < project_globals.NUM_DRAWS:

        for i in range(1, math.ceil(project_globals.NUM_DRAWS/n_draws)):

            max_draw = max(og_draws)
            if i == math.ceil(project_globals.NUM_DRAWS/n_draws)-1 and project_globals.NUM_DRAWS % n_draws > 0:
                max_draw = project_globals.NUM_DRAWS % n_draws - 1

            draw_map = pd.Series(range(i*n_draws, i*n_draws + n_draws), index=og_draws)

            new_data = data[data.draw <= max_draw].copy()
            new_data['draw'] = new_data.draw.apply(lambda x: draw_map[x])

            full_data = full_data.append(new_data)

    return full_data
