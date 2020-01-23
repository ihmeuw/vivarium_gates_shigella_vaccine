"""All generic functionality for standardizing raw data."""
from numbers import Real
from typing import List

import pandas as pd
from vivarium_inputs import globals as vi_globals
from vivarium_inputs import utilities, utility_data

from vivarium_gates_shigella_vaccine import globals as project_globals


FERTILE_AGE_GROUP_IDS = list(range(7, 15 + 1))  # need for calc live births by sex


###############################################################
# Functions to normalize GBD data over a standard demography. #
###############################################################

def normalize(data: pd.DataFrame, fill_value: Real = None,
              cols_to_fill: List[str] = vi_globals.DRAW_COLUMNS) -> pd.DataFrame:
    data = utilities.normalize_sex(data, fill_value, cols_to_fill)
    data = normalize_year(data)
    data = normalize_age(data, fill_value, cols_to_fill)
    return data


def normalize_year(data: pd.DataFrame) -> pd.DataFrame:
    years = range(project_globals.MIN_YEAR, project_globals.MAX_YEAR + 1)

    if 'year_id' not in data.columns:
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


def normalize_age(data: pd.DataFrame, fill_value: Real, cols_to_fill: List[str]) -> pd.DataFrame:
    data_ages = set(data.age_group_id.unique()) if 'age_group_id' in data.columns else set()
    gbd_ages = set(utility_data.get_age_group_ids())

    if not data_ages:
        # Data does not correspond to individuals, so no age column necessary.
        pass
    elif data_ages == {vi_globals.SPECIAL_AGES['all_ages']}:
        # Data applies to all ages, so copy.
        dfs = []
        for age in gbd_ages:
            missing = data.copy()
            missing.loc[:, 'age_group_id'] = age
            dfs.append(missing)
        data = pd.concat(dfs, ignore_index=True)
    elif data_ages < gbd_ages:
        # Data applies to subset, so fill other ages with fill value.
        key_columns = list(data.columns.difference(cols_to_fill))
        key_columns.remove('age_group_id')
        expected_index = pd.MultiIndex.from_product([data[c].unique() for c in key_columns] + [gbd_ages],
                                                    names=key_columns + ['age_group_id'])

        data = (data.set_index(key_columns + ['age_group_id'])
                .reindex(expected_index, fill_value=fill_value)
                .reset_index())
    elif data_ages > gbd_ages:
        data = data[data.age_group_id.isin(gbd_ages)]
    else:  # data_ages == gbd_ages
        pass
    return data
