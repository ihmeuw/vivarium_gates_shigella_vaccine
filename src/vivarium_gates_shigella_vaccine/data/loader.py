"""Loads, standardizes and validates input data for the simulation."""
import pandas as pd

from vivarium.framework.artifact import EntityKey
from vivarium_inputs import utilities

from vivarium_gates_shigella_vaccine import paths
from vivarium_gates_shigella_vaccine import globals as project_globals
from vivarium_gates_shigella_vaccine.data import extract, standardize, validate

BASE_COLUMNS = ['year_start', 'year_end', 'age_group_start', 'age_group_end', 'draw', 'sex']


def get_data(key: EntityKey, location: str):
    mapping = {
        EntityKey('population.structure'): load_population_structure,
        EntityKey('population.age_bins'): load_age_bins,
        EntityKey('population.demographic_dimensions'): load_demographic_dimensions,
    }

    return mapping[key](key, location)


def load_population_structure(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    path = paths.forecast_data_path(key)
    data = extract.load_forecast_from_xarray(path, location_id)
    data = data[data.scenario == project_globals.FORECASTING_SCENARIO].drop(columns='scenario')
    data = data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id', 'draw']).unstack()
    data.columns = pd.Index([f'draw_{i}' for i in range(1000)])
    data = data.reset_index()
    data = standardize.normalize(data)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_age_bins(key: EntityKey, location_id: int):
    return extract.get_age_bins()


def load_demographic_dimensions(key: EntityKey, location_id: int):
    age_bins = extract.get_age_bins().loc[:, ['age_group_start', 'age_group_end']]

    sex_data = []
    for sex in ['Male', 'Female']:
        df = age_bins.copy()
        df['sex'] = sex
        sex_data.append(df)

    sex_data = pd.concat([sex_data], ignore_index=True)

    years = range(project_globals.MIN_YEAR, project_globals.MAX_YEAR + 1)
    year_data = []
    for year_start in years:
        df = sex_data.copy()
        df['year_start'] = year_start
        df['year_end'] = year_start + 1
        year_data.append(df)

    return pd.concat(year_data, ignore_index=True)



