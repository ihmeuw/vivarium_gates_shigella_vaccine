"""Loads, standardizes and validates input data for the simulation."""
from vivarium.framework.artifact import EntityKey

from vivarium_gates_shigella_vaccine import paths
from vivarium_gates_shigella_vaccine.data import extract, standardize, validate

BASE_COLUMNS = ['year_start', 'year_end', 'age_group_start', 'age_group_end', 'draw', 'sex']


def get_data(key: EntityKey, location: str):
    mapping = {
        EntityKey('population.structure'): load_population_structure,
        EntityKey('population.age_bins'): load_age_bins,
    }
    location_id = extract.get_location_id(location)
    return mapping[key](key, location_id)


def load_population_structure(key: EntityKey, location_id: int):
    path = paths.forecast_data_path(key)
    data = extract.load_forecast_from_xarray(path, location_id)
    value_column = 'population'
    data = standardize.normalize_forecasting(data, value_column, sexes=['Male', 'Female', 'Both'])
    return data[BASE_COLUMNS + [value_column]]


def load_age_bins(key: EntityKey, location_id: int):
    return extract.get_age_bins()

