"""Loads, standardizes and validates input data for the simulation."""
import pandas as pd

from vivarium.framework.artifact import EntityKey
from vivarium_inputs import utilities, interface, utility_data, globals as vi_globals

from vivarium_gates_shigella_vaccine import paths
from vivarium_gates_shigella_vaccine import globals as project_globals
from vivarium_gates_shigella_vaccine.data import extract, standardize, validate

BASE_COLUMNS = ['year_start', 'year_end', 'age_group_start', 'age_group_end', 'draw', 'sex']


def get_data(key: EntityKey, location: str):
    mapping = {
        EntityKey('population.structure'): load_population_structure,
        EntityKey('population.age_bins'): load_age_bins,
        EntityKey('population.demographic_dimensions'): load_demographic_dimensions,
        EntityKey('population.theoretical_minimum_risk_life_expectancy'): load_theoretical_minimum_risk_life_expectancy,
        EntityKey('population.location_specific_life_expectancy'): load_location_specific_life_expectancy,
        EntityKey('cause.all_causes.cause_specific_mortality_rate'): load_all_cause_mortality_rate,
        EntityKey('covariate.live_births_by_year.estimate'): load_live_births_by_year,
        EntityKey('cause.shigellosis.cause_specific_mortality_rate'): load_shigella_cause_specific_mortality_rate,
        EntityKey('cause.shigellosis.incidence_rate'): load_shigella_incidence_rate,
        EntityKey('cause.shigellosis.remission_rate'): load_shigella_remission_rate,
        EntityKey('cause.shigellosis.disability_weight'): load_shigella_disability_weight,
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


def load_age_bins(key: EntityKey, location: str):
    return interface.get_age_bins()


def load_demographic_dimensions(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    ages = utility_data.get_age_group_ids()
    years = range(project_globals.MIN_YEAR, project_globals.MAX_YEAR + 1)
    sexes = [vi_globals.SEXES['Male'], vi_globals.SEXES['Female']]
    location = [location_id]
    values = [location, sexes, ages, years]
    names = ['location_id', 'sex_id', 'age_group_id', 'year_id']

    data = (pd.MultiIndex
            .from_product(values, names=names)
            .to_frame(index=False))

    data = standardize.normalize(data)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_theoretical_minimum_risk_life_expectancy(key: EntityKey, location: str):
    return interface.get_theoretical_minimum_risk_life_expectancy()


def load_location_specific_life_expectancy(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    data = extract.get_location_specific_life_expectancy(location_id)
    data = data.rename(columns={'age': 'age_start'})
    data['age_end'] = data.age_start.shift(-1).fillna(5.01)
    data = utilities.normalize_sex(data, None, ['value'])
    data = utilities.normalize_year(data)
    utilities.reshape(data, value_cols=['value'])
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_all_cause_mortality_rate(key: EntityKey, location: str):
    lookup_key = EntityKey('cause.all_causes.cause_specific_mortality')
    location_id = extract.get_location_id(location)
    path = paths.forecast_data_path(lookup_key)
    data = extract.load_forecast_from_xarray(path, location_id)
    data = data[data.scenario == project_globals.FORECASTING_SCENARIO].drop(columns='scenario')
    data = data.rename(columns={'_all': 'value'})
    data = data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id', 'draw']).unstack()
    data.columns = pd.Index([f'draw_{i}' for i in range(1000)])
    data = data.reset_index()
    data = standardize.normalize(data)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_live_births_by_year(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    asfr_key = EntityKey('covariate.age_specific_fertility_rate.estimate')
    pop_key = EntityKey('population.structure')

    asfr_data = extract.load_forecast_from_xarray(paths.forecast_data_path(asfr_key), location_id)
    asfr_data = asfr_data[(asfr_data.scenario == project_globals.FORECASTING_SCENARIO)
                          & (asfr_data >= project_globals.MIN_YEAR)].drop(columns='scenario')
    asfr_data = asfr_data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id', 'draw']).unstack()
    asfr_data.columns = pd.Index([f'draw_{i}' for i in range(1000)])

    pop_data = extract.load_forecast_from_xarray(paths.forecast_data_path(pop_key), location_id)
    pop_data = pop_data[(pop_data.scenario == project_globals.FORECASTING_SCENARIO)].drop(columns='scenario')
    pop_data = pop_data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id', 'draw']).unstack()
    pop_data.columns = pd.Index([f'draw_{i}' for i in range(1000)])
    pop_data = pop_data.loc[asfr_data.index]

    live_births = asfr_data * pop_data
    live_births = (live_births
                   .reset_index()
                   .drop(columns=['sex_id', 'age_group_id'])
                   .groupby(['location_id', 'year_id'])
                   .sum()
                   .reset_index())

    data = standardize.normalize(live_births)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_shigella_cause_specific_mortality_rate(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    lookup_key = EntityKey('etiology.shigellosis.cause_specific_mortality')
    data = extract.load_forecast_from_xarray(paths.forecast_data_path(lookup_key), location_id)
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


def load_shigella_incidence_rate(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    lookup_key = EntityKey('etiology.shigellosis.incidence')
    data = extract.load_forecast_from_xarray(paths.forecast_data_path(lookup_key), location_id)
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


def load_shigella_remission_rate(key: EntityKey, location: str):
    pass


def load_shigella_disability_weight(key: EntityKey, location: str):
    pass
