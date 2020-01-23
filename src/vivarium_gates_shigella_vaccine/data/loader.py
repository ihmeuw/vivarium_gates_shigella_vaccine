"""Loads, standardizes and validates input data for the simulation."""
# Only using the diarrhea causes, whose metadata is stable across
# GBD 2016 and GBD 2017
from gbd_mapping import causes
from loguru import logger
import pandas as pd
from vivarium.framework.artifact import EntityKey
from vivarium_inputs import utilities, interface, utility_data, globals as vi_globals

from vivarium_gates_shigella_vaccine import paths
from vivarium_gates_shigella_vaccine import globals as project_globals
from vivarium_gates_shigella_vaccine.data import extract, standardize

BASE_COLUMNS = ['year_start', 'year_end', 'age_group_start', 'age_group_end', 'draw', 'sex']


def get_data(lookup_key: EntityKey, location: str):
    mapping = {
        EntityKey('population.structure'): (
            load_forecast_data,
            EntityKey('population.structure')
        ),
        EntityKey('population.age_bins'): (
            load_age_bins,
            EntityKey('population.age_bins')
        ),
        EntityKey('population.demographic_dimensions'): (
            load_demographic_dimensions,
            EntityKey('population.demographic_dimensions')
        ),
        EntityKey('population.theoretical_minimum_risk_life_expectancy'): (
            load_theoretical_minimum_risk_life_expectancy,
            EntityKey('population.theoretical_minimum_risk_life_expectancy')
        ),
        EntityKey('population.location_specific_life_expectancy'): (
            load_location_specific_life_expectancy,
            EntityKey('population.location_specific_life_expectancy')
        ),
        EntityKey('cause.all_causes.cause_specific_mortality_rate'): (
            load_forecast_data,
            EntityKey('cause.all_causes.cause_specific_mortality')
        ),
        EntityKey('covariate.live_births_by_year.estimate'): (
            load_live_births_by_year,
            EntityKey('covariate.live_births_by_year.estimate')
        ),
        EntityKey('cause.shigellosis.cause_specific_mortality_rate'): (
            load_forecast_data,
            EntityKey('etiology.shigellosis.cause_specific_mortality')
        ),
        EntityKey('cause.shigellosis.incidence_rate'): (
            load_forecast_data,
            EntityKey('etiology.shigellosis.incidence')
        ),
        EntityKey('cause.shigellosis.remission_rate'): (
            load_shigella_remission_rate,
            EntityKey('cause.shigellosis.remission_rate')
        ),
        EntityKey('cause.shigellosis.disability_weight'): (
            load_shigella_disability_weight,
            EntityKey('cause.shigellosis.disability_weight')
        ),
        EntityKey('covariate.dtp3_coverage_proportion.estimate'): (
            load_forecast_data,
            EntityKey('covariate.dtp3_coverage_proportion.estimate')
        ),
        EntityKey('covariate.measles_vaccine_coverage_proportion.estimate'): (
            load_forecast_data,
            EntityKey('covariate.measles_vaccine_coverage_proportion.estimate')
        ),
        EntityKey('covariate.measles_vaccine_coverage_2_doses_proportion.estimate'): (
            load_forecast_data,
            EntityKey('covariate.measles_vaccine_coverage_2_doses_proportion.estimate')
        ),
    }
    loader, access_key = mapping[lookup_key]
    return loader(access_key, location)


def load_forecast_data(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    path = paths.forecast_data_path(key)
    data = extract.load_forecast_from_xarray(path, location_id)
    data = data[data.scenario == project_globals.FORECASTING_SCENARIO].drop(columns='scenario')
    if key == EntityKey('etiology.shigellosis.incidence'):
        # Only one draw for incidence
        data = pd.concat(
            project_globals.NUM_DRAWS * [data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id']).value],
            axis=1
        )
    else:
        data = data.set_index(['location_id', 'age_group_id', 'sex_id', 'year_id', 'draw']).unstack()
    if len(data.columns) == 100:  # Not 1000 draws for everything
        data = pd.concat([data]*10, axis=1)
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
    data = _get_raw_demographic_dimensions(location)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def _get_raw_demographic_dimensions(location: str):
    location_id = extract.get_location_id(location)
    ages = utility_data.get_age_group_ids()
    years = range(project_globals.MIN_YEAR, project_globals.MAX_YEAR + 1)
    sexes = [vi_globals.SEXES['Male'], vi_globals.SEXES['Female']]
    location_id = [location_id]
    values = [location_id, sexes, ages, years]
    names = ['location_id', 'sex_id', 'age_group_id', 'year_id']

    data = (pd.MultiIndex
            .from_product(values, names=names)
            .to_frame(index=False))
    data = standardize.normalize(data)
    data = utilities.reshape(data)
    return data


def load_theoretical_minimum_risk_life_expectancy(key: EntityKey, location: str):
    return interface.get_theoretical_minimum_risk_life_expectancy()


def load_location_specific_life_expectancy(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    data = extract.get_location_specific_life_expectancy(location_id)
    data = data.rename(columns={'age': 'age_start'})
    data['age_end'] = data.age_start.shift(-1).fillna(5.01)
    earliest_year = data[data.year_id == 2025]
    out = []
    for year in range(project_globals.MIN_YEAR, 2025):
        df = earliest_year.copy()
        df['year_id'] = year
        out.append(df)
    data = pd.concat(out + [data], ignore_index=True)
    data = utilities.normalize_sex(data, None, ['value'])
    data = standardize.normalize_year(data)
    data = utilities.reshape(data, value_cols=['value'])
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_live_births_by_year(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    asfr_key = EntityKey('covariate.age_specific_fertility_rate.estimate')
    pop_key = EntityKey('population.structure')

    asfr_data = extract.load_forecast_from_xarray(paths.forecast_data_path(asfr_key), location_id)
    asfr_data = asfr_data[(asfr_data.scenario == project_globals.FORECASTING_SCENARIO)
                          & (asfr_data.year_id >= project_globals.MIN_YEAR)].drop(columns='scenario')
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


def load_shigella_remission_rate(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)
    data = extract.get_modelable_entity_draws(causes.diarrheal_diseases.dismod_id, location_id)
    data = data[data.measure_id == vi_globals.MEASURES['Remission rate']]
    data = utilities.filter_data_by_restrictions(data, causes.diarrheal_diseases,
                                                 'yld', utility_data.get_age_group_ids())
    data = data[data.year_id == 2016].drop(columns='year_id')  # Use latest GBD results for all data
    data = standardize.normalize(data, fill_value=0)
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def load_shigella_disability_weight(key: EntityKey, location: str):
    location_id = extract.get_location_id(location)

    data = _get_raw_demographic_dimensions(location)
    data = pd.DataFrame(0, columns=vi_globals.DRAW_COLUMNS, index=data)
    data = data.set_index(utilities.get_ordered_index_cols(data.columns.difference(vi_globals.DRAW_COLUMNS)))

    for sequela in causes.diarrheal_diseases.sequelae:
        prevalence = _load_prevalence(sequela, location_id, 'sequela')
        disability = _load_diarrhea_sequela_disability_weight(sequela, location_id)
        disability.index = disability.index.set_levels([location_id], 'location_id')
        data += prevalence * disability

    diarrhea_prevalence = _load_prevalence(causes.diarrheal_diseases, location_id, 'cause')
    data = (data / diarrhea_prevalence).fillna(0).reset_index()
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    data = utilities.split_interval(data, interval_column='age', split_column_prefix='age')
    data = utilities.split_interval(data, interval_column='year', split_column_prefix='year')
    return utilities.sort_hierarchical_data(data)


def _load_prevalence(entity, location_id: int, entity_type: str):
    logger.info(f'Loading prevalence for {entity.name} from GBD 2016.')
    data = extract.get_como_draws(entity.gbd_id, location_id, entity_type)
    data = data[data.measure_id == vi_globals.MEASURES['Prevalence']]
    data = utilities.filter_data_by_restrictions(data, causes.diarrheal_diseases,
                                                 'yld', utility_data.get_age_group_ids())
    data = data[data.year_id == 2016].drop(columns='year_id')  # Use latest GBD results for all data
    data = standardize.normalize(data, fill_value=0)
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS)
    return utilities.reshape(data)


def _load_diarrhea_sequela_disability_weight(sequela, location_id: int):
    logger.info(f'Loading disability weight for {sequela.name} from GBD 2016.')
    data = extract.get_auxiliary_data('disability_weight', 'sequela', 'all', location_id)
    data = data.loc[data.healthstate_id == sequela.healthstate.gbd_id, :]
    data = standardize.normalize(data)
    data = utilities.clear_disability_weight_outside_restrictions(data, causes.diarrheal_diseases, 0.0,
                                                                  utility_data.get_age_group_ids())
    data = data.filter(vi_globals.DEMOGRAPHIC_COLUMNS + vi_globals.DRAW_COLUMNS)
    return utilities.reshape(data)
