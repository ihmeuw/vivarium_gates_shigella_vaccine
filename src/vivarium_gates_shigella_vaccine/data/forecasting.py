from loguru import logger
from vivarium.framework.artifact import EntityKey


#############################
# THIS MODULE IS DEPRECATED #
# It's only here till I     #
# move the functionality.   #
#############################

entity_map = {
    'causes': {
        'all_causes': {
            'kind': 'cause',
            'name': 'all_causes',
        },
        'diarrheal_diseases': {
            'kind': 'cause',
            'name': 'diarrheal_diseases',
        },
    },
    'etiologies': {
        'shigellosis': {
            'kind': 'etiology',
            'name': 'shigellosis'
        }
    },
    'covariates': {
        'age_specific_fertility_rate': {
            'kind': 'covariate',
            'name': 'age_specific_fertility_rate'
        },
        'live_births_by_sex': {
            'kind': 'covariate',
            'name': 'live_births_by_sex',
        },
        'dtp3_coverage_proportion': {
            'kind': 'covariate',
            'name': 'dtp3_coverage_proportion'
        },
        'measles_vaccine_coverage_proportion': {
            'kind': 'covariate',
            'name': 'measles_vaccine_coverage_proportion'
        },
        'measles_vaccine_coverage_2_doses_proportion': {
            'kind': 'covariate',
            'name': 'measles_vaccine_coverage_2_doses_proportion'
        }
    }
}


def load_forecast(entity_key: EntityKey, location: str):
    """Load forecast data."""
    entity_data = {
        "cause": {
            "mapping": entity_map['causes'],
            "getter": get_cause_data,
            "measures": ["cause_specific_mortality"]
        },
        "etiology": {
            "mapping": entity_map['etiologies'],
            "getter": get_etiology_data,
            "measures": ["incidence", "mortality"],
        },
        "population": {
            "mapping": {'': None},
            "getter": get_population_data,
            "measures": ["structure"],
        },
        "covariate": {
            "mapping": entity_map['covariates'],
            "getter": get_covariate_data,
            "measures": ["estimate"]
        },
    }
    mapping, getter, measures = entity_data[entity_key.type].values()
    entity = mapping[entity_key.name]
    data = getter(entity_key, get_location_id(location)).reset_index(drop=True)
    data['location'] = location
    validate_data(entity_key, data)
    return data


def get_cause_data(cause_key, location_id):
    """Load forecast cause data."""
    data = get_entity_measure(cause_key, location_id)
    data = standardize_data(data, 0)
    value_column = 'value'
    data = normalize_forecasting(data, value_column)
    return data[BASE_COLUMNS + [value_column]]


def get_etiology_data(etiology_key, location_id):
    """Load forecast etiology data."""
    data = get_entity_measure(etiology_key, location_id)
    data = standardize_data(data, 0)
    value_column = 'value'
    data = normalize_forecasting(data, value_column)
    data.value = data.value.fillna(0)  # incidence values for age 95-125 for shigella are NaN - fill to 0
    return data[BASE_COLUMNS + [value_column]]


def get_covariate_data(covariate_key, location_id):
    """Load forecast covariate data."""
    if covariate_key.measure != 'estimate':
        raise ValueError(f"The only measure that can be retrieved for covariates is estimate. You requested {measure}.")
    value_column = 'mean_value'
    if covariate_key.name == 'live_births_by_sex':  # we have to calculate
        data = _get_live_births_by_sex(location_id)
    else:
        data = get_entity_measure(covariate_key, location_id)
        data = standardize_data(data, 0)
        data = normalize_forecasting(data, value_column)
    if 'proportion' in covariate_key.name:
        logger.warning(f'Some values below zero found in {covariate_key} data.')
        data.value.loc[data.value < 0] = 0
    return data


def _get_live_births_by_sex(location_id):
    """Forecasting didn't save live_births_by_sex so have to calc from population
    and age specific fertility rate"""
    pop = get_population(location_id)
    asfr = get_entity_measure(EntityKey('covariate.age_specific_fertility_rate.estimate'), location_id)

    # calculation of live births by sex from pop & asfr from Martin Pletcher

    fertile_pop = pop[((pop.age_group_id.isin(FERTILE_AGE_GROUP_IDS)) & (pop.sex_id == 2))]
    data = asfr.merge(fertile_pop, on=['age_group_id', 'draw', 'year_id', 'sex_id', 'location_id', 'scenario'])
    data['live_births'] = data['asfr'] * data['population_agg']
    data = data.groupby(['draw', 'year_id', 'location_id'])[['live_births']].sum().reset_index()
    # normalize first because it would drop sex_id = 3 and duplicate for male and female but we need both for use in
    # vph FertilityCrudeBirthRate
    data = normalize_forecasting(data, 'mean_value', ['Both'])
    data['sex'] = 'Both'
    return data



