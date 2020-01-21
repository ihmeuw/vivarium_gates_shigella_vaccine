"""Data validation."""
from loguru import logger
import numpy as np

from vivarium_gates_shigella_vaccine import globals as project_globals

from .extract import get_age_bins


class DataMissingError(Exception):
    """Exception raised when data has unhandled missing entries."""
    pass


def validate_data(entity_key, data):
    """Check data quality."""
    validate_demographic_block(entity_key, data)
    validate_value_range(entity_key, data)


def validate_demographic_block(entity_key, data):
    """Check index quality."""
    ages = get_age_bins()
    age_start = ages['age_group_years_start']
    year_start = range(2017, project_globals.MAX_YEAR + 1)
    if 'live_births_by_sex' in entity_key:
        sexes = ['Both']
    elif 'population.structure' in entity_key:
        sexes = ['Male', 'Female', 'Both']
    else:
        sexes = ['Male', 'Female']

    values, names = 1, []
    if 'age_group_start' in data:
        values *= len(age_start)
        if set(data.age_group_start) != set(age_start):
            raise DataMissingError(f'Data for {entity_key} does not have the correct set of ages.')
        names += ['age_group_start']
    if 'year_start' in data:
        values *= len(year_start)
        if set(data.year_start) != set(year_start):
            raise DataMissingError(f'Data for {entity_key} does not have the correct set of years.')
        names += ['year_start']
    if 'sex' in data:
        values *= len(sexes)
        if set(data.sex) != set(sexes):
            raise DataMissingError(f'Data for {entity_key} does not have the correct set of sexes.')
        names += ['sex']
    if 'draw' in data:
        values *= project_globals.NUM_DRAWS
        if set(data.draw) != set(range(project_globals.NUM_DRAWS)):
            raise DataMissingError(f'Data for {entity_key} does not have the correct set of draws.')
        names += ['draw']

    demographic_block = data[names]
    if demographic_block.shape[0] != values:
        raise DataMissingError(f'Data for {entity_key} does not have a correctly-sized demographic block.')


def validate_value_range(entity_key, data):
    """Validates that values of particular types are in a reasonable range."""
    maxes = {
        'proportion': 1,
        'population': 100_000_000,
        'incidence': 50,
        'cause_specific_mortality': 6,
    }
    if 'value' in data:
        if 'proportion' in entity_key:
            max_value = maxes['proportion']
        elif 'population.structure' in entity_key:
            max_value = maxes['population']
        elif 'cause_specific_mortality' in entity_key:
            max_value = maxes['cause_specific_mortality']
        elif 'incidence' in entity_key:
            max_value = maxes['incidence']
        else:
            raise NotImplementedError(f'No max value on record for {entity_key}.')
        # for shigella model, all we care about is 2025-2040 so restricting to that range
        data = data[data.year_start >= 2025]
        # all supported entity/measures as of 3/22/19 should be > 0
        if np.any(data.value < 0):
            raise DataMissingError(f'Data for {entity_key} does not contain all values above 0.')

        if np.any(data.value > max_value):
            logger.debug(f'Data for {entity_key} contains values above maximum {max_value}.')

        if np.any(data.value.isna()) or np.any(np.isinf(data.value.values)):
            raise DataMissingError(f'Data for {entity_key} contains NaN or Inf values.')
