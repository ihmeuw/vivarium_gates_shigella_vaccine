import itertools
from typing import NamedTuple

PROJECT_NAME = 'vivarium_gates_shigella_vaccine'
CLUSTER_PROJECT = 'proj_cost_effect_diarrhea'

FORECASTING_SCENARIO = 0  # Reference scenario
GBD_ROUND_ID = 4  # GBD 2016
MIN_YEAR = 2017
MAX_YEAR = 2040
NUM_DRAWS = 1000


LOCATIONS = [
    'Bangladesh',
    'Cameroon',
    'Central African Republic',
    'Chad',
    "Cote d'Ivoire",
    'Democratic Republic of the Congo',
    'Ethiopia',
    'Ghana',
    'India',
    'Indonesia',
    'Kenya',
    'Madagascar',
    'Mozambique',
    'Niger',
    'Nigeria',
    'Pakistan',
    'Somalia',
    'South Sudan',
    'Tanzania',
    'Uganda',
]


# Data keys

METADATA_LOCATIONS = 'metadata.locations'

POPULATION_STRUCTURE = 'population.structure'
POPULATION_AGE_BINS = 'population.age_bins'
POPULATION_DEMOGRAPHY = 'population.demographic_dimensions'
POPULATION_TMRLE = 'population.theoretical_minimum_risk_life_expectancy'
POPULATION_LSLE = 'population.location_specific_life_expectancy'

COVARIATE_LIVE_BIRTHS = 'covariate.live_births_by_year.estimate'
COVARIATE_DTP3 = 'covariate.dtp3_coverage_proportion.estimate'
COVARIATE_MEASLES1 = 'covariate.measles_vaccine_coverage_proportion.estimate'
COVARIATE_MEASLES2 = 'covariate.measles_vaccine_coverage_2_doses_proportion.estimate'
COVARIATE_SHIGELLA_6MO = 'covariate.shigella_vaccine_6mo.coverage'
COVARIATE_SHIGELLA_9MO = 'covariate.shigella_vaccine_9mo.coverage'
COVARIATE_SHIGELLA_12MO = 'covariate.shigella_vaccine_12mo.coverage'
COVARIATE_SHIGELLA_15MO = 'covariate.shigella_vaccine_15mo.coverage'
COVARIATE_SHIGELLA_COVERAGES = [
    COVARIATE_SHIGELLA_6MO,
    COVARIATE_SHIGELLA_9MO,
    COVARIATE_SHIGELLA_12MO,
    COVARIATE_SHIGELLA_15MO
]


ALL_CAUSE_CSMR = 'cause.all_causes.cause_specific_mortality_rate'

SHIGELLA_CSMR = 'cause.shigellosis.cause_specific_mortality_rate'
SHIGELLA_DISABILITY_WEIGHT = 'cause.shigellosis.disability_weight'
SHIGELLA_INCIDENCE_RATE = 'cause.shigellosis.incidence_rate'
SHIGELLA_REMISSION_RATE = 'cause.shigellosis.remission_rate'
SHIGELLA_PREVALENCE = 'cause.shigellosis.prevalence'
SHIGELLA_EMR = 'cause.shigellosis.excess_mortality_rate'
SHIGELLA_RESTRICTIONS = 'cause.shigellosis.restrictions'

# Other string constants

SHIGELLA_VACCINE = 'shigellosis_vaccine'


class __SCENARIOS(NamedTuple):
    BASELINE: str = 'baseline'
    REFERENCE: str = 'reference'
    OPTIMISTIC: str = 'optimistic'
    SENSITIVITY_DURATION: str = 'sensitivity_duration'
    SENSITIVITY_EFFICACY: str = 'sensitivity_efficacy'
    SENSITIVITY_WANING: str = 'sensitivity_waning'


SCENARIOS = __SCENARIOS()


class __SCHEDULES(NamedTuple):
    NONE: str = 'none'
    SIX_NINE: str = '6_9'
    NINE_TWELVE: str = '9_12'
    NINE_TWELVE_FIFTEEN: str = '9_12_15'


SCHEDULES = __SCHEDULES()


class __DOSES(NamedTuple):
    NONE: str = 'none'
    FIRST: str = 'first'
    SECOND: str = 'second'
    THIRD: str = 'third'
    CATCHUP: str = 'catchup'
    LATE_CATCHUP_MISSED_1: str = 'late_catchup_missed_1'
    LATE_CATCHUP_MISSED_2: str = 'late_catchup_missed_2'
    LATE_CATCHUP_MISSED_1_2: str = 'late_catchup_missed_1_2'


DOSES = __DOSES()
VACCINE_DOSES = DOSES[1:]

# Result column constants

TOTAL_POP_COLUMN = 'total_population'
TOTAL_YLLS_COLUMN = 'years_of_life_lost'
TOTAL_YLDS_COLUMN = 'years_lived_with_disability'
RANDOM_SEED_COLUMN = 'random_seed'
INPUT_DRAW_COLUMN = 'input_draw'


STANDARD_COLUMNS = {'total_population': TOTAL_POP_COLUMN,
                    'total_ylls': TOTAL_YLLS_COLUMN,
                    'total_ylds': TOTAL_YLDS_COLUMN,
                    'random_seed': RANDOM_SEED_COLUMN,
                    'input_draw': INPUT_DRAW_COLUMN}

TOTAL_POP_COLUMN_TEMPLATE = 'total_population_{POP_STATE}'
PERSON_TIME_COLUMN_TEMPLATE = 'person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
YLDS_COLUMN_TEMPLATE = 'ylds_due_to_{CAUSE_OF_DISABILITY_STATE}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
DEATH_COLUMN_TEMPLATE = 'deaths_due_to_{CAUSE_OF_DEATH_STATE}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
YLLS_COLUMN_TEMPLATE = 'ylls_due_to_{CAUSE_OF_DEATH_STATE}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
LOCATION_SPECIFIC_YLLS_COLUMN_TEMPLATE = 'location_specific_ylls_due_to_{CAUSE_OF_DEATH_STATE}_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'

DISEASE_EVENTS_COLUMN_TEMPLATE = '{DISEASE_STATE}_counts_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
PREVALENT_CASES_COLUMN_TEMPLATE = '{DISEASE_STATE}_prevalent_cases_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
SUSCEPTIBLE_PERSON_TIME_COLUMN_TEMPLATE = '{DISEASE_STATE}_susceptible_person_time_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'
VACCINE_COUNT_COLUMN_TEMPLATE = 'shigellosis_vaccine_{VACCINE_DOSE}_dose_count_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}'


COLUMN_TEMPLATES = {'total_population': TOTAL_POP_COLUMN_TEMPLATE,
                    'person_time': PERSON_TIME_COLUMN_TEMPLATE,
                    'ylds': YLDS_COLUMN_TEMPLATE,
                    'deaths': DEATH_COLUMN_TEMPLATE,
                    'ylls': YLLS_COLUMN_TEMPLATE,
                    'location_specific_ylls': LOCATION_SPECIFIC_YLLS_COLUMN_TEMPLATE,
                    'disease_events': DISEASE_EVENTS_COLUMN_TEMPLATE,
                    'prevalent_cases': PREVALENT_CASES_COLUMN_TEMPLATE,
                    'susceptible_person_time': SUSCEPTIBLE_PERSON_TIME_COLUMN_TEMPLATE,
                    'vaccine_counts': VACCINE_COUNT_COLUMN_TEMPLATE}

SEXES = ['male', 'female']
AGE_GROUPS = ['early_neonatal', 'late_neonatal', 'post_neonatal', '1_to_4']
YEARS = list(range(2025, 2041))
POP_STATES = ['tracked', 'untracked', 'living', 'dead']
DISEASE_STATES = ['shigellosis']
CAUSE_OF_DEATH_STATES = ['shigellosis', 'other_causes']
CAUSE_OF_DISABILITY_STATES = DISEASE_STATES[:]


TEMPLATE_FIELD_MAP = {'SEX': SEXES,
                      'AGE_GROUP': AGE_GROUPS,
                      'YEAR': YEARS,
                      'POP_STATE': POP_STATES,
                      'CAUSE_OF_DISABILITY_STATE': CAUSE_OF_DISABILITY_STATES,
                      'CAUSE_OF_DEATH_STATE': CAUSE_OF_DEATH_STATES,
                      'DISEASE_STATE': DISEASE_STATES}


def RESULT_COLUMNS(kind='all'):
    if kind not in COLUMN_TEMPLATES and kind != 'all':
        raise ValueError(f'Unknown result column type {kind}')
    columns = []
    if kind == 'all':
        for k in COLUMN_TEMPLATES:
            columns += RESULT_COLUMNS(k)
        columns = list(STANDARD_COLUMNS.values()) + columns
    else:
        template = COLUMN_TEMPLATES[kind]
        filtered_field_map = {field: values for field, values in TEMPLATE_FIELD_MAP.items() if field in template}
        fields, value_groups = filtered_field_map.keys(), itertools.product(*filtered_field_map.values())
        for value_group in value_groups:
            columns.append(template.format(**{field: value for field, value in zip(fields, value_group)}))
    return columns
