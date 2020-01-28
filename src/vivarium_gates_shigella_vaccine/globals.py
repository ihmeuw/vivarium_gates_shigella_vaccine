from enum import Enum

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
COVARIATE_SHIGELLA_COVERAGES = {
    6: COVARIATE_SHIGELLA_6MO,
    9: COVARIATE_SHIGELLA_9MO,
    12: COVARIATE_SHIGELLA_12MO,
    15: COVARIATE_SHIGELLA_15MO
}

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


class SCHEDULES(Enum):
    SIX_NINE = '6_9'
    NINE_TWELVE = '9_12'
    NINE_TWELVE_FIFTEEN = '9_12_15'


class DOSES(Enum):
    NONE = 'none'
    FIRST = 'first'
    SECOND = 'second'
    THIRD = 'third'
    CATCHUP = 'catchup'
    LATE_CATCHUP_MISSED_1 = 'late_catchup_missed_1'
    LATE_CATCHUP_MISSED_2 = 'late_catchup_missed_2'
    LATE_CATCHUP_MISSED_1_2 = 'late_catchup_missed_1_2'
