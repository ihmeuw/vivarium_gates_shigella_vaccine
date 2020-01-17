import warnings

import pandas as pd
import xarray as xr

from .utilities import get_input_config, get_cache_directory

config = get_input_config()
if config.input_data.cache_data:
    from joblib import Memory
    memory = Memory(cachedir=get_cache_directory(config))
else:
    class Memory:
        def cache(self, f):
            return f

    memory = Memory()


# for now, use reference scenario: 0
FORECASTING_SCENARIO = 0
GBD_ROUND_ID = 4

# we have to get these from forecasting
FORECASTING_VERSIONS = {
    'covariate': {
        'estimate': {
            'age_specific_fertility_rate': 'asfr.nc',
            'dtp3_coverage_proportion': 'dtp3_coverage.nc',
            'measles_vaccine_coverage_proportion': 'mcv1_coverage.nc',
            'measles_vaccine_coverage_2_doses_proportion': 'mcv2_coverage.nc'
        }
    },
    'population': {
        'structure': 'population.nc'
    },
    'cause': {
        'cause_specific_mortality': {
            'all_causes': 'all_causes.nc',
            'diarrheal_diseases': 'diarrheal_diseases.nc'
        }
    },
    'etiology': {
        'incidence': {
            'shigellosis': 'shigellosis.nc'
        },
        'cause_specific_mortality': {
            'shigellosis': 'eti_diarrhea_shigellosis.nc'
        }
    }
}

FORECASTING_DATA_PATH = '/share/costeffectiveness/auxiliary_data/GBD_2016/00_external_data_backups'

FORECASTING_CAUSE_SET_ID = 6


@memory.cache
def get_entity_measure(entity, measure: str, location_id: int) -> pd.DataFrame:

    if entity['kind'] not in FORECASTING_VERSIONS:
        raise NotImplementedError(f"You requested forecasting data for a {entity['kind']} but we don't currently have "
                                  f"recorded versions for that kind of entity.")
    entity_path = {
        "covariate": _get_covariate_path,
        "cause": _get_cause_path,
        "etiology": _get_etiology_path
    }

    path = entity_path[entity['kind']](measure, entity)

    return _load_data(path, location_id)


@memory.cache
def get_population(location_id: int) -> pd.DataFrame:
    path = f"{FORECASTING_DATA_PATH}/structure/population/{FORECASTING_VERSIONS['population']['structure']}"
    return _load_data(path, location_id)


def _load_data(path: str, location_id: int) -> pd.DataFrame:
    return (xr.open_dataset(path).sel(location_id=location_id, scenario=FORECASTING_SCENARIO)
            .to_dataframe().reset_index())


def _get_covariate_path(measure: str, covariate: dict) -> str:
    covariate_versions = FORECASTING_VERSIONS['covariate'][measure]
    if covariate['name'] not in covariate_versions:
        raise NotImplementedError(f"You requested forecasting data for {covariate['name']} but we don't currently have "
                                  f"recorded versions for that covariate.")
    return f"{FORECASTING_DATA_PATH}/{measure}/covariate/{covariate['name']}/{covariate_versions[covariate['name']]}"


def _get_cause_path(measure: str, cause: dict) -> str:
    cause_versions = FORECASTING_VERSIONS['cause']
    if measure not in cause_versions:
        raise NotImplementedError(f"You requested forecasting data for {measure} for a cause but we don't currently "
                                  f"have recorded versions for that measure.")

    # Note that we only copy over all causes and diarrheal diseases csmr data to our aux data.
    if cause['name'] not in cause_versions[measure]:
        raise NotImplementedError(f"You requested forecasting data for {cause['name']} but we don't currently have"
                                  f"record versions for that cause.")

    return f"{FORECASTING_DATA_PATH}/{measure}/cause/{cause['name']}/{cause_versions[measure][cause['name']]}"


def _get_etiology_path(measure: str, etiology: dict) -> str:
    etiology_versions = FORECASTING_VERSIONS['etiology']
    if measure not in etiology_versions:
        raise NotImplementedError(f"You requested forecasting data for {measure} for an etiology but we don't currently "
                                  f"have recorded versions for that measure.")
    return f"{FORECASTING_DATA_PATH}/{measure}/etiology/{etiology['name']}/{etiology_versions[measure][etiology['name']]}"


def query(q: str, conn_def: str):
    """Wrapper around central comp's db_tools.ezfuncs.query"""
    from db_tools import ezfuncs
    warnings.filterwarnings("default", module="db_tools")
    return ezfuncs.query(q, conn_def=conn_def)


def get_location_ids() -> pd.DataFrame:
    from db_queries import get_location_metadata
    # FIXME: Magic location_set_id
    return get_location_metadata(location_set_id=2, gbd_round_id=GBD_ROUND_ID)[["location_id", "location_name"]]


def get_location_id(location_name):
    return {r.location_name: r.location_id for _, r in get_location_ids().iterrows()}[location_name]


def get_age_group_id() -> list:
    """Get the age group ids associated with a gbd round."""
    from db_queries.get_demographics import get_demographics
    warnings.filterwarnings("default", module="db_queries")

    # TODO: There are several team versions of demographics. We've always used
    # the 'epi' version. Maybe for actual reasons?  I don't know why though.
    # -J.C. 10/28/18
    team = 'epi'
    return get_demographics(team, GBD_ROUND_ID)['age_group_id']


def get_age_bins() -> pd.DataFrame:
    """Get the age group bin edges, ids, and names associated with a gbd round."""
    q = f"""
    SELECT age_group_id,
           age_group_years_start,
           age_group_years_end,
           age_group_name
    FROM age_group
    WHERE age_group_id IN ({','.join([str(a) for a in get_age_group_id()])})
    """
    return query(q, 'shared')
