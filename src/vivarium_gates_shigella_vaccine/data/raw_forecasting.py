from pathlib import Path
import warnings

import pandas as pd
from vivarium.framework.artifact import EntityKey
import xarray as xr

from vivarium_gates_shigella_vaccine import paths

# for now, use reference scenario: 0
FORECASTING_SCENARIO = 0
GBD_ROUND_ID = 4
FORECASTING_CAUSE_SET_ID = 6


def get_entity_measure(entity_key: EntityKey, location_id: int) -> pd.DataFrame:
    """Loads measure data for an entity."""
    path = paths.forecast_data_path(entity_key)
    return _load_data(path, location_id)


def get_population(location_id: int) -> pd.DataFrame:
    """Loads forecasted population data."""
    path = paths.forecast_data_path(EntityKey('population.structure'))
    return _load_data(path, location_id)


def _load_data(path: Path, location_id: int) -> pd.DataFrame:
    return (xr.open_dataset(path).sel(location_id=location_id, scenario=FORECASTING_SCENARIO)
            .to_dataframe().reset_index())


def query(q: str, conn_def: str):
    """Wrapper around central comp's db_tools.ezfuncs.query"""
    from db_tools import ezfuncs
    warnings.filterwarnings("default", module="db_tools")
    return ezfuncs.query(q, conn_def=conn_def)


def get_location_ids() -> pd.DataFrame:
    """Load the set of location ids for the GBD round."""
    from db_queries import get_location_metadata
    # FIXME: Magic location_set_id
    return get_location_metadata(location_set_id=2, gbd_round_id=GBD_ROUND_ID)[["location_id", "location_name"]]


def get_location_id(location_name):
    """Map location names to ids."""
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
