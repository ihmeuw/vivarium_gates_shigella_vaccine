"""All functionality that touches raw files or databases.

.. admonition::

   Any modules used in this file that are not referenced in the
   ``install_requires`` section of the package ``setup.py`` should
   be imported locally.

"""
from pathlib import Path
import warnings

import pandas as pd

from vivarium_gates_shigella_vaccine import globals as project_globals


def load_forecast_from_xarray(path: Path, location_id: int) -> pd.DataFrame:
    """Loads forecast data stored in xarray datasets.

    Parameters
    ----------
    path
        The absolute path to the xarray dataset.
    location_id
        The location id to slice out of the dataset.

    Returns
    -------
        The requested dataset as a dataframe. The format is stable, but
        may vary based on the provided path.

    """
    import xarray as xr
    return (xr
            .open_dataset(path)
            .sel(location_id=location_id, scenario=project_globals.FORECASTING_SCENARIO)
            .to_dataframe()
            .reset_index())


def query(q: str, conn_def: str):
    """Wrapper around central comp's db_tools.ezfuncs.query"""
    from db_tools import ezfuncs
    warnings.filterwarnings("default", module="db_tools")
    return ezfuncs.query(q, conn_def=conn_def)


def get_location_ids() -> pd.DataFrame:
    """Load the set of location ids for the GBD round."""
    from db_queries import get_location_metadata
    # FIXME: Magic location_set_id
    location_data = get_location_metadata(location_set_id=2, gbd_round_id=project_globals.GBD_ROUND_ID)
    return location_data.loc[:, ["location_id", "location_name"]]


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
    return get_demographics(team, project_globals.GBD_ROUND_ID)['age_group_id']


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
