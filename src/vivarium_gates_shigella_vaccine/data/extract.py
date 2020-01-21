"""All functionality that touches raw files or databases.

.. admonition::

   Any modules used in this file that are not referenced in the
   ``install_requires`` section of the package ``setup.py`` should
   be imported locally.

"""
from pathlib import Path
from typing import Iterable, Union
import warnings

import pandas as pd
from vivarium_inputs import globals as vi_globals

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


def get_location_specific_life_expectancy(location_id: int):
    """Loads formatted country specific life expectancy table."""

    path = Path(__file__).resolve().parent / "life_expectancy_with_forecasted_data_12.23.19.csv"
    df = pd.read_csv(path, index_col=False)
    df = df.drop(['Unnamed: 0'], axis=1)  # Old index, why can't I avoid reading it in?
    df = df.rename(columns={'ex_inc': 'value'})
    for id_type in ['location_id', 'sex_id', 'year_id']:
        df.loc[:, id_type] = df.loc[:, id_type].astype(int)
    return df.loc[df.location_id == location_id, :]


def query(q: str, conn_def: str):
    """Wrapper around central comp's db_tools.ezfuncs.query"""
    from db_tools import ezfuncs
    warnings.filterwarnings("default", module="db_tools")
    return ezfuncs.query(q, conn_def=conn_def)


def get_draws(*args, **kwargs):
    """Wrapper around central comp's get_draws.api.get_draws"""
    from get_draws.api import get_draws
    warnings.filterwarnings("default", module="get_draws")
    return get_draws(*args, **kwargs)


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


def get_publication_ids_for_round(gbd_round_id: int) -> Iterable[int]:
    """Gets the Lancet publication ids associated with a particular gbd round."""
    round_year = {3: 2015, 4: 2016}[gbd_round_id]

    q = f"""
    SELECT publication_id
    FROM shared.publication
    WHERE gbd_round = {round_year}
    AND shared.publication.publication_type_id = 1
    """

    return query(q, 'epi').publication_id.values


def get_dismod_model_version(me_id: int,
                             publication_ids: Union[Iterable[int], None]) -> Union[int, str, None]:
    """Grabs the model version ids for dismod draws."""
    q = f"""
    SELECT modelable_entity_id, 
           model_version_id
    FROM epi.publication_model_version
    JOIN epi.model_version USING (model_version_id)
    JOIN shared.publication USING (publication_id)
    WHERE publication_id in ({','.join([str(pid) for pid in publication_ids])})
    """
    mapping = query(q, 'epi')
    version_dict = dict(mapping[['modelable_entity_id', 'model_version_id']].values)
    return version_dict.get(me_id, None)


def get_modelable_entity_draws(me_id: int, location_id: int) -> pd.DataFrame:
    """Gets draw level epi parameters for a particular dismod model, location, and gbd round."""
    publication_ids = get_publication_ids_for_round(project_globals.GBD_ROUND_ID)
    model_version = get_dismod_model_version(me_id, publication_ids)
    return get_draws(gbd_id_type='modelable_entity_id',
                     gbd_id=me_id,
                     source="epi",
                     location_id=location_id,
                     sex_id=[vi_globals.SEXES['Male'], vi_globals.SEXES['Female']],
                     age_group_id=get_age_group_id(),
                     version_id=model_version,
                     gbd_round_id=project_globals.GBD_ROUND_ID)
