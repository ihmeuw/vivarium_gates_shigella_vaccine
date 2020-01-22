"""All functionality that touches raw files or databases.

.. admonition::

   Any modules used in this file that are not referenced in the
   ``install_requires`` section of the package ``setup.py`` should
   be imported locally.

"""
from pathlib import Path
from typing import Iterable, Union, List
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


def get_estimation_years(gbd_round_id: int) -> List[int]:
    """Gets the estimation years for a particular gbd round."""
    from db_queries.get_demographics import get_demographics
    warnings.filterwarnings("default", module="db_queries")

    return get_demographics('epi', gbd_round_id=gbd_round_id)['year_id']


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


def get_gbd_tool_version(publication_ids: Iterable[int], source: str) -> Union[int, None]:
    """Grabs the version id for codcorrect, burdenator, and como draws."""
    metadata_type_name = {'codcorrect': 'CoDCorrect Version',
                          'burdenator': 'Burdenator Version',
                          'como': 'Como Version'}[source]

    q = f"""
    SELECT DISTINCT val
    FROM gbd.gbd_process_version_metadata
    JOIN gbd.gbd_process_version_publication USING (gbd_process_version_id)
    JOIN gbd.gbd_process_version using (gbd_process_version_id)
    JOIN gbd.metadata_type using (metadata_type_id)
    WHERE metadata_type = '{metadata_type_name}'
    AND publication_id in ({','.join([str(pid) for pid in publication_ids])})
    AND gbd.gbd_process_version.gbd_process_id = 1
    """

    version_ids = query(q, 'gbd')
    if version_ids.empty:
        tool_version = None
    else:
        tool_version = version_ids['val'].astype('int')[0]

    return tool_version


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


def get_como_draws(entity_id: int, location_id: int, entity_type: str = 'cause') -> pd.DataFrame:
    """Gets draw level epi parameters for a particular cause, location, and gbd round."""
    # FIXME: Should submit a ticket to IT to determine if we need to specify an
    # output_version_id or a model_version_id to ensure we're getting the correct results
    # publication_ids = get_publication_ids_for_round(GBD_ROUND_ID)
    # version_id = get_gbd_tool_version(publication_ids, source='codcorrect')

    id_type = 'cause_id' if entity_type == 'cause' else 'sequela_id'
    publication_ids = get_publication_ids_for_round(project_globals.GBD_ROUND_ID)
    # NOTE: Currently this doesn't pull any thing because the tables haven't been built yet,
    # but get_draws doesn't mind and this will automatically update once the DB tables are in place - J.C 11/20
    model_version = get_gbd_tool_version(publication_ids, 'como')

    return get_draws(gbd_id_type=id_type,
                     gbd_id=entity_id,
                     source="como",
                     location_id=location_id,
                     sex_id=[vi_globals.SEXES['Male'], vi_globals.SEXES['Female']],
                     age_group_id=get_age_group_id(),
                     version_id=model_version,
                     year_id=get_estimation_years(project_globals.GBD_ROUND_ID),
                     gbd_round_id=project_globals.GBD_ROUND_ID)


def get_auxiliary_data(measure, entity_type, entity_name, location_id):
    aux_data_folder = Path("/share/costeffectiveness/auxiliary_data/GBD_2016/02_processed_data")
    root = aux_data_folder / f'{measure}/{entity_type}/{entity_name}'
    if not root.is_dir():
        raise NotADirectoryError(f'No directory found at {str(root)}')
    data_files = [p for p in root.iterdir() if p.suffix == '.hdf']

    data = []
    for f in data_files:
        data.append(pd.read_hdf(str(root / f)))
    data = pd.concat(data, ignore_index=True)

    data = data[data.location_id.isin([location_id, 1])]
    if 'year_id' in data and set(data.year_id) == {1}:
        data = data.drop('year_id', axis=1)

    return data
