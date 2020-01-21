from pathlib import Path
import warnings

import pandas as pd
import xarray as xr

from vivarium_gates_shigella_vaccine import globals as project_globals


def load_forecast_from_xarray(path: Path, location_id: int):
    return (xr.open_dataset(path).sel(location_id=location_id, scenario=project_globals.FORECASTING_SCENARIO)
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
    return get_location_metadata(location_set_id=2,
                                 gbd_round_id=project_globals.GBD_ROUND_ID)[["location_id", "location_name"]]


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


def normalize_for_simulation(df):
    """
    Parameters
    ----------
    df : DataFrame
        dataframe to change
    Returns
    -------
    Returns a df with column year_id changed to year, and year_start and year_end
    created as bin ends around year_id with year_start set to year_id;
    sex_id changed to sex, and sex values changed from 1 and 2 to Male and Female
    Notes
    -----
    Used by -- load_data_from_cache
    Assumptions -- None
    Questions -- None
    Unit test in place? -- Yes
    """
    if "sex_id" in df:
        if set(df["sex_id"]) == {3}:
            df_m = df.copy()
            df_f = df.copy()
            df_m['sex'] = 'Male'
            df_f['sex'] = 'Female'
            df = pd.concat([df_m, df_f], ignore_index=True)
        else:
            df["sex"] = df.sex_id.map({1: "Male", 2: "Female", 3: "Both"}).astype(
                pd.api.types.CategoricalDtype(categories=["Male", "Female", "Both"], ordered=False))

        df = df.drop("sex_id", axis=1)

    if "year_id" in df:
        # FIXME: use central comp interpolation tools
        if 2006 in df.year_id.unique() and 2007 not in df.year_id.unique():
            df = df.loc[(df.year_id != 2006)]

        df = df.rename(columns={"year_id": "year_start"})
        idx = df.index

        mapping = df[['year_start']].drop_duplicates().sort_values(by="year_start")
        mapping['year_end'] = mapping['year_start'].shift(-1).fillna(mapping.year_start.max()+1).astype(int)

        df = df.set_index("year_start", drop=False)
        mapping = mapping.set_index("year_start", drop=False)

        df[["year_start", "year_end"]] = mapping[["year_start", "year_end"]]

        df = df.set_index(idx)

    return df


def get_age_group_bins_from_age_group_id(df):
    """Creates "age_group_start" and "age_group_end" columns from the "age_group_id" column
    Parameters
    ----------
    df: df for which you want an age column that has an age_group_id column
    Returns
    -------
    df with "age_group_start" and "age_group_end" columns
    """
    if df.empty:
        df['age_group_start'] = 0
        df['age_group_end'] = 0
        return df

    df = df.copy()
    idx = df.index
    mapping = get_age_bins()
    mapping = mapping.set_index('age_group_id')

    df = df.set_index('age_group_id')
    df[['age_group_start', 'age_group_end']] = mapping[['age_group_years_start', 'age_group_years_end']]

    df = df.set_index(idx)

    return df
