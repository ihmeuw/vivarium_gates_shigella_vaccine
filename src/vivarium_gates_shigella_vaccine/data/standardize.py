"""All generic functionality for standardizing raw data."""
import math

import pandas as pd

from vivarium_gates_shigella_vaccine import globals as project_globals

from .extract import get_age_bins, get_age_group_id


FERTILE_AGE_GROUP_IDS = list(range(7, 15 + 1))  # need for calc live births by sex


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


def normalize_forecasting(data: pd.DataFrame, value_column='value', sexes=('Male', 'Female')) -> pd.DataFrame:
    """Standardize index column names and do some filtering."""
    assert not data.empty

    data = normalize_for_simulation(rename_value_columns(data, value_column))

    if "age_group_id" in data:
        if (data["age_group_id"] == 22).all():  # drop age if data is for all ages
            data = data.drop("age_group_id", "columns")
        else:
            # drop any age group ids that don't map to bins we use from gbd (e.g., 1 which is <5 or 158 which is <20)
            data = data[data.age_group_id.isin(get_age_bins().age_group_id)]
            data = get_age_group_bins_from_age_group_id(data)

    # not filtering on year as in vivarium_inputs.data_artifact.utilities.normalize b/c will drop future data
    # only keeping data out to 2040 for consistency
    if 'year_start' in data:
        data = data[(data.year_start >= 2017) & (data.year_start <= project_globals.MAX_YEAR)]

    if 'scenario' in data:
        data = data.drop("scenario", "columns")

    if 'sex' in data:
        data = data[data.sex.isin(sexes)]

    # make sure there are at least NUM_DRAWS draws
    return replicate_data(data)


def standardize_data(data: pd.DataFrame, fill_value: int) -> pd.DataFrame:
    """Standardize data shape and clean up nulls."""
    # because forecasting data is already in long format, we need a custom standardize method

    # age_groups that we expect to exist for each entity
    whole_age_groups = get_age_group_id() if set(data.age_group_id) != {22} else [22]
    sex_id = data.sex_id.unique()
    year_id = data.year_id.unique()
    location_id = data.location_id.unique()

    index_cols = ['year_id', 'location_id', 'sex_id', 'age_group_id']
    expected_list = [year_id, location_id, sex_id, whole_age_groups]

    if 'draw' in data:
        index_cols += ['draw']
        expected_list += [data.draw.unique()]

    value_cols = {c for c in data.dropna(axis=1).columns if c not in index_cols}
    data = data.set_index(index_cols)

    # expected indexes to be in the data
    expected = pd.MultiIndex.from_product(expected_list, names=index_cols)

    new_data = data.copy()
    missing = expected.difference(data.index)

    # assign dtype=float to prevent the artifact error with mixed dtypes
    to_add = pd.DataFrame({column: fill_value for column in value_cols}, index=missing, dtype=float)

    new_data = new_data.append(to_add).sort_index()

    return new_data.reset_index()


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


def rename_value_columns(data: pd.DataFrame, value_column: str = 'value') -> pd.DataFrame:
    """Standardize the value column name."""
    if not ('value' in data or 'mean_value' in data):
        # we need to rename the value column to match vivarium_inputs convention
        col = set(data.columns) - {'year_id', 'sex_id', 'age_group_id', 'draw', 'scenario', 'location_id'}
        if len(col) > 1:
            raise ValueError(f"You have multiple value columns in your data.")
        data = data.rename(columns={col.pop(): value_column})
    return data


def replicate_data(data):
    """If data has fewer than NUM_DRAWS draws, duplicate to have the full set.
    Assumes draws in data are sequential and start at 0
    """
    if 'draw' not in data:  # for things with only 1 draw, draw isn't included as a col
        data['draw'] = 0

    full_data = data.copy()
    og_draws = data.draw.unique()
    n_draws = len(og_draws)

    if n_draws < project_globals.NUM_DRAWS:

        for i in range(1, math.ceil(project_globals.NUM_DRAWS/n_draws)):

            max_draw = max(og_draws)
            if i == math.ceil(project_globals.NUM_DRAWS/n_draws)-1 and project_globals.NUM_DRAWS % n_draws > 0:
                max_draw = project_globals.NUM_DRAWS % n_draws - 1

            draw_map = pd.Series(range(i*n_draws, i*n_draws + n_draws), index=og_draws)

            new_data = data[data.draw <= max_draw].copy()
            new_data['draw'] = new_data.draw.apply(lambda x: draw_map[x])

            full_data = full_data.append(new_data)

    return full_data
