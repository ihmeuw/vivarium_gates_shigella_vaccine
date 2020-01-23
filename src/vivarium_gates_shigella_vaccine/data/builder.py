"""Modularized functions for building project data artifacts.

.. admonition::

   Logging in this module should be done at the ``debug`` level.

"""
from pathlib import Path

from loguru import logger
from vivarium.framework.artifact import Artifact, EntityKey, get_location_term

from vivarium_gates_shigella_vaccine.data import loader


def open_artifact(output_path: Path, location: str) -> Artifact:
    """Creates or opens an artifact at the output path.

    Parameters
    ----------
    output_path
        Fully resolved path to the artifact file.
    location
        Proper GBD location name represented by the artifact.

    Returns
    -------
        A new artifact.

    """
    if not output_path.exists():
        logger.debug(f"Creating artifact at {str(output_path)}.")
    else:
        logger.debug(f"Opening artifact at {str(output_path)} for appending.")

    artifact = Artifact(output_path, filter_terms=[get_location_term(location)])

    key = EntityKey('metadata.locations')
    if key not in artifact:
        artifact.write(key, [location])

    return artifact


def load_and_write(artifact: Artifact, key: EntityKey, location: str):
    """Loads data and writes it to the artifact if not already present.

    Parameters
    ----------
    artifact
        The artifact to write to.
    key
        The entity key associated with the data to write.
    location
        The location associated with the data to load and the artifact to
        write to.

    Notes
    -----
        This function supports simple remapping of keys. Complex tailoring
        of input data should not use this function.  To support appending,
        they should check for the write key in the artifact manually,
        then load any relevant data and transform it as necessary to
        write out using ``artifact.write``.

    """
    if key in artifact:
        logger.debug(f'Data for {key} already in artifact.  Skipping...')
    else:
        logger.debug(f'Loading data for {key} for location {location}.')
        data = loader.get_data(key, location)
        logger.debug(f'Writing data for {key} to artifact.')
        artifact.write(key, data)
    return artifact.load(key)


def load_and_write_demographic_data(artifact: Artifact, location: str):
    keys = [
        EntityKey('population.structure'),
        EntityKey('population.age_bins'),
        EntityKey('population.demographic_dimensions'),
        EntityKey('population.theoretical_minimum_risk_life_expectancy'),
        EntityKey('population.location_specific_life_expectancy'),
        EntityKey('cause.all_causes.cause_specific_mortality_rate'),
        EntityKey('covariate.live_births_by_year.estimate'),
    ]

    logger.debug('Loading and writing demographic data.')
    for key in keys:
        load_and_write(artifact, key, location)


def load_and_write_cause_data(artifact: Artifact, location: str):
    logger.debug('Loading and writing shigella data.')

    key = EntityKey('cause.shigellosis.cause_specific_mortality_rate')
    csmr = load_and_write(artifact, key, location)
    key = EntityKey('cause.shigellosis.disability_weight')
    load_and_write(artifact, key, location)

    key = EntityKey('cause.shigellosis.incidence_rate')
    incidence = load_and_write(artifact, key, location)
    key = EntityKey('cause.shigellosis.remission_rate')
    remission = load_and_write(artifact, key, location)

    key = EntityKey('cause.shigellosis.prevalence')
    if key in artifact:
        logger.debug(f'Data for {key} already in artifact.  Skipping...')
    else:
        logger.debug(f'Loading data for {key} for location {location}.')
        # Approximate prevalence as incidence * duration
        data = incidence / remission
        logger.debug(f'Writing data for {key} to artifact.')
        artifact.write(key, data)
    prevalence = artifact.load(key)

    key = EntityKey('cause.shigellosis.excess_mortality_rate')
    if key in artifact:
        logger.debug(f'Data for {key} already in artifact.  Skipping...')
    else:
        logger.debug(f'Loading data for {key} for location {location}.')
        # Approximate prevalence as incidence * duration
        data = (csmr / prevalence).fillna(0)
        logger.debug(f'Writing data for {key} to artifact.')
        artifact.write(key, data)


def load_and_write_vaccine_data(artifact: Artifact, location: str):
    keys = [
        # EntityKey('covariate.shigella_vaccine_6mo.coverage')
        # EntityKey('covariate.shigella_vaccine_9mo.coverage')
        # EntityKey('covariate.shigella_vaccine_12mo.coverage')
        # EntityKey('covariate.shigella_vaccine_18mo.coverage')
    ]
    pass




