"""Main application functions for building artifacts.

.. admonition::

   Logging in this module should typically be done at the ``info`` level.
   Use your best judgement.

"""
from pathlib import Path

import click
from loguru import logger

from vivarium_gates_shigella_vaccine import globals as project_globals
from vivarium_gates_shigella_vaccine.data import builder
from vivarium_gates_shigella_vaccine.utilites import sanitize_location


def build_artifacts(location: str, output_dir: str, append: bool):
    output_dir = Path(output_dir)

    if location in project_globals.LOCATIONS:
        path = Path(output_dir) / f'{sanitize_location(location)}.hdf'

        if path.exists() and not append:
            click.confirm(f"Existing artifact found for {location}. Do you want to delete and rebuild?",
                          abort=True)
            logger.info(f'Deleting artifact at {str(path)}.')
            path.unlink()

        build_single_location_artifact(path, location)

    elif location == 'all':
        # FIXME: could be more careful
        existing_artifacts = set([item.stem for item in output_dir.iterdir()
                                  if item.is_file() and item.suffix == '.hdf'])
        locations = set([sanitize_location(loc) for loc in project_globals.LOCATIONS])
        existing = locations.intersection(existing_artifacts)

        if existing and not append:
            click.confirm(f'Existing artifacts found for {existing}. Do you want to delete and rebuild?',
                          abort=True)
            for l in existing:
                path = output_dir / f'{l}.hdf'
                logger.info(f'Deleting artifact at {str(path)}.')
                path.unlink()

        build_all_artifacts(output_dir)

    else:
        raise ValueError(f'Location must be one of {project_globals.LOCATIONS} or the string "all". '
                         f'You specified {location}.')


def build_all_artifacts(output_dir):
    pass


def build_single_location_artifact(path, location):
    logger.info(f'Building artifact for {location} at {str(path)}.')
    artifact = builder.open_artifact(path, location)
    logger.info(f'Loading and writing demographic data.')
    builder.load_and_write_demographic_data(artifact, location)
