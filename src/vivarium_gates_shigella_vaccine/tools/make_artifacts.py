"""Main application functions for building artifacts.

.. admonition::

   Logging in this module should typically be done at the ``info`` level.
   Use your best judgement.

"""
from pathlib import Path
import shutil
import sys
import time

import click
from loguru import logger

from vivarium_gates_shigella_vaccine import globals as project_globals
from vivarium_gates_shigella_vaccine.data import builder
from vivarium_gates_shigella_vaccine.utilites import sanitize_location

from vivarium_gates_shigella_vaccine.tools.app_logging import add_logging_sink


def build_artifacts(location: str, output_dir: str, append: bool, verbose: int):
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

        build_all_artifacts(output_dir, verbose)

    else:
        raise ValueError(f'Location must be one of {project_globals.LOCATIONS} or the string "all". '
                         f'You specified {location}.')


def build_all_artifacts(output_dir, verbose):
    from vivarium_cluster_tools.psimulate.utilities import get_drmaa
    drmaa = get_drmaa()

    jobs = {}
    with drmaa.Session() as session:
        for location in project_globals.LOCATIONS:
            path = output_dir / f'{sanitize_location(location)}.hdf'

            job_template = session.createJobTemplate()
            job_template.remoteCommand = shutil.which("python")
            job_template.args = [__file__, str(path), location]
            job_template.nativeSpecification = (f'-V -b y -P {project_globals.CLUSTER_PROJECT} -q all.q '
                                                f'-l fmem=3G -l fthread=1 -l h_rt=3:00:00 '
                                                f'-N {sanitize_location(location)}_artifact')
            jobs[location] = (session.runJob(job_template), drmaa.JobState.UNDETERMINED)
            logger.info(f'Submitted job {jobs[location]} to build artifact for {location}.')
            session.deleteJobTemplate(job_template)

        decodestatus = {drmaa.JobState.UNDETERMINED: 'process status cannot be determined',
                        drmaa.JobState.QUEUED_ACTIVE: 'job is queued and active',
                        drmaa.JobState.SYSTEM_ON_HOLD: 'job is queued and in system hold',
                        drmaa.JobState.USER_ON_HOLD: 'job is queued and in user hold',
                        drmaa.JobState.USER_SYSTEM_ON_HOLD: 'job is queued and in user and system hold',
                        drmaa.JobState.RUNNING: 'job is running',
                        drmaa.JobState.SYSTEM_SUSPENDED: 'job is system suspended',
                        drmaa.JobState.USER_SUSPENDED: 'job is user suspended',
                        drmaa.JobState.DONE: 'job finished normally',
                        drmaa.JobState.FAILED: 'job finished, but failed'}

        if verbose:
            logger.info('Entering monitoring loop.')
            jobs_running = any([job[1] not in [drmaa.JobState.DONE, drmaa.JobState.FAILED] for job in jobs.values()])

            while jobs_running:
                for location, (job_id, status) in jobs.items():
                    jobs[location] = (job_id, decodestatus[session.jobStatus(job_id)])
                    logger.info(f'{location}: {jobs[location]}')
                    jobs_running = any(
                        [job[1] not in [drmaa.JobState.DONE, drmaa.JobState.FAILED] for job in jobs.values()])
                time.sleep(10)
                logger.info('Checking status again')
                logger.info('---------------------')

    logger.info('Done')


def build_single_location_artifact(path, location, log_to_file=False):
    path = Path(path)
    if log_to_file:
        log_file = path.parent / 'logs' / f'{sanitize_location(location)}.log'
        if log_file.exists():
            log_file.unlink()
        add_logging_sink(log_file, verbose=2)

    logger.info(f'Building artifact for {location} at {str(path)}.')
    artifact = builder.open_artifact(path, location)
    logger.info(f'Loading and writing demographic data.')
    builder.load_and_write_demographic_data(artifact, location)


if __name__ == "__main__":
    artifact_path = sys.argv[1]
    artifact_location = sys.argv[2]
    build_single_location_artifact(artifact_path, artifact_location, log_to_file=True)



