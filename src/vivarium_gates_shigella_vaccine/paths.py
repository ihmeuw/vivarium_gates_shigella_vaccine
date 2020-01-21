from pathlib import Path

from vivarium.framework.artifact import EntityKey

import vivarium_gates_shigella_vaccine.globals as project_globals

AUXILIARY_DATA_ROOT = Path('/share/costeffectiveness/auxiliary_data/GBD_2016/00_external_data_backups')
ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{project_globals.PROJECT_NAME}/")
MODEL_SPEC_DIR = (Path(__file__).parent / 'model_specifications').resolve()


def forecast_data_path(entity_key: EntityKey) -> Path:
    path_map = {
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
            'cause_specific_mortality_rate': {
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
    try:
        if entity_key.name:
            file_name = path_map[entity_key.type][entity_key.measure][entity_key.name]
            path = AUXILIARY_DATA_ROOT / entity_key.measure / entity_key.type / entity_key.name / file_name
        else:
            file_name = path_map[entity_key.type][entity_key.measure]
            path = AUXILIARY_DATA_ROOT / entity_key.measure / entity_key.type / file_name
        return path
    except KeyError:
        raise FileNotFoundError(f'No forecasting data available for {entity_key}.')
