from types import SimpleNamespace


PROJECT_NAME = 'vivarium_gates_shigella_vaccine'
CLUSTER_PROJECT = 'proj_cost_effect_diarrhea'

FORECASTING_SCENARIO = 0  # Reference scenario
GBD_ROUND_ID = 4  # GBD 2016
MIN_YEAR = 2017
MAX_YEAR = 2040
NUM_DRAWS = 1000

DIARRHEAL_DISEASES = SimpleNamespace(
    dismod_id=1181,
    restrictions=SimpleNamespace(
        male_only=False,
        female_only=False,
        yll_only=False,
        yld_only=False,
        yll_age_group_id_start=2,
        yll_age_group_id_end=235,
        yld_age_group_id_start=2,
        yld_age_group_id_end=235,
    )
)

LOCATIONS = [
    'Bangladesh',
    'Cameroon',
    'Central African Republic',
    'Chad',
    "Cote d'Ivoire",
    'Democratic Republic of the Congo',
    'Ethiopia',
    'Ghana',
    'India',
    'Indonesia',
    'Kenya',
    'Madagascar',
    'Mozambique',
    'Niger',
    'Nigeria',
    'Pakistan',
    'Somalia',
    'South Sudan',
    'Tanzania',
    'Uganda',
]

