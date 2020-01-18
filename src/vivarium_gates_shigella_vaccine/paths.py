from pathlib import Path

from vivarium_gates_shigella_vaccine.globals import PROJECT_NAME

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{PROJECT_NAME}/")
MODEL_SPEC_DIR = (Path(__file__).parent / 'model_specifications').resolve()
