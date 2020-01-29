import numpy as np
import pandas as pd

from vivarium_gates_shigella_vaccine import globals as project_globals

from .utilities import sample_beta


class ShigellaEffect:

    configuration_defaults = {
        project_globals.SHIGELLA_VACCINE: {
            'scenario': project_globals.SCENARIOS.REFERENCE
        }
    }

    def name(self):
        return f'{project_globals.SHIGELLA_VACCINE}_effect'

    def setup(self, builder):
        self.clock = builder.time.clock()
        config = self.get_configuration(builder.configuration.shigellosis_vaccine.scenario)
        self.immunity_duration = pd.Timedelta(days=config['immunity_duration'])
        self.efficacy = self.sample_efficacy(builder, config)
        self.single_dose_proportion_protected = config['single_dose_protected']
        self.waning_rate = config['waning_rate']
        self.onset_delay = pd.Timedelta(days=config['onset_delay'])

        self.doses_for_protection = pd.Series()

        self.randomness = builder.randomness.get_stream('shigella_doses_for_protection')

        builder.population.initializes_simulants(self.on_initialize_simulants)
        self.population_view = builder.population.get_view(['vaccine_dose_count', 'vaccine_event_time'])

        builder.value.register_value_modifier('shigellosis.incidence_rate', self.apply_vaccine_protection)

    def on_initialize_simulants(self, pop_data):
        doses_for_protection = pd.Series(2, index=pop_data.index)
        single_dose_protected = self.randomness.filter_for_probability(pop_data.index,
                                                                       self.single_dose_proportion_protected)
        doses_for_protection.loc[single_dose_protected] = 1
        self.doses_for_protection = self.doses_for_protection.append(doses_for_protection)

    def apply_vaccine_protection(self, index, incidence_rate):
        protection = pd.Series(0, index=index)

        pop = self.population_view.get(index)
        protected = pop['vaccine_dose_count'] >= self.doses_for_protection.loc[index]
        time_since_vaccination = self.clock() - pop['vaccine_event_time']
        time_in_waning = self.clock() - (pop['vaccine_event_time'] + self.onset_delay + self.immunity_duration)

        in_delay = (protected & (pop['vaccine_dose_count'] == 1) & (time_since_vaccination < self.onset_delay))
        in_waning = (protected & (time_in_waning > pd.Timedelta(days=0)))

        in_full_protection = protected & ~in_delay & ~in_waning

        protection.loc[in_full_protection | in_waning] = self.efficacy
        protection.loc[in_waning] *= np.exp(-self.waning_rate * time_in_waning.loc[in_waning] / pd.Timedelta(days=1))
        return incidence_rate * (1 - protection)

    @staticmethod
    def sample_efficacy(builder, config):
        draw = str(builder.configuration.input_data.input_draw_number)
        stream = builder.randomness.get_stream('shigella_vaccine_efficacy')
        seed = stream.get_seed(draw)
        mu = config['mean']
        sigma = config['sd']
        return sample_beta(seed, mu, sigma)

    @staticmethod
    def get_configuration(scenario: str):
        config = {
            'immunity_duration': 720,
            'efficacy': {
                'mean': .5,
                'sd': .1
            },
            'single_dose_protected': 0.7,
            'waning_rate': 0.038,
            'onset_delay': 14,
        }
        optimistic_duration = 1460
        optimistic_efficacy = {
            'mean': 0.7,
            'sd': 0.15
        }
        optimistic_waning = 0.0134

        if scenario == project_globals.SCENARIOS.REFERENCE:
            update = {}
        elif scenario == project_globals.SCENARIOS.OPTIMISTIC:
            update = {'duration': optimistic_duration,
                      'efficacy': optimistic_efficacy,
                      'waning_rate': optimistic_waning}
        elif scenario == project_globals.SCENARIOS.SENSITIVITY_DURATION:
            update = {'immunity_duration': optimistic_duration}
        elif scenario == project_globals.SCENARIOS.SENSITIVITY_EFFICACY:
            update = {'efficacy': optimistic_efficacy}
        elif scenario == project_globals.SCENARIOS.SENSITIVITY_WANING:
            update = {'waning_rate': optimistic_waning}
        else:
            raise ValueError(f'Invalid scenario specified.  Scenario must be one of '
                             f'{list(project_globals.SCENARIOS)}')

        config.update(update)
        return config

