import numpy as np
import pandas as pd

from .utilities import sample_beta


class ShigellaEffect:

    configuration_defaults = {
        'shigellosis_vaccine': {
            'immunity_duration': 720,
            'efficacy': {
                'mean': .5,
                'sd': .1
            },
            'single_dose_protected': 0.7,
            'waning_rate': 0.038,
            'onset_delay': 14,
        }
    }

    def name(self):
        return 'shigella_vaccine_effect'

    def setup(self, builder):
        self.clock = builder.time.clock()
        self.immunity_duration = pd.Timedelta(days=builder.configuration.shigellosis_vaccine.immunity_duration)
        self.efficacy = self.sample_efficacy(builder)
        self.single_dose_proportion_protected = builder.configuration.shigellosis_vaccine.single_dose_protected
        self.waning_rate = builder.configuration.shigellosis_vaccine.waning_rate
        self.onset_delay = pd.Timedelta(days=builder.configuration.shigellosis_vaccine.onset_delay)

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
    def sample_efficacy(builder):
        draw = str(builder.configuration.input_data.input_draw_number)
        stream = builder.randomness.get_stream('shigella_vaccine_efficacy')
        seed = stream.get_seed(draw)
        mu = builder.configuration.shigellosis_vaccine.efficacy.mean
        sigma = builder.configuration.shigellosis_vaccine.efficacy.sd
        return sample_beta(seed, mu, sigma)
