import numpy as np
import pandas as pd

from .utilities import sample_beta


class ShigellaEffect:

    configuration_defaults = {
        'shigella_vaccine': {
            'immunity_duration': 720,
            'efficacy': {
                'mean': .5,
                'sd': .1
            },
            'single_dose_protected': 0.7,
            'waning_rate': 0.038
        }
    }

    def name(self):
        return 'shigella_vaccine_effect'

    def setup(self, builder):
        self.immunity_duration = builder.configuration.shigella_vaccine.immunity_duration
        self.efficacy = self.sample_efficacy(builder)
        self.single_dose_proportion_protected = builder.configuration.shigella_vaccine.single_dose_protected
        self.waning_rate = builder.configuration.shigella_vaccine.waning_rate

        self.doses_for_protection = pd.Series()

        self.randomness = builder.randomness.get_stream('shigella_doses_for_protection')

        builder.population.initializes_simulants(self.on_initialize_simulants)

    def on_initialize_simulants(self, pop_data):
        doses_for_protection = pd.Series(2, index=pop_data.index)
        single_dose_protected = self.randomness.filter_for_probability(pop_data.index,
                                                                       self.single_dose_proportion_protected)
        doses_for_protection.loc[single_dose_protected] = 1
        self.doses_for_protection.append(doses_for_protection)

    @staticmethod
    def sample_efficacy(builder):
        draw = str(builder.configuration.input_data.input_draw_number)
        stream = builder.randomness.get_stream('shigella_vaccine_efficacy')
        seed = stream.get_seed(draw)
        mu = builder.configuration.shigella_vaccine.efficacy.mean
        sigma = builder.configuration.shigella_vaccine.efficacy.sd
        return sample_beta(seed, mu, sigma)
