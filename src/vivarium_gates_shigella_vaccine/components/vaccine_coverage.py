import numpy as np
import pandas as pd
import scipy.stats


class ShigellaCoverage:

    configuration_defaults = {
        'shigella_vaccine': {
            'schedule': '6_9',
            'catchup_fraction': {
                'mean': 0.34,
                'sd': 0.21,
            }
        }
    }

    def __init__(self):
        pass

    @property
    def name(self):
        return 'shigella_vaccine_coverage'

    def setup(self, builder):
        self.coverage = {
            dose: builder.lookup.build_table(coverage, key_columns=['sex'], parameter_columns=['age', 'year'])
            for dose, coverage in self.get_dose_coverages(builder).items()
        }

        columns = [
            'vaccine_dose',
            'vaccine_event_time',
        ]
        self.population_view = builder.population.get_view(['age', 'alive'] + columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns,
                                                 requires_streams=['dose_age'])

    def on_initialize_simulants(self, pop_data):
        self.population_view.update(pd.DataFrame({
            'vaccine_dose': None,
            'vaccine_event_time': pd.NaT,
        }, index=pop_data.index))

    def get_dose_coverages(self, builder):
        schedule = builder.configuration.shigella_vaccine.schedule
        catchup_proportion = self.sample_catchup_proportion(builder)

        coverage = {}
        for month in [6, 9, 12, 15]:
            data = builder.data.load(f'covariate.shigella_vaccine_{month}mo.coverage')
            # Vaccine coverage does not vary by age or sex.
            data = data[(data.age_start == 0) & (data.sex == 'Female')].drop(columns=['age_start', 'age_end', 'sex'])
            data = data.set_index(['year_start', 'year_end']).value
            coverage[month] = data

        dose_coverage = {}
        if schedule == '6_9':
            first = coverage[6]
            second = coverage[9]
            # Since these coverages come from two different schedules,
            # We treat the probability of receiving dose 2 independently from
            # whether the person received dose 1.
            dose_coverage['first'] = first
            dose_coverage['second'] = second
            dose_coverage['catchup'] = second

        elif schedule == '9_12':
            first = coverage[9]
            second = coverage[12]
            dose_coverage['first'] = first
            dose_coverage['second'] = second / first
            dose_coverage['catchup'] = catchup_proportion

        elif schedule == '9_12_15':
            first = coverage[9]
            second = coverage[12]
            third = coverage[15]

            dose_coverage['first'] = first
            dose_coverage['second'] = second / first
            dose_coverage['catchup_1a'] = catchup_proportion

            dose_coverage['third'] = third / (second / first)
            dose_coverage['catchup_2'] = catchup_proportion
            dose_coverage['catchup_1b'] = catchup_proportion

        else:
            raise ValueError(f'Unknown vaccine schedule {schedule}.')

        for dose, cov in dose_coverage.items():
            if any(cov < 0) or any(cov > 1):
                raise ValueError

        return dose_coverage

    @staticmethod
    def sample_catchup_proportion(builder):
        draw = str(builder.configuration.input_data.input_draw_number)
        stream = builder.randomness.get_stream('shigella_vaccine_catchup_proportion')
        seed = stream.get_seed(draw)
        np.random.seed(seed)

        mu = builder.configuration.shigella_vaccine.catchup_fraction.mean
        sigma = builder.configuration.shigella_vaccine.catchup_fraction.sd
        alpha = mu * (mu * (1 - mu) / sigma ** 2 - 1)
        beta = (1 - mu) * (mu * (1 - mu) / sigma ** 2 - 1)

        return scipy.stats.beta.rvs(alpha, beta)




