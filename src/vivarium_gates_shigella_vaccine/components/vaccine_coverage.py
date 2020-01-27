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
        self.dose_age_ranges = self.get_age_ranges(builder)

        self.dose_ages = pd.DataFrame(columns=self.dose_age_ranges.keys())

        self.dose_age_randomness = builder.randomness.get_stream('shigella_vaccine_dose_age')

        columns = [
            'vaccine_dose',
            'vaccine_event_time',
        ]
        self.population_view = builder.population.get_view(['age', 'alive'] + columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns,
                                                 requires_streams=['dose_age'])

    def on_initialize_simulants(self, pop_data):
        self.dose_ages.append(self.sample_ages(pop_data.index))

        self.population_view.update(pd.DataFrame({
            'vaccine_dose': 'none',
            'vaccine_event_time': pd.NaT,
        }, index=pop_data.index))

    def sample_ages(self, index):
        dose_ages = pd.DataFrame(index=index)
        for dose, (age_min, age_max) in self.dose_age_ranges.items():
            dose_age_draw = self.dose_age_randomness.get_draw(index, additional_key=dose)

            mean_age = (age_min + age_max) / 2
            age_std_dev = (mean_age - age_min) / 3

            age_at_dose = scipy.stats.norm(mean_age, age_std_dev).ppf(dose_age_draw)
            age_at_dose[age_at_dose > age_max] = age_max * 0.99
            age_at_dose[age_at_dose < age_min] = age_min * 1.01
            dose_ages[dose] = age_at_dose

        return dose_ages

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

    @staticmethod
    def get_age_ranges(builder):
        schedule = builder.configuration.shigella_vaccine.schedule
        six = [to_years(180), to_years(270)]
        nine = [to_years(270), to_years(300)]
        twelve = [to_years(360), to_years(390)]
        fifteen = [to_years(450), to_years(480)]

        age_ranges = {
            '6_9': {
                'first': six,
                'second': nine,
            },
            '9_12': {
                'first': nine,
                'second': twelve,
            },
            '9_12_15': {
                'first': nine,
                'second': twelve,
                'third': fifteen,
            }
        }
        return age_ranges[schedule]


def to_years(duration):
    days_in_year = 365.25
    seconds_in_day = 60 * 60 * 24

    if isinstance(duration, pd.Timedelta):
        duration = duration.total_seconds() / seconds_in_day

    return duration / days_in_year
