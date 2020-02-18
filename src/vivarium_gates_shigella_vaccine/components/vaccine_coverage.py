from collections import Counter

import pandas as pd
import scipy.stats

from vivarium_gates_shigella_vaccine import globals as project_globals
from .utilities import sample_beta, to_years


class ShigellaCoverage:

    configuration_defaults = {
        project_globals.SHIGELLA_VACCINE: {
            'schedule': project_globals.SCHEDULES.NONE,
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
        self.schedule = builder.configuration.shigellosis_vaccine.schedule
        self.counts = Counter()

        self.coverage = {}
        for dose, coverage in self.get_dose_coverages(builder).items():
            coverage = builder.lookup.build_table(coverage.reset_index(), parameter_columns=['year'])
            self.coverage[dose] = coverage

        self.dose_age_ranges = self.get_age_ranges(builder)

        self.dose_ages = pd.DataFrame(columns=self.dose_age_ranges.keys())

        self.dose_randomness = builder.randomness.get_stream(f'{self.name}_dose')
        self.dose_age_randomness = builder.randomness.get_stream(f'{self.name}_dose_age')

        columns = [
            'vaccine_dose',
            'vaccine_dose_count',
            'vaccine_event_time',
        ]
        self.population_view = builder.population.get_view(['age', 'alive'] + columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns,
                                                 requires_streams=[f'{self.name}_dose_age'])

        builder.event.register_listener('time_step', self.on_time_step)
        builder.value.register_value_modifier('metrics', self.metrics)

    def on_initialize_simulants(self, pop_data):
        self.dose_ages = self.dose_ages.append(self.sample_ages(pop_data.index))

        self.population_view.update(pd.DataFrame({
            'vaccine_dose': 'none',
            'vaccine_dose_count': 0,
            'vaccine_event_time': pd.NaT,
        }, index=pop_data.index))

    def on_time_step(self, event):
        counts = {f'eligible_for_{dose}_dose_count': 0 for dose in project_globals.VACCINE_DOSES}
        if self.schedule == project_globals.SCHEDULES.NONE:
            self.counts.update(counts)
            return

        pop = self.population_view.get(event.index, query='alive == "alive"')

        age_eligible = self.dose_age_mask(pop, project_globals.DOSES.FIRST, event.step_size)

        pop = self.dose(pop,
                        dose=project_globals.DOSES.FIRST,
                        prior_dose=project_globals.DOSES.NONE,
                        age_mask=age_eligible, event_time=event.time, counts=counts)

        age_eligible = self.dose_age_mask(pop, project_globals.DOSES.SECOND, event.step_size)

        pop = self.dose(pop,
                        dose=project_globals.DOSES.SECOND,
                        prior_dose=project_globals.DOSES.FIRST,
                        age_mask=age_eligible, event_time=event.time, counts=counts)
        pop = self.dose(pop,
                        dose=project_globals.DOSES.CATCHUP,
                        prior_dose=project_globals.DOSES.NONE,
                        age_mask=age_eligible, event_time=event.time, counts=counts)

        if self.schedule == project_globals.SCHEDULES.NINE_TWELVE_FIFTEEN:
            age_eligible = self.dose_age_mask(pop, project_globals.DOSES.THIRD, event.step_size)

            pop = self.dose(pop,
                            dose=project_globals.DOSES.THIRD,
                            prior_dose=project_globals.DOSES.SECOND,
                            age_mask=age_eligible, event_time=event.time, counts=counts)
            # Got first, missed second
            pop = self.dose(pop,
                            dose=project_globals.DOSES.LATE_CATCHUP_MISSED_2,
                            prior_dose=project_globals.DOSES.FIRST,
                            age_mask=age_eligible, event_time=event.time, counts=counts)
            # Missed first, got second
            pop = self.dose(pop,
                            dose=project_globals.DOSES.LATE_CATCHUP_MISSED_1,
                            prior_dose=project_globals.DOSES.CATCHUP,
                            age_mask=age_eligible, event_time=event.time, counts=counts)
            # Missed first, missed, second
            pop = self.dose(pop,
                            dose=project_globals.DOSES.LATE_CATCHUP_MISSED_1_2,
                            prior_dose=project_globals.DOSES.NONE,
                            age_mask=age_eligible, event_time=event.time, counts=counts)

        self.counts.update(counts)
        self.population_view.update(pop)

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

    def dose_age_mask(self, pop, dose, step_size):
        dose_age = self.dose_ages.loc[pop.index, dose]
        return (pop.age < dose_age) & (dose_age <= pop.age + to_years(step_size))

    def dose(self, pop, dose, prior_dose, age_mask, event_time, counts):
        dose_eligible = pop[(pop.vaccine_dose == prior_dose) & age_mask]
        counts[f'eligible_for_{dose}_dose_count'] = len(dose_eligible)

        dose_coverage = self.coverage[dose](dose_eligible.index)
        received_dose = self.dose_randomness.filter_for_probability(dose_eligible.index, dose_coverage,
                                                                    additional_key=dose)
        pop.loc[received_dose, 'vaccine_dose'] = dose
        pop.loc[received_dose, 'vaccine_dose_count'] += 1
        pop.loc[received_dose, 'vaccine_event_time'] = event_time
        return pop

    def get_dose_coverages(self, builder):
        schedule = builder.configuration.shigellosis_vaccine.schedule
        self.catchup_proportion = self.sample_catchup_proportion(builder)

        coverage = {}
        for key in project_globals.COVARIATE_SHIGELLA_COVERAGES:
            data = builder.data.load(key)
            # Vaccine coverage does not vary by age or sex.
            data = data[(data.age_start == 0) & (data.sex == 'Female')].drop(columns=['age_start', 'age_end', 'sex'])
            data = data.set_index(['year_start', 'year_end']).value
            coverage[key] = data

        dose_coverage = {}
        if schedule == project_globals.SCHEDULES.NONE:
            pass
        elif schedule == project_globals.SCHEDULES.SIX_NINE:
            first = coverage[project_globals.COVARIATE_SHIGELLA_6MO]
            second = coverage[project_globals.COVARIATE_SHIGELLA_9MO]
            # Since these coverages come from two different schedules,
            # We treat the probability of receiving dose 2 independently from
            # whether the person received dose 1.
            dose_coverage[project_globals.DOSES.FIRST] = first
            dose_coverage[project_globals.DOSES.SECOND] = second
            dose_coverage[project_globals.DOSES.CATCHUP] = second

        elif schedule == project_globals.SCHEDULES.NINE_TWELVE:
            first = coverage[project_globals.COVARIATE_SHIGELLA_9MO]
            second = coverage[project_globals.COVARIATE_SHIGELLA_12MO]
            dose_coverage[project_globals.DOSES.FIRST] = first
            dose_coverage[project_globals.DOSES.SECOND] = second / first
            dose_coverage[project_globals.DOSES.CATCHUP] = pd.Series(self.catchup_proportion, index=first.index)

        elif schedule == project_globals.SCHEDULES.NINE_TWELVE_FIFTEEN:
            first = coverage[project_globals.COVARIATE_SHIGELLA_9MO]
            second = coverage[project_globals.COVARIATE_SHIGELLA_12MO]
            third = coverage[project_globals.COVARIATE_SHIGELLA_15MO]

            dose_coverage[project_globals.DOSES.FIRST] = first
            dose_coverage[project_globals.DOSES.SECOND] = second / first
            dose_coverage[project_globals.DOSES.THIRD] = third / second
            dose_coverage[project_globals.DOSES.CATCHUP] = pd.Series(self.catchup_proportion, index=first.index)
            dose_coverage[project_globals.DOSES.LATE_CATCHUP_MISSED_1] = pd.Series(self.catchup_proportion, index=first.index)
            dose_coverage[project_globals.DOSES.LATE_CATCHUP_MISSED_2] = pd.Series(self.catchup_proportion, index=first.index)
            dose_coverage[project_globals.DOSES.LATE_CATCHUP_MISSED_1_2] = pd.Series(self.catchup_proportion, index=first.index)
        else:
            raise ValueError(f'Unknown vaccine schedule {schedule}.')
        return dose_coverage

    @staticmethod
    def sample_catchup_proportion(builder):
        draw = str(builder.configuration.input_data.input_draw_number)
        stream = builder.randomness.get_stream('shigella_vaccine_catchup_proportion')
        seed = stream.get_seed(draw)
        mu = builder.configuration.shigellosis_vaccine.catchup_fraction.mean
        sigma = builder.configuration.shigellosis_vaccine.catchup_fraction.sd
        return sample_beta(seed, mu, sigma)

    @staticmethod
    def get_age_ranges(builder):
        schedule = builder.configuration.shigellosis_vaccine.schedule
        six = [to_years(180), to_years(270)]
        nine = [to_years(270), to_years(300)]
        twelve = [to_years(360), to_years(390)]
        fifteen = [to_years(450), to_years(480)]

        age_ranges = {
            project_globals.SCHEDULES.NONE: {},
            project_globals.SCHEDULES.SIX_NINE: {
                project_globals.DOSES.FIRST: six,
                project_globals.DOSES.SECOND: nine,
            },
            project_globals.SCHEDULES.NINE_TWELVE: {
                project_globals.DOSES.FIRST: nine,
                project_globals.DOSES.SECOND: twelve,
            },
            project_globals.SCHEDULES.NINE_TWELVE_FIFTEEN: {
                project_globals.DOSES.FIRST: nine,
                project_globals.DOSES.SECOND: twelve,
                project_globals.DOSES.THIRD: fifteen,
            }
        }
        return age_ranges[schedule]

    def metrics(self, index, metrics):
        metrics['shigella_vaccine_catchup_proportion'] = self.catchup_proportion
        metrics.update(self.counts)
        return metrics
