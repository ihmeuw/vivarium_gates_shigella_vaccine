from collections import Counter

import pandas as pd
from vivarium_public_health.metrics import MortalityObserver
from vivarium_public_health.metrics.utilities import (get_age_bins, QueryString, get_group_counts,
                                                      get_output_template, get_years_of_life_lost)


class ShigellaVaccineObserver:

    configuration_defaults = {
        'metrics': {
            'shigellosis_vaccine_observer': {
                'by_age': False,
                'by_year': False,
                'by_sex': False,
            }
        }
    }

    @property
    def name(self):
        return f'shigellosis_vaccine_observer'

    def setup(self, builder):
        self.schedule = builder.configuration.shigellosis_vaccine.schedule
        self.config = builder.configuration['metrics'][self.name].to_dict()
        self.age_bins = get_age_bins(builder)
        self.counts = Counter()

        columns_required = ['alive', f'vaccine_dose', f'vaccine_event_time']
        if self.config['by_age']:
            columns_required += ['age']
        if self.config['by_sex']:
            columns_required += ['sex']
        self.population_view = builder.population.get_view(columns_required)

        builder.value.register_value_modifier('metrics', self.metrics)
        builder.event.register_listener('collect_metrics', self.on_collect_metrics)

    def on_collect_metrics(self, event):
        base_key = get_output_template(**self.config).substitute(year=event.time.year)
        base_filter = QueryString('')

        pop = self.population_view.get(event.index)
        pop = pop.loc[pop[f'vaccine_event_time'] == event.time]

        doses = ['first', 'second', 'third', 'catchup',
                 'late_catchup_missed_1', 'late_catchup_missed_2', 'late_catchup_missed_1_2']
        dose_counts = {}
        for dose in doses:
            dose_filter = base_filter + f'vaccine_dose == "{dose}"'
            group_counts = get_group_counts(pop, dose_filter, base_key, self.config, self.age_bins)
            for group_key, count in group_counts.items():
                group_key = group_key.substitute(measure=f'shigellosis_vaccine_{dose}_dose_count')
                dose_counts[group_key] = count

        self.counts.update(dose_counts)

    def metrics(self, index, metrics):
        metrics.update(self.counts)
        return metrics

    def __repr__(self):
        return f"ShigellaVaccineObserver()"


class LocationSpecificMortalityObserver(MortalityObserver):

    def setup(self, builder):
        super().setup(builder)

        self.location_specific_life_expectancy = builder.data.load('population.location_specific_life_expectancy')
        # Hack columns to make splitting the pop table a bit easier
        # We end up with ['sex', 'year', 'age_group', 'value']
        age_groups = self.location_specific_life_expectancy['age_start']
        self.location_specific_life_expectancy['age_group'] = pd.cut(age_groups, age_groups.unique(),
                                                                     include_lowest=True)
        self.location_specific_life_expectancy = (self.location_specific_life_expectancy
                                                  .rename(columns={'year_start': 'year'})
                                                  .drop(columns=['year_end', 'age_start', 'age_end']))

    def metrics(self, index, metrics):
        metrics = super().metrics(index, metrics)

        pop = self.population_view.get(index)
        pop.loc[pop.exit_time.isnull(), 'exit_time'] = self.clock()

        # Splits groups by demography then calls
        # sum(self.get_location_specific_life_expectancy(split_index))
        # on each group.
        location_specific_ylls = get_years_of_life_lost(pop, self.config.to_dict(), self.start_time, self.clock(),
                                                        self.age_bins, self.get_location_specific_life_expectancy,
                                                        self.causes)
        location_specific_ylls = {f'location_specific_{k}': v for k, v in location_specific_ylls.items()}
        metrics.update(location_specific_ylls)

        return metrics

    def get_location_specific_life_expectancy(self, index: pd.Index) -> pd.Series:
        """Gets location specific life expectancy for each person in index.

        When called by :func:`get_years_of_life_lost`, guaranteed to only
        get dead people.

        """
        if not index.empty:
            pop = self.population_view.get(index)
            # Build an index for use with our LE table.
            pop['year'] = pop.exit_time.dt.year  # LE is specific by the year the person died.
            age_groups = pd.IntervalIndex(self.location_specific_life_expectancy.age_group.unique())
            pop['age_group'] = pd.cut(pop.age, age_groups)
            index_cols = ['sex', 'year', 'age_group']
            lookup_index = pop.set_index(index_cols).index
            return self.location_specific_life_expectancy.set_index(index_cols).loc[lookup_index].value
        else:
            return pd.Series([0])
