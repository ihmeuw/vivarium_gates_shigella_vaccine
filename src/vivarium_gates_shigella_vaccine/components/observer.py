from collections import Counter

from vivarium_public_health.metrics.utilities import get_age_bins, QueryString, get_group_counts, get_output_template


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
