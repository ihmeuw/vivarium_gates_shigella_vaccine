import numpy as np
from vivarium_public_health import utilities


class FertilityCrudeBirthRate:
    """Population-level model of births using crude birth rate.

    The number of births added each time step is calculated as

    new_births = sim_pop_size_t0 * live_births / true_pop_size * step_size

    Where

    sim_pop_size_t0 = the initial simulation population size
    live_births = annual number of live births in the true population
    true_pop_size = the true population size

    This component has configuration flags that determine whether the
    live births and the true population size should vary with time.

    Notes
    -----
    The OECD definition of crude birth rate can be found on their
    `website <https://stats.oecd.org/glossary/detail.asp?ID=490>`_,
    while a more thorough discussion of fertility and
    birth rate models can be found on
    `Wikipedia <https://en.wikipedia.org/wiki/Birth_rate>`_ or in demography
    textbooks.

    """

    @property
    def name(self):
        return "crude_birthrate_fertility"

    def setup(self, builder):
        self.birth_rate = self.load_birth_rate(builder)
        self.clock = builder.time.clock()
        self.randomness = builder.randomness.get_stream('crude_birth_rate')
        self.simulant_creator = builder.population.get_simulant_creator()
        builder.event.register_listener('time_step', self.on_time_step)

    def on_time_step(self, event):
        """Adds new simulants every time step based on the Crude Birth Rate
        and an assumption that birth is a Poisson process
        Parameters
        ----------
        event
            The event that triggered the function call.
        """
        birth_rate = self.birth_rate.at[self.clock().year]
        step_size = utilities.to_years(event.step_size)

        mean_births = birth_rate * step_size
        # Assume births occur as a Poisson process
        r = np.random.RandomState(seed=self.randomness.get_seed())
        simulants_to_add = r.poisson(mean_births)

        if simulants_to_add > 0:
            self.simulant_creator(simulants_to_add,
                                  population_configuration={
                                      'age_start': 0,
                                      'age_end': 0,
                                      'sim_state': 'time_step',
                                  })

    @staticmethod
    def load_birth_rate(builder):
        initial_population_size = builder.configuration.population.population_size
        start_year = builder.configuration.time.start.year
        births = (builder.data.load('covariate.live_births_by_year.estimate')
                  .drop(columns='year_end')
                  .set_index('year_start'))
        pop = builder.data.load('population.structure')
        pop = pop.groupby(['year_start'])['value'].sum()
        pop = pop.at[start_year]
        live_birth_rate = (initial_population_size / pop) * births
        return live_birth_rate.value

    def __repr__(self):
        return "FertilityCrudeBirthRate()"
