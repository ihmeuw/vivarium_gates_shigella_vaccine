from typing import Union

import numpy as np
import pandas as pd
import scipy.stats


def to_years(duration: Union[int, float, pd.Timedelta]) -> float:
    """Coerce time spans into fractions of a year.

    Parameters
    ----------
    duration
        A timespan in days if a number, or any span represented by
        a time delta.

    Returns
    -------
        The time span as a fraction of a year.

    """
    days_in_year = 365.25
    seconds_in_day = 60 * 60 * 24

    if isinstance(duration, pd.Timedelta):
        duration = duration.total_seconds() / seconds_in_day

    return duration / days_in_year


def sample_beta(seed: int, mu: float, sigma: float):
    """Gets a single sample from a beta distribution.

    Parameters
    ----------
    seed
        The seed for the random number generator for reproducibility.
    mu
        The mean of the distribution.
    sigma
        The standard deviation of the distribution.

    Returns
    -------
        The beta-distributed random sample.

    """
    np.random.seed(seed)
    alpha = mu * (mu * (1 - mu) / sigma ** 2 - 1)
    beta = (1 - mu) * (mu * (1 - mu) / sigma ** 2 - 1)
    return scipy.stats.beta.rvs(alpha, beta)
