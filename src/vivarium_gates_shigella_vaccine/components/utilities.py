import numpy as np
import pandas as pd
import scipy.stats


def to_years(duration):
    days_in_year = 365.25
    seconds_in_day = 60 * 60 * 24

    if isinstance(duration, pd.Timedelta):
        duration = duration.total_seconds() / seconds_in_day

    return duration / days_in_year


def sample_beta(seed, mu, sigma):
    np.random.seed(seed)
    alpha = mu * (mu * (1 - mu) / sigma ** 2 - 1)
    beta = (1 - mu) * (mu * (1 - mu) / sigma ** 2 - 1)
    return scipy.stats.beta.rvs(alpha, beta)
