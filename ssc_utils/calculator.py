from statsmodels.stats.power import tt_ind_solve_power
import numpy as np

# Set of helper functions that do the power tests

# Functions that set default parameters

def effect(x=0.01):
    return x

def variations(x=1):
    return x

def allocation(x=0.50):
    return x

def power(x=0.8):
    return x

def alpha(x=0.05):
    return x


def sample_power_ttest(p1, p2, sd_diff, alpha=0.05, power=0.8, ratio=1, alternative = 'two-sided'):
    mean_diff = abs(p2 - p1)
    std_effect_size = np.divide(mean_diff, sd_diff)
    n = tt_ind_solve_power(effect_size=std_effect_size, 
                         alpha=alpha, 
                         power=power, 
                         ratio=ratio, 
                         alternative=alternative) # Potential improvement: make this able to handle one-sided tests
    return np.array(n).round()

