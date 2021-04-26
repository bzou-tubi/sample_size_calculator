from statsmodels.stats.power import tt_ind_solve_power
import numpy as np

# Set of helper functions that do the power tests

# Functions that set default parameters

def effect(effect=0.010):
    return effect

def treatments(treatments=1):
    return treatments

def allocation(allocation=0.50):
    return allocation

def power(power=0.8):
    return power

def alpha(alpha=0.05):
    return alpha


def sample_power_ttest(p1, p2, sd_diff, alpha=0.05, power=0.8, ratio=1, alternative = 'two-sided'):
    mean_diff = abs(p2 - p1)
    std_effect_size = np.divide(mean_diff, sd_diff)
    n = tt_ind_solve_power(effect_size=std_effect_size, 
                         alpha=alpha, 
                         power=power, 
                         ratio=ratio, 
                         alternative=alternative) # Potential improvement: make this able to handle one-sided tests
    return np.array(n).round()

# ---------- Constants ---------- # 

def calculate_sample_required(df, 
                              effect_size_relative, 
                              number_variations, 
                              allocation, 
                              power, 
                              alpha, 
                              col_name_p = 'avg_cuped_result', 
                              std_col_name = 'std_cuped_result', 
                              ratio = 1):
    
    corrected_alpha = alpha.result / number_variations.result
    p2_multiplicative_factor =  1 + effect_size_relative.result

    # ---------- Implementation ---------- #
    df['sample_required'] =  df.apply(lambda row: sample_power_ttest(p1 = row[col_name_p], 
                                                                     p2 = row[col_name_p] * p2_multiplicative_factor, 
                                                                     sd_diff = row[std_col_name], 
                                                                     alpha = corrected_alpha, 
                                                                     power = power.result, 
                                                                     ratio = ratio
                                                                    ), axis=1)

    df['weeks_required'] = np.divide(df['sample_required'], (df['observations'] * 0.5 * allocation.result))
    df['sample_required'] = df['sample_required'].astype('int')
    df['weeks_required'] = df['weeks_required'].astype('float')
    
    return df