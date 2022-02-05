import numpy as np
import pandas as pd
from scipy.stats import linregress


def momentum_function(close):
    returns = np.log(close)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * (rvalue ** 2) * 10000
    return round(momentum, 2)

def average_momentum(close, momentum_period):
    fast_momentum = close.rolling(int(momentum_period * 0.5)).apply(momentum_function)
    slow_momentum = close.rolling(int(momentum_period)).apply(momentum_function)
    average_momentum = (fast_momentum + slow_momentum) / 2
    return average_momentum

def rvalue_function(close):
    returns = np.log(close)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * (rvalue ** 2)
    return rvalue

def average_rvalue(close, momentum_period, rvalue_filter):
    fast_rvalue = close.rolling(int(momentum_period * 0.5)).apply(rvalue_function)
    slow_rvalue = close.rolling(int(momentum_period)).apply(rvalue_function)
    the_fastest_rvalue = fast_rvalue.apply(lambda x: x if x > rvalue_filter else 0)
    the_slowest_rvalue = slow_rvalue.apply(lambda x: x if x > rvalue_filter else 0)
    average_rvalue = ((the_fastest_rvalue ** 2) + (the_slowest_rvalue ** 2)) / 2
    return round(average_rvalue, 4)
