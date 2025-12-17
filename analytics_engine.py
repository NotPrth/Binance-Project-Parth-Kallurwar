import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
from statsmodels.tsa.stattools import adfuller


def calc_beta(a, b):
    df = pd.concat([a, b], axis=1).dropna()
    if len(df) < 20:
        return np.nan
    y = df.iloc[:, 0]
    x = add_constant(df.iloc[:, 1])
    return OLS(y, x).fit().params[1]


def spread_z(a, b, window):
    beta = calc_beta(a, b)
    if np.isnan(beta):
        return pd.Series(), pd.Series()
    spread = a - beta * b
    mean = spread.rolling(window).mean()
    std = spread.rolling(window).std()
    z = (spread - mean) / std
    return spread, z


def rolling_corr(a, b, window):
    return a.rolling(window).corr(b)


def adf(series):
    series = series.dropna()
    if len(series) < 30:
        return None, None
    stat, p = adfuller(series)[0:2]
    return stat, p
