import math
import pandas as pd
from pathlib import Path


def get_project_root():
    root = str(Path(__file__).parent.parent.parent).replace('\\', '/')
    return root


def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    if decimals <= 0:
        return int(math.floor(n * multiplier) / multiplier)
    else:
        return math.floor(n * multiplier) / multiplier


def round_up(n, decimals=0):
    multiplier = 10 ** decimals
    if decimals <= 0:
        return int(math.ceil(n * multiplier) / multiplier)
    else:
        return math.ceil(n * multiplier) / multiplier


def read_pairs():
    pair_settings_dataframe = pd.read_excel("Pairs.xlsx", dtype=str, index_col=False)

    return pair_settings_dataframe
