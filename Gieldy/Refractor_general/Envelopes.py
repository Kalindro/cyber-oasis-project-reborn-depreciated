from talib import NATR
from talib import SMA

from Gieldy.Refractor_general.Get_history import get_history_full


def history_dataframe_envelope(pair, timeframe, days_of_history, ENV_period, ATRP_period, ATRP_fix_global, API):
    history_dataframe = get_history_full(pair=pair, timeframe=timeframe, days_of_history=days_of_history, API=API)
    history_dataframe["ATRP"] = NATR(high=history_dataframe["High"], low=history_dataframe["Low"], close=history_dataframe["Close"],
                                     timeperiod=ATRP_period) / 100 * ATRP_fix_global
    history_dataframe["Envelope_middle"] = SMA(history_dataframe["Close"], timeperiod=ENV_period)
    perc = history_dataframe["ATRP"] / 2
    history_dataframe["Envelope_upper"] = history_dataframe["Envelope_middle"] * (1 + perc)
    history_dataframe["Envelope_lower"] = history_dataframe["Envelope_middle"] * (1 - perc)

    return history_dataframe
