from CCXT_functions_builtin import get_pairs_precisions_status


def pairs_list_USDT(API):
    """Get all USDT active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]

    return pairs_list


def pairs_list_BTC(API):
    """Get all BTC active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]

    return pairs_list
