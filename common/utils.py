def dot_wei_to_ether(wei: str) -> float:
    ether_value = float(wei) / 1e10
    return ether_value


def calc_net_profit(market_price: float, avg_cost: float, total_size: float) -> float:
    net_profit = (market_price - avg_cost) * total_size
    return net_profit
