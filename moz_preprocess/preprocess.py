import pandas as pd


def preprocess(series, series_name, min_count):
    """
    Preprocess a series of discrete data by creating dummy columns for the top 3 most-frequently-occurring values
    that also occur more than min_count times. If only 1 value occurs more than min_count times, return None.
    Args:
    * series (pd.Series): Discrete data to create dummy columns for.
    * series_name (str): Name of `series`. Used to name the dummy columns.
    """
    top_values = get_top_values(series, 3, min_count)
    if len(top_values) <= 1:
        return None
    dummies = pd.get_dummies(series.astype(str))[top_values]
    dummies.columns = [f"{series_name}_{x}" for x in dummies.columns]
    return dummies


def get_top_values(series, n, min_count):
    """
    Return the `n` most-frequently-occurring, non-null values in `series`, that also occur more than `min_count` times.
    Args:
    * series (pd.Series): Data to fetch top values in.
    * n (int): Number of values to return.
    * min_count (int): Only return values that occur more than min_count times in series.
    """
    value_counts = series.astype(str).value_counts().head(n)
    return value_counts[value_counts > min_count].index.tolist()
