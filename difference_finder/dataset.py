class Dataset:
    def __init__(self, df, split_col, split_type, binary_cols, continuous_cols):
        """
        Args:
        * df (pd.DataFrame): A dataframe with one row per (client, split) and all
          the columns you want to test for differences.
        * split_col (str): The name of a column in `df` that identifies how you
          want to split the data for testing.
        * split_type (str): How you want to treat the `split_col`. Possible values:
          'unordered_discrete', 'ordered_discrete', 'continuous'.
        * binary_cols (list of str): Columns in `df` that you want to treat as
          binary for testing. To test a discrete column, create an indicator column
          for each value and test the indicator columns.
        * continuous_cols (list of str): Columns in `df` that you want to treat as
          continuous for testing.
        """
        self.df = df
        self.split_col = split_col
        self.split_type = split_type
        self.binary_cols = binary_cols
        self.continuous_cols = continuous_cols
