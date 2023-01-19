from statsmodels.stats.proportion import proportion_confint
from scipy.stats import ks_2samp


class DataMiner:
    def __init__(self, dataset):
        """
        Args:
        * dataset (Dataset): Dataset to run the tests on.
        """
        self.dataset = dataset
        self.tests = {}

    def add_test(self, test, columns):
        """
        Args:
        * test (str): The name of a statistical test to run. Possible values: .
        * columns (list of str): The columns you want to run `test` on.
        """
        self.tests[test] = columns

    def set_default_tests(self):
        if self.dataset.split_type == "unordered_discrete":
            self.add_test("binomial", self.dataset.binary_cols)
            self.add_test("ks", self.dataset.continuous_cols)
        elif self.dataset.split_type == "ordered_discrete":
            # test Spearman R on the value itself (continuous) or means (binary / discrete)
            # option for quadratic relationships on continuous
            pass
        elif self.dataset.split_type == "continuous":
            # test spearman R on all values
            pass

    def run_tests(self):
        self.set_alpha_with_bonferroni()
        getattr(self, f"_run_tests_{self.dataset.split_type}")()

    def _run_tests_unordered_discrete(self):
        results = []
        splits = [
            (x, self.dataset.df[self.dataset.df[self.dataset.split_col] == x])
            for x in self.dataset.df[self.dataset.split_col].unique()
        ]
        for test, columns in self.tests.items():
            for c in columns:
                for i, name_split in enumerate(splits):
                    name1, split1 = name_split
                    for name2, split2 in splits[i + 1 :]:
                        getattr(self, f"_run_{test}")(
                            split1[c].dropna(), name1, split2[c].dropna(), name2, c
                        )

    def _run_binomial(self, split1, name1, split2, name2, column):
        MIN_OBS = 10
        ci = []
        for name, split in [(name1, split1), (name2, split2)]:
            if len(split) < MIN_OBS:
                print(
                    f"{column}: Not running binomial test. Too few observations in {name} (MIN_OBS={MIN_OBS})."
                )
                return
            ci.append(
                proportion_confint(
                    split.sum(), len(split), method="wilson", alpha=self.alpha
                )
            )
        if (ci[0][1] < ci[1][0]) or (ci[1][1] < ci[0][0]):
            print()
            print(
                f"{column}: {name1} [{ci[0][0]:.2f}, {ci[0][1]:.2f}] vs {name2} [{ci[1][0]:.2f}, {ci[1][1]:.2f}]"
            )
        else:
            print(f"No difference in {column} for {name1} vs {name2}.")

    def _run_ks(self, split1, name1, split2, name2, column):
        _, p = ks_2samp(split1, split2)
        if p < self.alpha:
            print()
            print(f"{column}: p = {p}")
            print(f"{name1}: {split1.mean():.2f} vs {name2}: {split2.mean():.2f}")
            # plt.clf()
            # split1.hist(color="red", density=True, alpha=0.1, label=name1)
            # split2.hist(color="blue", density=True, alpha=0.1, label=name2)
            # plt.show()
        else:
            print(f"No difference in {column} for {name1} vs {name2}.")

    def set_alpha_with_bonferroni(self):
        ALPHA = 0.05
        n_cols = len(self.dataset.binary_cols) + len(self.dataset.continuous_cols)
        if self.dataset.split_type == "unordered_discrete":
            # For each column, we will test each split against every other split.
            n_splits = self.dataset.df[self.dataset.split_col].nunique()
            n_tests = n_cols * n_splits * (n_splits - 1) / 2
        else:
            n_tests = n_cols
        self.alpha = ALPHA / n_tests
