import os
from google.cloud import bigquery


path_to_parent_directory = os.path.dirname(os.path.realpath(__file__))


def fetch_weekly_aggregate(
    week_start_date, segment, target=None, sample=None, verbose=False
):
    """
    Fetch a DataFrame from BigQuery with 1 week's worth of aggregated data for all Firefox Desktop clients
    satisfying `filter`. The DataFrame has a column called "segment" to segment clients on. The DataFrame
    has 1 row for each (client_id, segment).

    Args:
    * week_start_date (str): Date in YYYY-MM-DD format.
    * segment (str): SQL select clause to create a field for segmenting clients.
    * target (str): SQL where clause to apply to Clients Daily or Search Clients Daily.
    * sample (int): If not None, takes a sample of weekly clients stratified across segments
      (i.e., takes a random sample of N weekly clients from each segment where N=`sample`).
    * verbose (bool): If True, prints the SQL that is run.
    """
    if not target:
        target = "True = True"    
    if not sample:
        sample = "True = True"
    else:
        sample = f"RAND() < {str(sample)} / clients_per_segment"
    path_to_file = os.path.join(path_to_parent_directory, "weekly_aggregate.sql")
    with open(path_to_file, "r") as f:
        sql = f.read().strip()
    sql = sql.format(week_start_date=week_start_date, segment=segment, target=target, sample=sample)
    if verbose:
        print(sql)
    client = bigquery.Client(project="mozdata")
    return client.query(sql).to_dataframe()