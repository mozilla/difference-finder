import os 
from google.cloud import bigquery


def fetch_weekly_aggregate(week_start_date, segment, target=None):
    """
    Fetch a DataFrame from BigQuery with 1 week's worth of aggregated data for all Firefox Desktop clients
    satisfying `filter`. The DataFrame has a column called "segment" to segment clients on. The DataFrame
    has 1 row for each (client_id, segment).

    Args:
    * week_start_date (str): Date in YYYY-MM-DD format.
    * segment (str): SQL select clause to create a field for segmenting clients.
    * target (str): SQL where clause to apply to Clients Daily or Search Clients Daily.
    """
    if not target:
        target = "True = True"
    path_to_parent_directory = os.path.dirname(os.path.realpath(__file__))
    path_to_file = f"{path_to_parent_directory}/weekly_aggregate.sql"  # To Do: This doesn't work on Windows.
    with open(path_to_file, "r") as f:
        sql = f.read().strip()
    sql = (
        sql.replace("@week_start_date", week_start_date)
        .replace("@segment", segment)
        .replace("@target", target)
    )
    client = bigquery.Client(project="mozdata")
    return client.query(sql).to_dataframe()
