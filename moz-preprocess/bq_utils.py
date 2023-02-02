from google.cloud import bigquery


def fetch_weekly_aggregate(week_start_date, segment, filter=None):
    """
    Fetch a DataFrame from BigQuery with 1 week's worth of aggregated data for all Firefox Desktop clients
    satisfying `filter`. The DataFrame has a column called "segment" to segment clients on. The DataFrame
    has 1 row for each (client_id, segment).

    Args:
    * week_start_date (str): Date in YYYY-MM-DD format.
    * segment (str): SQL select clause to create a field for segmenting clients.
    * filter (str): SQL where clause to apply to Clients Daily or Search Clients Daily.
    """
    if not filter:
        filter = "True = True"
    with open("weekly_aggregate.sql", "r") as f:
        sql = f.read().strip()
    sql = (
        sql.replace("@week_start_date", week_start_date)
        .replace("@segment", segment)
        .replace("@filter", filter)
    )
    client = bigquery.Client(project="mozdata")
    return client.query(sql).to_dataframe()
