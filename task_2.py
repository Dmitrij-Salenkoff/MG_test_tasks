import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

sql_query = """WITH 
update_data AS (
    WITH 
        accounts_costs AS
    (
       SELECT date,
              'Google Ads' AS source_name,
              account_name,
              cost
       FROM   `raw_data.google_ads`
       UNION ALL
       SELECT date,
              'Yandex Direct' AS source_name,
              account_name,
              cost
       FROM   `raw_data.yandex_direct` ),

        leads AS
    (
         SELECT   date_create,
                  account_name,
                  count(*) AS leads_count
         FROM     `raw_data.crm_deals`
         GROUP BY date_create,
                  account_name )

    SELECT    accounts_costs.date         AS date,
              accounts_costs.source_name  AS source_name,
              accounts_costs.account_name AS account_name,
              accounts_costs.cost         AS cost,
              leads.leads_count           AS leads_count
    FROM      accounts_costs
    LEFT JOIN leads
    ON        accounts_costs.date = leads.date_create
    AND       accounts_costs.account_name = leads.account_name
    WHERE     date >= date_sub(CURRENT_DATE(), interval 30 day))


UPDATE `mart_data.marketing_account` AS o
SET    leads_count = n.leads_count
FROM   update_data n
WHERE  o.date >= date_sub(CURRENT_DATE(), interval 30 day)"""

with DAG(
        dag_id="mart_data.marketing_account",
        schedule='0 3 * * *',
        start_date=pendulum.datetime(2023, 1, 1),
        catchup=False,
        dagrun_timeout=pendulum.duration(minutes=60),
) as dag:
    join_tables_job_task = BigQueryInsertJobOperator(
        task_id="join_tables_job",
        configuration={
            "query": {
                "query": sql_query,
                "useLegacySql": False,
            }
        }
    )
