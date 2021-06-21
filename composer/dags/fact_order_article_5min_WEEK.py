from datetime import datetime, timedelta
from airflow import models
from dependencies.helpers import create_tasks, get_layers_directories
from dependencies.pattern import get_pattern_date, get_pattern_timestamp, get_pattern_full

default_args = {
	'start_date': datetime(2021,6,14),
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

mon_env = models.Variable.get('gcp_project')
pattern = "5min"
mon_pattern_date = get_pattern_date(pattern)
mon_pattern_timestamp = get_pattern_timestamp(pattern)
mon_pattern_full = get_pattern_full(pattern)
mon_pattern_date_long = get_pattern_date_long(pattern)
cron_schedule = None
nom_dag = 'fact_order_article_5min_week'

with models.DAG(
		nom_dag,
		schedule_interval=cron_schedule,
        template_searchpath=get_layers_directories(nom_dag),
		default_args=default_args,
        params={
            "environnement": mon_env,
            "pattern_date": mon_pattern_date,
            "pattern_timestamp": mon_pattern_timestamp,
            "pattern_full": mon_pattern_full,
            "pattern_date_long": mon_pattern_date_long

        }) as dag:

    create_tasks(nom_dag)