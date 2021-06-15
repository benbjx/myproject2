from datetime import datetime, timedelta
from airflow import models
from dependencies.helpers import create_tasks, get_layers_directories, get_pattern_date

default_args = {
	'start_date': datetime(2020,3,17),
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

mon_env = models.Variable.get('gcp_project')
mon_pattern_date = get_pattern_date("LAST_24H")
cron_schedule = "35 3 * * *"
nom_dag = 'dim_employee_last_24h'

with models.DAG(
		nom_dag,
		schedule_interval=cron_schedule,
        template_searchpath=[get_layers_directories(nom_dag)],
		default_args=default_args,
        params={
            "environnement": mon_env,
            "pattern_date": mon_pattern_date,
        }) as dag:

    create_tasks(nom_dag)