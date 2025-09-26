from airflow import DAG

with DAG(
    dag_id="dag_quality",
    description="Validation automatiques avec Soda Core et test de qualité"
) as dags:
    pass