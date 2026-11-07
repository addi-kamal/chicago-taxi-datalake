"""Pipeline médaillon Chicago Taxi Trips (bronze -> silver -> gold).

Le code métier de ce package est indépendant de l'orchestrateur : les DAGs Airflow
(`dags/`) ne font qu'appeler les fonctions de `taxi_pipeline.jobs`.
"""

__version__ = "1.0.0"
