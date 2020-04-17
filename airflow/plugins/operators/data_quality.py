from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Run particular user-defined tests. Currently I am testing whether there are any null values for the primary key of particular tables.
    
    Params:
        redshift_conn_id: a reference to a specific postgres database, type: str
        tests: a list of dictionaries representing the table, the column to be tested, and the expected value of the query, type: List[dict]
    
    Return:
        Nothing.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="redshift",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for test in self.tests:
            table = test["table"]
            column = test["column"]
            expected_val = test["expected_val"]
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM public.{table} WHERE {column} = NULL")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records != expected_val:
                raise ValueError(f"Data quality check failed. {table} contained {num_records} rows.\
                            {table} is expected to contain {expected_val} rows.")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")