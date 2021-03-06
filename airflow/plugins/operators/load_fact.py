from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Creates fact table in the Redshift Cluster from staging tables
    
    Params:
        redshift_conn_id: a reference to a specific postgres database, type: str
        insert_only: whether we want the INSERT-ONLY mode or the TRUNCATE-BEFORE-INSERT mode, type: bool, default: True
        table: table name that we want to perform the operations on, type: str
        sql: a reference to the SQL query that insert records into the particular table, type : str
        
    Return:
        Nothing
    """

    ui_color = '#F98866'
    
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="redshift",
                 insert_only = True,
                 table = "",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.insert_only = insert_only
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('Starting LoadFactOperator...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.insert_only:
            self.log.info('Deleting from existing table...')
            redshift.run("TRUNCATE public.{};".format(self.table))
        self.log.info('Inserting into fact table from staging tables:')
        redshift.run(self.sql)
