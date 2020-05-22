from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 operation="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.sql_query= sql_query
        self.operation= "truncate"

    def execute(self, context):
        """
          Insert data into dimensional tables from staging events and song data.
          Using a truncate-insert method to empty target tables prior to load.
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.operation == 'truncate':
            redshift.run(f"TRUNCATE TABLE {self.table}")
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql)
        
        self.log.info(f"Success: {self.task_id}")
