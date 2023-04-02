from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 tableAppend = False,
                 sql_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id 
        self.table = table
        self.tableAppend = tableAppend 
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading Dimension table {self.table}") 
        
        # If self.tableAppend=True then append to table
        if self.tableAppend:
            execute_sql = """
            INSERT INTO {}
            {} ;
            """.format(self.table, self.sql_query) 
 
        # If Append is False (which we set as default), then clear the table first. Use the 'TRUNCATE' statment from postgres which is more efficient than 'DELETE' as all data is cleared without scanning the table first
        else:
            execute_sql = """
            TRUNCATE TABLE {} ;
            INSERT INTO {}
            {} ;
            """.format(self.table, self.table, self.sql_query)          
          
        redshift.run(execute_sql)
