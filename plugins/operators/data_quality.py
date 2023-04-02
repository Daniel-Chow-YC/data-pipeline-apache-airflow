from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks 
        
    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for check in self.data_quality_checks:
            sql_query = check.get('test')
            result = check.get('result') 
            records = redshift_hook.get_records(sql_query)
            
            if records[0][0] == result:
                raise ValueError(f"Data quality check failed. The test '{sql_query}' returned: {result}") 
                
            self.log.info(f"Data quality check passed. The test '{sql_query}' returned: {result}")