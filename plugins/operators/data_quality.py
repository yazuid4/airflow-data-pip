from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 not_null_columns={},
                 not_empty_tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.not_null_columns = not_null_columns
        self.not_empty_tables = not_empty_tables
    
    def execute(self, context):
        self.log.info('DataQualityOperator execution starting..')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # check if tables are empty:
        for table in self.not_empty_tables:
            records = redshift.get_records(f"select * from {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"data validation failed, {table} is empty!")
        
        # check if null values exists in table columns:
        for table, col in self.not_null_columns:
            records = redshift.get_records(f"select count(*) from {table} where {col} is NULL")
            if len(records) > 0 and records[0][0] > 0:
                raise ValueError(f"data validation failed!, {col} in table {table} should not contains NULL values")

        self.log.info("data validation checks are passed successfuly.")
