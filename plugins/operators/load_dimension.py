from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_statements import SqlQuery

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                truncate=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
    
    def execute(self, context):
        self.log.info('LoadDimensionOperator init..')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"create {self.table} dim table, skip if table already exist")
        create_dim_table = getattr(SqlQuery, f"{self.table}_table_create")                
        redshift.run(create_dim_table)

        self.log.info(f"load data to {self.table} table")
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info(f"load data to the dimension {self.table}:")
        insert_dim_table = getattr(SqlQuery, f"{self.table}_table_insert")
        redshift.run(insert_dim_table)