from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_statements import SqlQuery

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                target_table,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table

    def execute(self, context):
        self.log.info('StageToRedshiftOperator connections init')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        drop_table_query = getattr(SqlQuery, f"{self.target_table}_table_drop")
        redshift.run(drop_table_query)
        
        self.log.info(f"create {self.target_table} fact table:")
        create_table_query = getattr(SqlQuery, f"{self.target_table}_table_create")
        redshift.run(create_table_query)
        
        self.log.info(f"load data to {self.target_table} fact table:")
        insert_table_query = getattr(SqlQuery, f"{self.target_table}_table_insert")
        redshift.run(insert_table_query)

