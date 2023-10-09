from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_statements import SqlQuery

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        TIMEFORMAT 'epochmillisecs'
    """
    @apply_defaults
    def __init__(self,
                aws_credentials_id="",
                redshift_conn_id="",
                target_table="",
                s3_bucket="",
                s3_key="",
                s3_schema="",
                *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_schema = s3_schema

    def execute(self, context):
        self.log.info('StageToRedshiftOperator connections init..')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("drop redshift table")
        drop_table_query = getattr(SqlQuery, f"{self.target_table}_table_drop")
        redshift.run(drop_table_query)

        self.log.info("create redshift table:")
        create_table_query = getattr(SqlQuery, f"{self.target_table}_table_create")
        redshift.run(create_table_query)

        s3_files_path =  "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.s3_schema:
            s3_files_schema = self.s3_schema
        else:
            s3_files_schema = 'auto'

        COPY_SQL_FORMATTED = StageToRedshiftOperator.COPY_SQL.format(self.target_table, 
            s3_files_path,
            credentials.access_key, 
            credentials.secret_key,
            s3_files_schema
        )
        redshift.run(COPY_SQL_FORMATTED)