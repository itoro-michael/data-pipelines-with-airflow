from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pathlib import Path

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}' ;
    """

    @apply_defaults
    def __init__(self,                 
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_param="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key        
        self.aws_credentials_id = aws_credentials_id
        self.json_param = json_param

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='redshift')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Create staging and dimension tables if not exists")
        create_sql = Path(Path(__file__).parent.parent.parent/"dags"/"project"/ \
                          "create_tables.sql").read_text()
        redshift.run(create_sql)
        
        
        self.log.info("Delete content of staging table.")
        redshift.run("DELETE FROM public.{}".format(self.table))
        
        
        self.log.info("Copy data from S3 to Redshift")
        rendered_key = self.s3_key.format()
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_param
        )
        redshift.run(formatted_sql)
        
        self.log.info('Data staged to Redshift')





