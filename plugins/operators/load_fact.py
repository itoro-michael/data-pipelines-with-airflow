from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {table_name}
        {select_sql}
    """

    @apply_defaults
    def __init__(self,                 
                 redshift_conn_id="",
                 table_name="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_sql = LoadFactOperator.insert_sql.format(
            table_name=self.table_name,
            select_sql=self.select_sql
        )
        self.log.info('Load fact table from dimensions')
        redshift.run(insert_sql)
