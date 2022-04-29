from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {table_name}
        {select_sql}
    """

    @apply_defaults
    def __init__(self,                 
                 redshift_conn_id="",
                 table_name="",
                 select_sql="",
                 append_row=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.select_sql = select_sql
        self.append_row = append_row

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_row:
            self.log.info("Delete data if exist from dimension table")
            redshift.run("DELETE FROM {}".format(self.table_name))
        
        insert_sql = LoadDimensionOperator.insert_sql.format(
            table_name=self.table_name,
            select_sql=self.select_sql
        )
        self.log.info('Load dimension from staging tables')
        redshift.run(insert_sql)
