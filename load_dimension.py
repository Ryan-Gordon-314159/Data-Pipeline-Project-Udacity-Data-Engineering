from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql = '',
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.append_only = append_only
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if not self.append_only:
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Deleting {} dimension table".format(self.table))

        self.log.info("Inserting data from staging table to {} dimension table".format(self.table))

        statement = f"Insert INTO {self.table} \n{self.sql}"
        redshift.run(statement)

        self.log.info(f"Running SQL: \n{statement}")
        self.log.info(f"Successfully inserted data into {self.table}")
