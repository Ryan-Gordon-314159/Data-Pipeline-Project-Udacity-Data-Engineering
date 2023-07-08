from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 sql = '',
                 table = '',
                 append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_only = append_only
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if not self.append_only:
            self.log.info("Deleting contents of fact table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Load fact table {}".format(self.table))
        redshift.run(f'INSERT INTO {self.table} {self.sql}')
