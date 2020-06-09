from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Load a fact table using a INSERT INTO SELECT query

    Args:
        redshift_conn_id (str): Id of the Redshift connection
        table (str): name of the table where the data will be loaded
        select_query (str): the SELECT part of the INSERT INTO SELECT query
        truncate_insert (bool): whether to truncate the table before inserting or not
        *args: Variable argument list passed to Base Operator
        **kwargs: Key arguments passed to Base Operator
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 truncate_insert=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info(f"Clearing data from destination Redshift table {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Inserting data into table: {self.table}")
        insert_sql = f"""INSERT INTO {self.table}\n{self.select_query}"""
        self.log.debug(f"Running query: {insert_sql}")
        redshift.run(insert_sql)
