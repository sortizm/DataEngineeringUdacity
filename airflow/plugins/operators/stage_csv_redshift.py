from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageCSVToRedshiftOperator(BaseOperator):
    """
    Load CSV data from an S3 bucket into a stage table using a COPY command

    Args:
        redshift_conn_id (str): Id of the Redshift connection
        iam_role_arn (str): IAM role ARN used to authenticate against the S3 bucket
        table (str): name of the table where the data will be loaded
        s3_bucket (str): name of the S3 bucket
        s3_key (str): key of the object in the S3 bucket
        delimiter (str): delimiter used in the CSV file
        ignore_headers (int): number of lines from the top of the files that are headers (to ignore them)
        *args: Variable argument list passed to Base Operator
        **kwargs: Key arguments passed to Base Operator
    """

    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 iam_role_arn='',
                 table="",
                 s3_bucket='',
                 s3_key='',
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageCSVToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.iam_role_arn = iam_role_arn
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Copying data from S3 to Redshift: {s3_path}")
        formatted_sql = StageCSVToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.iam_role_arn,
            self.ignore_headers,
            self.delimiter,
        )
        redshift.run(formatted_sql)
