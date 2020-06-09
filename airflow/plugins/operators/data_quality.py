from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Operator to do quality checks on Redshift tables. Extends BaseOperator

    Args:
        redshift_conn_id (str): Id of the Redshift connection
        checks ([]): List of checks to run. Each check is a dictionary with the following keys:
            - test (str): the query to run to get a result
            - equals (str) [optional]: a value to compare the result against. Check passes if equal
            - not_equals (str) [optional]: a value to compare the result against. Check passes if not equal
            Either equals or not_equals keys should be provided
        *args: Variable argument list passed to Base Operator
        **kwargs: Key arguments passed to Base Operator
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.checks:
            self.log.info(f"Running quality check {check['test']}")
            records = redshift.get_records(check['test'])
            
            if len(records) < 1 or len(records[0]) < 1:
                raise Exception("Check failed. Test returned no results")
            result = records[0][0]
            
            if check.get("equals") == str(result):
                self.log.info(f"Check passed with result {result}")
            elif check.get("not_equals") and check.get("not_equals") != str(result):
                self.log.info(f"Check passed with result {result}")
            else:
                raise Exception(f"Check failed with result {result}")
