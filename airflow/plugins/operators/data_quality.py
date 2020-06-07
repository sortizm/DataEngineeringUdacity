from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

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
