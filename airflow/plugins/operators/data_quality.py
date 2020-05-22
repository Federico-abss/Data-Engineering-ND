from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    ops = {
    "==": operator.eq,
    ">": operator.lt,
    "<": operator.gt,
    "!=": operator.ne,
    ">=": operator.ge,
    "<=": operator.le
    }  

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        '''
            Data quality check, it allows the user to create custom test,
            it first ensures that the query returned any result and then 
            compares it with logical operators to the expected value.
            It works by using the dictionaries inside the 'dq_checks' array to 
            retrieve query, result and operator to query against the tables 
            listed in the tables array.
            dq_checks=[{'query': desired query
                        'expected_result': value
                        'operator': one of ==, !=, >=, <=, > or <
                        }]
        '''
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in dq_checks:
            for table in self.tables:
                records = redshift_hook.get_records(check['query'].format(table))
                comp_func = ops[check['operator']]

                if len(records) < 1:
                    self.log.error(f"Data quality check failed. {table} returned no results")
                    raise ValueError(f"Data quality check failed. {table} returned no results")
                    
                for record in records:
                    if not comp_func(record[0], check['expected_value']):
                        raise ValueError("Data quality check failed, {} is not {} {}".
                                         format(record[0], check['operator'], check['expected_value']))

            self.log.info(f"Data quality on table {table} check passed with {record[0]} records")