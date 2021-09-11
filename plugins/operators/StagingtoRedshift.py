from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StagetoRedshiftOperator(BaseOperator):
    """
    Stages data to a specific redshift cluster from a specified S3 location.
    
    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param aws_credentials: reference to a aws hook containing iam details
    :type aws_credentials: str
    :param table: destination staging table on redshift.
    :type table: str
    :param s3_bucket: source s3 bucket name
    :type s3_bucket: str
    :param s3_key: source s3 prefix 
    :type s3_key: str
    """

    staging_table_stmt = """
        COPY {} FROM 's3://{}/{}'
        CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' CSV;
        """
    @apply_defaults
    def __init__(
                self,
                redshift_conn_id='',
                aws_credentials_id='',
                table='',
                s3_bucket='',
                s3_key='',
                *args,
                **kwargs
            ):
        super().__init__(*args,**kwargs)
        redshift_conn_id = redshift_conn_id
        aws_credentials_id = aws_credentials_id
        table = table
        s3_bucket = s3_bucket
        s3_key = s3_key

    def execute(self, context):
        self.log.info('The Starting of StagetoRedshift')
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        credentials = aws_hook.get_credentials()

        redshift_hook.run("TRUNCATE {}".format(self.table))

        sql_stmt = StagetoRedshift.staging_table_stmt.format(
                                        self.table,
                                        self.s3_bucket,
                                        self.s3_key,
                                        credentials.access_key,
                                        credentials.secret_key
                                        )
        self.log.info(f"Running COPY SQL: {sql_stmt}")
        redshift_hook.run(sql_stmt)
