from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDataOperator(BaseOperator):
    """
    Loads data to the given fact or dimension table by running the provided sql statement.

    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination table on redshift.
    :type table: str
    :param columns: columns of the destination table
    :type columns: str containing column names in csv format.
    :param sql_stmt: sql statement to be executed.
    :type sql_stmt: str
    :param append: if False, a delete-insert is performed.
        if True, a append is performed.
        (default value: False)
    :type append: bool
    """
    load_stmt = "INSERT INTO {} {} {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 columns='',
                 stmt='',
                 append=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        redshift_conn_id = redshift_conn_id
        table = table
        columns = columns
        stmt = stmt
        append = append

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift)

        if not self.append:
            redshift_hook.run("TRUNCATE {}".format(self.table))

        redshift_hook.run(LoadData.load_stmt.format(self.table,
                                                    self.columns,
                                                    self.stmt))
