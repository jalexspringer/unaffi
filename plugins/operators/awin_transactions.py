from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AwinTransactionOperator(BaseOperator):
    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            table: str,
            s3_bucket: str,
            redshift_conn_id: str = 'redshift_default',
            aws_conn_id: str = 'aws_default',
            copy_options: str = '',
            *args, **kwargs):
        super(AwinTransactionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self._s3_hook = None
        self._postgres_hook = None

    def execute(self, context):
        self._postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self._s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = self._s3_hook.get_credentials()

        copy_query = """
            COPY public.{table}
            FROM 's3://{s3_bucket}/{table}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {self.copy_options};
        """.format(table=self.table,
                   s3_bucket=self.s3_bucket,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self._postgres_hook.run(copy_query)
        self.log.info("COPY command complete...")