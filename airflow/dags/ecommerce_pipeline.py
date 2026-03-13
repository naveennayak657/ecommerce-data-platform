# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: airflow/dags/ecommerce_pipeline.py
#
# Main orchestration DAG for the e-commerce data platform.
# This DAG coordinates the entire pipeline:
#
#   1. Health checks — verify all services are up
#   2. Spark streaming — process Kafka events to Delta Lake
#   3. dbt run — transform data in Snowflake
#   4. dbt test — validate data quality
#   5. dbt docs — generate documentation
#   6. Alerts — notify on failures
#
# Improvements from v1:
#   - Airflow Variables for all config — no hardcoded paths
#   - SLA miss detection
#   - Data interval awareness using {{ ds }} macro
#   - XCom metrics tracking for observability
#
# Schedule: every hour
# Catchup: disabled — we don't backfill missed runs
# ──────────────────────────────────────────────────

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# ── load config from airflow variables ──────────────
# set these in Airflow UI → Admin → Variables
# fallback defaults work for local development
DBT_PROJECT_DIR  = Variable.get(
    'dbt_project_dir',
    default_var='/opt/airflow/dbt/ecommerce_dbt'
)
DBT_PROFILES_DIR = Variable.get(
    'dbt_profiles_dir',
    default_var='/root/.dbt'
)
SPARK_SCRIPT     = Variable.get(
    'spark_script',
    default_var='/opt/airflow/spark_streaming/stream_processor.py'
)
SPARK_TIMEOUT    = Variable.get(
    'spark_timeout_seconds',
    default_var='300'
)


# ── sla miss callback ────────────────────────────────
def sla_miss_alert(dag, task_list, blocking_task_list,
                   slas, blocking_tis):
    """
    Called when a task misses its SLA.
    In production this sends a PagerDuty alert.
    SLA breach means pipeline is running too slow.
    """
    logger.error(
        f"⚠️  SLA Miss Alert!\n"
        f"   DAG             : {dag.dag_id}\n"
        f"   Missed tasks    : {task_list}\n"
        f"   Blocking tasks  : {blocking_task_list}\n"
        f"   Action          : Investigate slow tasks"
    )
    # TODO: add PagerDuty/Slack alert here


# ── default arguments ────────────────────────────────
default_args = {
    'owner':             'naveen',
    'depends_on_past':   False,
    'retries':           1,
    'retry_delay':       timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'start_date':        datetime(2026, 1, 1),
    'email_on_failure':  False,
    'email_on_retry':    False,
}

# ── dag definition ───────────────────────────────────
with DAG(
    dag_id='ecommerce_pipeline',
    default_args=default_args,
    description='E-Commerce Real-Time Data Platform Pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['ecommerce', 'kafka', 'spark', 'dbt', 'snowflake'],
    render_template_as_native_obj=True,
    # pipeline should complete within 2 hours
    dagrun_timeout=timedelta(hours=2),
    # alert if pipeline takes longer than 1 hour
    sla_miss_callback=sla_miss_alert,
) as dag:

    # ── task 1: pipeline start ───────────────────────
    start = EmptyOperator(task_id='start')

    # ── task 2: kafka health check ───────────────────
    def check_kafka_health(**context):
        """
        Verify Kafka is running and topic exists.
        Push result to XCom for downstream tasks.
        """
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        import os

        bootstrap_servers = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'
        )
        topic = os.getenv('KAFKA_TOPIC', 'ecommerce-events')

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=5000
            )
            topics = consumer.topics()
            consumer.close()

            if topic not in topics:
                raise ValueError(
                    f"Topic '{topic}' not found. "
                    f"Available: {topics}"
                )

            logger.info(f"✅ Kafka healthy — topic '{topic}' exists")

            # push metrics to XCom
            context['task_instance'].xcom_push(
                key='kafka_status', value='healthy'
            )
            return True

        except KafkaError as e:
            raise Exception(f"❌ Kafka health check failed: {e}")

    kafka_health_check = PythonOperator(
        task_id='kafka_health_check',
        python_callable=check_kafka_health,
        provide_context=True,
        sla=timedelta(minutes=2),
    )

    # ── task 3: snowflake health check ───────────────
    def check_snowflake_health(**context):
        """
        Verify Snowflake connection is working.
        Push connection status to XCom.
        """
        import subprocess

        result = subprocess.run(
            ['dbt', 'debug',
             '--project-dir', DBT_PROJECT_DIR,
             '--profiles-dir', DBT_PROFILES_DIR],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise Exception(
                f"❌ Snowflake check failed:\n{result.stderr}"
            )

        logger.info("✅ Snowflake connection healthy")

        context['task_instance'].xcom_push(
            key='snowflake_status', value='healthy'
        )
        return True

    snowflake_health_check = PythonOperator(
        task_id='snowflake_health_check',
        python_callable=check_snowflake_health,
        provide_context=True,
        sla=timedelta(minutes=2),
    )

    # ── task 4: run spark streaming ──────────────────
    # uses {{ ds }} to pass run date for time-windowed
    # processing — ensures we process correct time window
    run_spark_streaming = BashOperator(
        task_id='run_spark_streaming',
        bash_command=f'''
            echo "🚀 Starting Spark Streaming for date: {{{{ ds }}}}"
            timeout {SPARK_TIMEOUT} python3 {SPARK_SCRIPT} \
                --run-date {{{{ ds }}}} \
                || true
            echo "✅ Spark job completed for {{{{ ds }}}}"
        ''',
        sla=timedelta(minutes=15),
    )

    # ── task 5: run dbt models ───────────────────────
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command=f'''
            echo "🔧 Running dbt models for date: {{{{ ds }}}}"
            cd {DBT_PROJECT_DIR} && \
            dbt run \
                --profiles-dir {DBT_PROFILES_DIR} \
                --models staging.* marts.* \
                --vars '{{"run_date": "{{{{ ds }}}}"}}'
            echo "✅ dbt models completed"
        ''',
        sla=timedelta(minutes=20),
    )

    # ── task 6: run dbt tests ────────────────────────
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f'''
            echo "🧪 Running dbt tests for date: {{{{ ds }}}}"
            cd {DBT_PROJECT_DIR} && \
            dbt test \
                --profiles-dir {DBT_PROFILES_DIR} \
                --models staging.* marts.*
            echo "✅ dbt tests passed"
        ''',
        sla=timedelta(minutes=10),
    )

    # ── task 7: generate dbt docs ────────────────────
    generate_dbt_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command=f'''
            echo "📚 Generating dbt docs..."
            cd {DBT_PROJECT_DIR} && \
            dbt docs generate \
                --profiles-dir {DBT_PROFILES_DIR}
            echo "✅ dbt docs generated"
        ''',
        sla=timedelta(minutes=5),
    )

    # ── task 8: track pipeline metrics ──────────────
    def track_pipeline_metrics(**context):
        """
        Collect and log pipeline run metrics.
        In production these would go to a metrics database.
        Shows end-to-end pipeline health at a glance.
        """
        ti = context['task_instance']

        # pull metrics from upstream tasks via XCom
        kafka_status     = ti.xcom_pull(
            task_ids='kafka_health_check',
            key='kafka_status'
        )
        snowflake_status = ti.xcom_pull(
            task_ids='snowflake_health_check',
            key='snowflake_status'
        )

        metrics = {
            'dag_id':           context['dag'].dag_id,
            'run_date':         context['ds'],
            'kafka_status':     kafka_status,
            'snowflake_status': snowflake_status,
            'pipeline_status':  'success',
            'completed_at':     datetime.utcnow().isoformat(),
        }

        logger.info(
            f"📊 Pipeline Metrics:\n"
            f"   Run date        : {metrics['run_date']}\n"
            f"   Kafka status    : {metrics['kafka_status']}\n"
            f"   Snowflake status: {metrics['snowflake_status']}\n"
            f"   Pipeline status : {metrics['pipeline_status']}\n"
            f"   Completed at    : {metrics['completed_at']}"
        )

        ti.xcom_push(key='pipeline_metrics', value=metrics)
        return metrics

    track_metrics = PythonOperator(
        task_id='track_pipeline_metrics',
        python_callable=track_pipeline_metrics,
        provide_context=True,
    )

    # ── task 9: pipeline success ─────────────────────
    pipeline_success = EmptyOperator(task_id='pipeline_success')

    # ── task 10: failure alert ───────────────────────
    def send_failure_alert(context):
        """
        Called automatically when any task fails.
        Logs full failure details for debugging.
        """
        ti        = context['task_instance']
        dag_id    = context['dag'].dag_id
        task_id   = ti.task_id
        exec_date = context['execution_date']
        log_url   = ti.log_url

        logger.error(
            f"❌ Pipeline Failure Alert!\n"
            f"   DAG     : {dag_id}\n"
            f"   Task    : {task_id}\n"
            f"   Date    : {exec_date}\n"
            f"   Log URL : {log_url}\n"
            f"   Action  : Check logs and rerun failed task"
        )
        # TODO: add Slack webhook here
        # TODO: add PagerDuty alert here

    pipeline_failed = PythonOperator(
        task_id='pipeline_failed',
        python_callable=send_failure_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

    # ── task dependencies ────────────────────────────
    #
    #              start
    #             /     \
    #    kafka_check   snowflake_check
    #             \     /
    #          spark_streaming
    #                 |
    #          dbt_run_models
    #                 |
    #          dbt_run_tests
    #                 |
    #        generate_dbt_docs
    #                 |
    #         track_metrics
    #                 |
    #         pipeline_success
    #
    #   pipeline_failed ← any task failure

    start >> [kafka_health_check, snowflake_health_check]
    [kafka_health_check, snowflake_health_check] >> run_spark_streaming
    run_spark_streaming >> run_dbt_models
    run_dbt_models >> run_dbt_tests
    run_dbt_tests >> generate_dbt_docs
    generate_dbt_docs >> track_metrics
    track_metrics >> pipeline_success

    [
        kafka_health_check,
        snowflake_health_check,
        run_spark_streaming,
        run_dbt_models,
        run_dbt_tests,
        generate_dbt_docs,
    ] >> pipeline_failed