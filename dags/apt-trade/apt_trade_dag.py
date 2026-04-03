"""
apt_trade_initial_collect_dag.py

매일 자정에 실행해서 미수집된 (lawd_cd x deal_ymd) 조합을 일일 한도(DAILY_LIMIT)만큼 수집한다.

초기 수집이 100% 완료되면 이 DAG는 비활성화화고,
신규 거래 수집용 DAG(별도)로 전환한다
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

# 설정

DAILY_LIMIT = 10000
IS_INITIAL_LOAD = False


default_args = {
    "owner": "jongin",
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False
}


# 함수

def collect_task(**context):
    """
    apt_collector.run_batch()를 호출하고
    결과를 Xcom에 push한다.
    """

    import sys
    import os

    # dags/ 폴더가 PYTHONPATH에 있으면 바로 import 가능
    # 그렇지 않으면 아래처럼 경로 추가
    sys.path.insert(0, os.path.dirname(__file__))

    from collector import run_batch

    stats = run_batch(daily_limit=DAILY_LIMIT)

    # XCom으로 결과 기록 (Airflow UI에서 확인 가능)
    context["ti"].xcom_push(key="stats", value=stats)

    print(f"""
[DAG] 오늘 결과 | 
처리: {stats['processed']} | 
완료: {stats['done']} | 
유지: {stats['same']} | 
스킵: {stats['skip']} | 
실패: {stats['fail']} | 
트래픽 초과: {stats['rate_limited']}
""")

    # 트래픽 초과여도 Task 자체는 성공 처리
    # (내일 자동으로 이어서 실행되도록)
    return stats


def renew_collect_task(**context):
    """기존 데이터 수집 마친 후 매일 새로운 데이터 수집하는 코드"""

    import sys
    import os

    sys.path.insert(0, os.path.dirname(__file__))

    from renew_collector import run_batch

    stats = run_batch(daily_limit=DAILY_LIMIT)

    # XCom으로 결과 기록 (Airflow UI에서 확인 가능)
    context["ti"].xcom_push(key="stats", value=stats)

    print(f"""
[DAG] 오늘 결과 | 
처리: {stats['processed']} | 
완료: {stats['done']} | 
유지: {stats['same']} | 
스킵: {stats['skip']} | 
실패: {stats['fail']} | 
트래픽 초과: {stats['rate_limited']}
""")

    # 트래픽 초과여도 Task 자체는 성공 처리
    # (내일 자동으로 이어서 실행되도록)
    return stats
    

def check_progress_task(**context):
    """
    수집 진행률을 DB에서 조회해서 로그로 출력한다.
    선택적 Task - 진행 상황 모니터링용
    """

    import os
    import sys
    sys.path.insert(0, os.path.dirname(__file__))

    from db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT status, COUNT(*) as cnt
        FROM collection_log
        GROUP BY status
        ORDER BY status
    """)
    rows = cursor.fetchall()

    cursor.execute("SELECT COUNT(DISTINCT lawd_cd) FROM collection_log WHERE status = 'done'")
    done_count = cursor.fetchone()[0]

    dag_dir = os.path.dirname(os.path.abspath(__file__))
    sgg_codes_filename = "sgg_codes_260327.txt"
    sgg_codes_filedir = os.path.join(dag_dir, sgg_codes_filename)

    if os.path.exists(sgg_codes_filedir):
        print("-------------- sgg_codes_filedir 파일 존재! -------------------------")
        with open(sgg_codes_filedir, "r", encoding="utf-8") as f:
            total_lawd_cd_count = len(f.readlines())
    else:
        print("-------------- sgg_codes_filedir 파일 찾을 수 없음 -------------------------")
        total_lawd_cd_count = 254 # 260312 기준, sgg_codes.txt 길이 출력 결과

    cursor.close()
    conn.close()

    print("---- 수집 진행 상황 ----")
    for status, cnt in rows:
        print(f"  {status:10s}: {cnt:,}건")
    print(f"  진행률 (lawd_cd 기준): 총 {total_lawd_cd_count:,}개의 시군구 중 {done_count:,}개 수집 완료, {done_count/total_lawd_cd_count * 100:.1f}% 진행")
    print("----------------------")


def branch_by_mode(**context):
    if IS_INITIAL_LOAD:
        return "collect_batch"
    else:
        return "renew_collect_batch"


# DAG

with DAG(
    dag_id="apt_trade_initial_collect",
    description="아파트 실거래가 데이터 수집 (초기 수집 or 갱신 수집)",
    default_args=default_args,
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 3, 12, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["real_estate", "initial-load"],
    ) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branch_by_mode",
        python_callable=branch_by_mode,
    )

    collect = PythonOperator(
        task_id="collect_batch",
        python_callable=collect_task,
    )
    
    renew_collect = PythonOperator(
        task_id="renew_collect_batch",
        python_callable=renew_collect_task
    )

    check_progress = PythonOperator(
        task_id="check_progress",
        python_callable=check_progress_task,
        trigger_rule="none_failed_min_one_success",    
    )
 
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="apt_trade_dbt",
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",    
    )

    end = EmptyOperator(task_id="end")

    start >> branch
    branch >> collect >> check_progress >> trigger_dbt >> end
    branch >> renew_collect >> check_progress >> trigger_dbt >> end
