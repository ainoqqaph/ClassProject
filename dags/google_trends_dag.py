from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta

# ==========================================
# 1. 設定 DAG 的預設參數
# ==========================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # 失敗時重試 1 次
    'retry_delay': timedelta(minutes=3),
}

# ==========================================
# 2. 定義 DAG (有向無環圖)
# ==========================================
local_tz = pendulum.timezone("Asia/Taipei")

with DAG(
    dag_id='google_trends_master_pipeline', 
    default_args=default_args,
    description='每日自動化：Google Trends 爬蟲 ➔ AI 深度學習 ➔ 摘要補抓',
    schedule_interval='0 9 * * *', # 早上 09:00 執行
    start_date=pendulum.datetime(2024, 3, 25, tz=local_tz),
    catchup=False, 
    tags=['trends', 'crawler', 'AI'],
) as dag:

    SCRIPT_DIR = '/opt/airflow/dags'

    # Task 1: 執行主爬蟲程式 (抓取熱搜 + 基礎 AI 分類)
    t1_run_crawler = BashOperator(
        task_id='task1_google_trends_crawler',
        bash_command=f'cd {SCRIPT_DIR} && python google_trends_automation_headless.py',
    )

    # Task 2: 執行 AI 規則學習程式 (處理 Other 未知詞彙)
    t2_run_ai_learner = BashOperator(
        task_id='task2_ai_keyword_learner',
        bash_command=f'cd {SCRIPT_DIR} && python Aikeyword.py',
    )

    # Task 3: 執行摘要補抓程式
    t3_run_log_backfill = BashOperator(
        task_id='task3_search_log_backfill',
        bash_command=f'cd {SCRIPT_DIR} && python search_logs.py',
    )

    # ==========================================
    # 3. 設定任務執行順序 (Dependencies)
    # ==========================================
    t1_run_crawler >> t2_run_ai_learner >> t3_run_log_backfill