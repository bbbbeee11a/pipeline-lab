from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen, Request

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_kobis_weekly(**context):
    api_key = os.environ.get("KOBIS_API_KEY")
    if not api_key:
        raise RuntimeError("KOBIS_API_KEY env not set")

    # KOBIS weekly box office는 보통 주간 종료일(YYYYMMDD)을 targetDt로 받음
    # 실행일 기준으로 "어제"를 넣되, 운영에서는 월요일마다 지난주 종료일로 맞추는 식으로 조정 가능
    run_dt = context["data_interval_end"]  # pendulum datetime
    target_dt = (run_dt - timedelta(days=1)).strftime("%Y%m%d")

    base_url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json"
    params = {
        "key": api_key,
        "targetDt": target_dt,
        # 필요하면 주간/주중/주말, 한국/외국 등 옵션 추가 가능
        # "weekGb": "0",
        # "repNationCd": "K",
    }

    url = f"{base_url}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "pipeline-lab-airflow"})
    with urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")

    data = json.loads(raw)

    out_dir = "/opt/airflow/data/raw/kobis/weekly"
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, f"weekly_boxoffice_targetDt={target_dt}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 간단히 XCom에 파일 경로 남겨서 다음 태스크(변환/적재)에서 재사용 가능
    return {"targetDt": target_dt, "path": out_path}


with DAG(
    dag_id="kobis_weekly_raw",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 일단 수동 실행, 나중에 "0 6 * * MON" 같은 크론으로 바꾸자
    catchup=False,
    tags=["kobis", "raw"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_weekly_boxoffice_raw",
        python_callable=fetch_kobis_weekly,
    )
