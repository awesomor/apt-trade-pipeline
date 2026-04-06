import itertools
import requests
import os
import math
import xml.etree.ElementTree as ET
import logging
import pytz
from db import get_connection
from config import BASE_DIR, API_KEY, API_URL, DATA_DIR, SGG_FILENAME
from datetime import datetime
from dateutil.relativedelta import relativedelta


TIME_RANGE = 1 # 현재 기준 몇 달 전까지 갱신 범위로 잡을 지 (단위: 월)

# logger Class 생성
class ExtraFormatter(logging.Formatter):
    def format(self, record):
        # 기본 메세지 생성
        msg = super().format(record)

        # 기본 속성(asctime, name 등)을 제외한 커스텀 extra 데이터만 추출
        default_attrs = logging.LogRecord(None, None, None, None, None, None, None).__dict__.keys()
        extra_data = {k: v for k, v in record.__dict__.items() if k not in default_attrs and k != "message"}
    
        if extra_data:
            # 딕셔너리 내용을 문자열로 변환해서 뒤에 붙임
            extra_str = " | " + " | ".join([f"{k}:{v}" for k, v in extra_data.items()])
            return msg + extra_str

        return msg


logger = logging.getLogger(__name__)
logger.propagate = False

handler = logging.StreamHandler()
handler.setFormatter(ExtraFormatter('%(asctime)s [%(levelname)s] %(message)s'))

logger.addHandler(handler)
 
# 시군구 코드 파일로부터 조회, 파일 없으면 빈 리스트 반환
def get_sgg_codes(file_name=os.path.join(DATA_DIR, SGG_FILENAME)):
    try:
        with open(file_name, "r", encoding="utf-8") as f:
            logger.info("시군구 파일 확인. 파일로부터 수직 시작", extra={"file": file_name})
            return [line.strip() for line in f if line.strip()]

    except FileNotFoundError:
        logger.warning("시군구 파일 확인 불가. 빈 리스트 반환", extra={"file": file_name})
        return []

    except PermissionError:
        logger.error("파일 접근 권한이 없습니다.", extra={"file": file_name})
        return []

    except Exception as e:
        logger.error("알 수 없는 오류 발생: %s", str(e), exc_info=True, extra={"file": file_name})
        return []

# TIME_RANGE 로부터 갱신하는 수집 기간 리스트를 만드는 함수
def set_daterange():
    result = []
    now = datetime.now().replace(day=1)
    start = now - relativedelta(months=TIME_RANGE)
 
    while start <= now:
        result.append(start.strftime("%Y%m"))
        start += relativedelta(months=1)

    return result

def check_and_increment_api_count(conn):
    """DB에서 오늘 날짜의 호출 횟수를 1 증가시키고 현재 값을 반환"""
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 오늘 날짜 행이 없으면 생성, 있으면 count + 1 (MERGE 문 활용)
    cursor.execute("""
        MERGE INTO api_traffic_stats t
        USING DUAL ON (t.call_date = TO_DATE(:today, 'YYYY-MM-DD'))
        WHEN MATCHED THEN
            UPDATE SET t.call_count = t.call_count + 1
        WHEN NOT MATCHED THEN
            INSERT (call_date, call_count) VALUES (TO_DATE(:today, 'YYYY-MM-DD'), 1)
    """, {"today": today})

    # 현재 카운트 조회
    cursor.execute("SELECT call_count FROM api_traffic_stats WHERE call_date = TO_DATE(:today, 'YYYY-MM-DD')", {"today": today})
    current_count = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return current_count



# API 호출
class RateLimitExceeded(Exception):
    """일일 트래픽 초과 시 발생. 상위 호출자가 수집을 중단하도록 신호."""
    pass

def fetch_api(conn, lawd_cd, deal_ymd, page=1, num_of_rows=10000, daily_limit=10000):
    # DB 카운트 증가 및 확인
    current_usage = check_and_increment_api_count(conn)

    if current_usage > daily_limit:
        logger.error("DB 기록 기준 일일 제한 초과")
        raise RateLimitExceeded("Daily API Limit Reached (DB count)")


    params = {
        "serviceKey": API_KEY,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "numOfRows": num_of_rows,
        "pageNo": page,
    }
    extra_content = {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd}    

    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        result_code = root.findtext(".//resultCode")

        if result_code in ("22", "99", "30"):
            raise RateLimitExceeded(f"API result_code={result_code}")
            logger.warning("[WARN] 예상치 못한 result_code: %s", str(result_code), extra=extra_content) 
            return None, 0

        if result_code not in ("00", "000"):
            logger.warning("[WARN] 예상치 못한 result_code: %s", str(result_code), extra=extra_content) 
            return None, 0

        items = root.findall(".//item")
        total_count = int(root.findtext(".//totalCount") or 0)
        return items, total_count

    except RateLimitExceeded:
        raise

    except requests.exceptions.HTTPError as e:
        if resp.status_code == 429:
            raise RateLimitExceeded("HTTP 429")
        logger.error("[ERROR] HTTPError: %s", str(e), exc_info=True, extra=extra_content)
        return None, 0

    except Exception as e:
        logger.error("[ERROR] API 호출 실패: %s", str(e), exc_info=True, extra=extra_content)
        return None, 0

# 파싱 / DB 저장

def parse_item(item, lawd_cd, deal_ymd):
    def txt(tag):
        v = item.findtext(tag)
        return v.strip() if v.strip() else None

    return {
        "lawd_cd": lawd_cd,
        "deal_ymd": deal_ymd,
        "apt_nm": txt("aptNm"),
        "deal_amount": txt("dealAmount"),
        "deal_year": txt("dealYear"),
        "deal_month": txt("dealMonth"),
        "deal_day": txt("dealDay"),
        "area": txt("excluUseAr"),
        "floor": txt("floor"),
        "build_year": txt("buildYear"),
        "jibun": txt("jibun"),
        "dong": txt("aptDong"),
    }

def upsert_log(cursor, lawd_cd, deal_ymd, status, row_count):
    logger.debug("collection_log에 log 추가", extra={"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": status, "row_count": row_count})
    cursor.execute("""
        INSERT INTO collection_log (lawd_cd, deal_ymd, status, row_count)
        VALUES (:lawd_cd, :deal_ymd, :status, :row_count)
""", {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": status, "row_count": row_count})



def collect_one(conn, lawd_cd, deal_ymd, daily_limit):
    """
    단일 (lawd_cd, deal_ymd) 조합을 수집

    Returns:
        "skip"      - 이미 수집 완료
        "empty"     - API 응답은 있으나 거래 데이터 없음
        "done(N)"   - N건 저장 완료
        "fail"      - API 오류

    Raise:
        RateLimitExceeded - 일일 한도 초과 시 (호출자가 수집 중단해야 함)
    """
    # logger.info("[START] collect_one 실행", extra={"lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
    cursor = conn.cursor()

    cursor.execute("""
        SELECT row_count 
        FROM collection_log 
        WHERE lawd_cd=:lawd_cd AND deal_ymd=:deal_ymd
        ORDER BY collected_at DESC
        FETCH FIRST 1 ROW ONLY
    """, {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
    row = cursor.fetchone()
    
    try:
        row_count = row[0]
    except Exception as e:
        row_count = 0
        logger.error("", exc_info=False, extra={"e": e})

    all_rows = []
    page = 1

    while True:
        items, total_count = fetch_api(conn, lawd_cd, deal_ymd, page=page, daily_limit=daily_limit) # RateLimitExceeded는 그대로 전파
        if items is None:
            logger.warning("[WARN] items가 None으로 fail 처리", extra={"total_count": total_count, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
            upsert_log(cursor, lawd_cd, deal_ymd, "fail", 0)
            conn.commit()
            cursor.close()
            return "fail"

        if total_count <= row_count:
            logger.info("변동없는 데이터로 업데이트 하지 않음", extra={"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "total_count": total_count, "row_count": row_count})
            upsert_log(cursor, lawd_cd, deal_ymd, "same", row_count)
            conn.commit()
            cursor.close()
            return "same"

        for item in items:
            all_rows.append(parse_item(item, lawd_cd, deal_ymd))

        if len(all_rows) >= total_count or not items:
            break
        page += 1

    if not all_rows:
        upsert_log(cursor, lawd_cd, deal_ymd, "empty", 0)
        conn.commit()
        cursor.close()
        return "empty"
    
    cursor.execute("""
        DELETE FROM apt_trade_raw
        WHERE lawd_cd = :lawd_cd AND deal_ymd = :deal_ymd
        """, {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
        

    cursor.executemany("""
        INSERT INTO apt_trade_raw
            (lawd_cd, deal_ymd, apt_nm, deal_amount, deal_year, deal_month, deal_day,
            area, floor, build_year, jibun, dong)
        VALUES
            (:lawd_cd, :deal_ymd, :apt_nm, :deal_amount, :deal_year, :deal_month, :deal_day,
            :area, :floor, :build_year, :jibun, :dong)
    """, all_rows)

    logger.info("데이터 업데이트 완료. 기존 %s건 삭제 후 %s 업로드 완료", str(row_count), str(total_count), extra={"lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
    upsert_log(cursor, lawd_cd, deal_ymd, "done", len(all_rows))
    conn.commit()
    cursor.close()
    return f"done({len(all_rows)})"


# 배치 실행

def run_batch(daily_limit: int = 10000, target_ym: str = None):
    """
    미수집 조합을 daily_limit 건만큼 처리
    Airflow PythonOperator의 callable로 사용

    Return:
        dict: {"processed": N, "done": N, "skip": N, "fail": N, "rate_limited": bool}
    """

    sgg = get_sgg_codes()
    yearmonths = set_daterange()
    sgg_yms = list(itertools.product(sgg, yearmonths))

    stats = {"processed": 0, "done": 0, "same": 0, "skip": 0, "fail": 0, "empty": 0, "rate_limited": False}
    conn = get_connection()
    logger.info(f"""
============================================
============================================
============================================
[START] run_batch 실행
시군구 코드 : {len(sgg)}개, 총 수집기간 : {len(yearmonths)}개월 ({datetime.strptime(min(yearmonths), "%Y%m").strftime("%Y.%m")} ~ {datetime.strptime(max(yearmonths), "%Y%m").strftime("%Y.%m")})
수집 대상 (시군구, 연월) : {len(sgg) * len(yearmonths)}개
============================================
============================================
============================================
""")

    try:
        for idx, (lawd_cd, ym) in enumerate(sgg_yms):
            logger.info(f"===={idx+1}/{len(sgg_yms)}번 진행중.. ({(idx+1) / len(sgg_yms) * 100:.1f}%)====", extra={"lawd_cd": lawd_cd, "deal_ymd": ym})
            try:
                result = collect_one(conn, lawd_cd, ym, daily_limit)
            except RateLimitExceeded as e:
                logger.error("[BATCH] 트래픽 초과 감지", extra={"e": e})
                stats["rate_limited"] = True
                return stats

            stats["processed"] += 1
            logger.info("[RESULT] %s (%s건 진행)", result, str(stats["processed"]))

            if result == "skip":
                stats["skip"] += 1
            elif result.startswith("done"):
                stats["done"] += 1
            elif result == "fail":
                stats["fail"] += 1
            elif result == "empty":
                stats["empty"] += 1
            elif result == "same":
                stats["same"] += 1

    finally:
        conn.close()

    logger.info(f"[BATCH] 완료: {stats}")
    return stats


if __name__ == "__main__":
    result = run_batch(daily_limit=10000)
    print(result)
