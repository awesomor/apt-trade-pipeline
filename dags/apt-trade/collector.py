import itertools
import requests
import os
import math
import pytz
import oracledb
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET

BASE_DIR = os.path.dirname(__file__)
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
SGG_API_URL = os.getenv("SGG_API_URL")

SGG_FILENAME = "sgg_codes_260327.txt"  # sgg_codes_260327.txt는 새로 만든 시군구 코드. 500개가 넘는 전체 5자리 시군구 코드 중 7개의 연월(2006~2024 중 선택함)에 대해 totalCount가 0이 아닌 코드를 다시 선별함. 기존에 코드를 사용했을 땐 230여개? 코드 중 하위 코드가 존재하는 걸 검토해서 걸러내는 식으로 정제하면 215개 정도로 줄었었는데, 새 코드들은 총 254개로 17% 정도 늠.


def get_connection():
    return oracledb.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dsn=os.getenv("DB_DSN"),
        config_dir=os.getenv("DB_CONFIG_DIR"),
        wallet_location=os.getenv("DB_WALLET_LOCATION"),
        wallet_password=os.getenv("DB_WALLET_PASSWORD"),
    )

# 시군구
def get_sgg_codes(file_name=os.path.join(BASE_DIR, SGG_FILENAME)):
    if os.path.exists(file_name):
        print(f"{file_name} 파일 이미 존재. 지역 코드 수집 과정 생략")
        with open(file_name, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    # 시군구 코드 파일 존재하지 않을 시, sgg 코드 제공 API 주소로부터 수집하는 절차
    print(f"파일명 {SGG_FILENAME} 없음. API에서 수집시작.")
    api_total = 49861 # 아마 10자리의 법정동 코드 총 개수일걸. 그 중 앞 5자리가 시군구 코드이므로 우선 불러와서 정제하는 식
    PER_PAGE = 1000
    pages = math.ceil(api_total / PER_PAGE)
    all_data = []

    for page in range(1, pages + 1):
        res = requests.get(
            SGG_API_URL,
            params={"serviceKey": API_KEY, "page": page, "perPage": PER_PAGE},
        )
        all_data.extend(res.json()["data"])

    # 시군구 단위 후보 추출 (기존 로직 동일)
    candidates = [
        str(d["법정동코드"])[:5]
        for d in all_data
        if str(d["법정동코드"]).endswith("00000")
        and not str(d["법정동코드"]).startswith("00000")
        and len(str(d["법정동코드"])) == 10
        and str(d["법정동코드"])[2:5] != "000"
        and d["폐지여부"] == "존재"
    ]

    # 하위 구 코드가 존재하는 시(市) 코드 제거
    # ex) 41110(수원시)은 41111,41113... 이 있으므로 제거, 41111~은 유지
    candidate_set = set(candidates)
    has_children = {
        code for code in candidate_set
        if any(
            other != code and other.startswith(code[:4])
            for other in candidate_set
        )
    }

    sgg = sorted(candidate_set - has_children)

    with open(file_name, "w", encoding="utf-8") as f:
        f.write("\n".join(sgg) + "\n")

    print(f"총 {len(sgg)}개 시군구 코드 저장 완료.")
    return sgg


def get_all_yearmonths(start="200601"): # 아파트 실거래 데이터 공개법이 2006년부터 시행되어 200601로 고정하는 걸로 암
    result = []
    current = datetime.strptime(start, "%Y%m")
    now = datetime.now().replace(day=1)
    while current <= now:
        result.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)
    return result


def get_remaining_targets(conn, all_targets):
    """
    전체 대상에서 이미 done인 것을 제외한 목록만 반환
    all targets: [(lawd_cd, deal_ymd), ...] 형태의 리스트
    """
    cursor = conn.cursor()

    # 임시 테이블 활용
    cursor.execute("""
            SELECT lawd_cd, deal_ymd
            FROM collection_log
            WHERE (status = 'done' OR status = 'empty')
            """)
    done_set = set((row[0], row[1]) for row in cursor.fetchall())

    remaining = [(lawd_cd, deal_ymd) for lawd_cd, deal_ymd in all_targets if (lawd_cd, deal_ymd) not in done_set]
    return remaining

# API 호출

class RateLimitExceeded(Exception):
    """일일 트래픽 초과 시 발생. 상위 호출자가 수집을 중단하도록 신호."""
    pass

def fetch_api(lawd_cd, deal_ymd, page=1, num_of_rows=10000):
    params = {
        "serviceKey": API_KEY,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "numOfRows": num_of_rows,
        "pageNo": page,
    }

    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        result_code = root.findtext(".//resultCode")

        if result_code in ("22", "99", "30"):
            raise RateLimitExceeded(f"API result_code={result_code}")

        if result_code not in ("00", "000"):
            print(f"[WARN] 예상치 못한 result_code={result_code} ({lawd_cd} {deal_ymd})")
            return None, 0

        items = root.findall(".//item")
        total_count = int(root.findtext(".//totalCount") or 0)
        return items, total_count

    except RateLimitExceeded:
        raise

    except requests.exceptions.HTTPError as e:
        if resp.status_code == 429:
            raise RateLimitExceeded("HTTP 429")
        print(f"[ERROR] HTTPError {lawd_cd} {deal_ymd}: {e}")
        return None, 0

    except Exception as e:
        print(f"[ERROR] API 호출 실패 {lawd_cd} {deal_ymd}: {e}")
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
    cursor.execute("""
        MERGE INTO collection_log cl
        USING (SELECT :lawd_cd lawd_cd, :deal_ymd deal_ymd FROM dual) src
        ON (cl.lawd_cd = src.lawd_cd AND cl.deal_ymd = src.deal_ymd)
        WHEN MATCHED THEN
            UPDATE SET status = :status, row_count = :row_count, collected_at = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (lawd_cd, deal_ymd, status, row_count)
            VALUES (:lawd_cd, :deal_ymd, :status, :row_count)
    """, {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": status, "row_count": row_count})


def collect_one(conn, lawd_cd, deal_ymd):
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
    cursor = conn.cursor()

    cursor.execute(
        "SELECT status FROM collection_log WHERE lawd_cd=:lawd_cd AND deal_ymd=:deal_ymd",
        {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
    row = cursor.fetchone()
    if row and row[0] in ("done", "empty"):
        cursor.close()
        return "skip"

    all_rows = []
    page = 1
    while True:
        items, total_count = fetch_api(lawd_cd, deal_ymd, page=page) # RateLimitExceeded는 그대로 전파
        if items is None:
            print(f"\nitems가 None인 관계로 접습니다.\n")
            upsert_log(cursor, lawd_cd, deal_ymd, "fail", 0)
            conn.commit()
            cursor.close()
            return "fail"

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

    cursor.executemany("""
        INSERT INTO apt_trade_raw
            (lawd_cd, deal_ymd, apt_nm, deal_amount, deal_year, deal_month, deal_day,
            area, floor, build_year, jibun, dong)
        VALUES
            (:lawd_cd, :deal_ymd, :apt_nm, :deal_amount, :deal_year, :deal_month, :deal_day,
            :area, :floor, :build_year, :jibun, :dong)
    """, all_rows)

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
    yearmonths = get_all_yearmonths("200601")
    sgg_yms = list(itertools.product(sgg, yearmonths))

    stats = {"processed": 0, "done": 0, "skip": 0, "fail": 0, "empty": 0, "rate_limited": False}
    conn = get_connection()
    remaining_sgg_yms =  get_remaining_targets(conn, sgg_yms)

    print(f"""======================
[MY_LOG] 시각 {datetime.now(tz=pytz.timezone('Asia/Seoul')).strftime('%Y.%m.%d %H:%M')}.
함수 collector.run_batch() 실행.
시군구 코드 : {len(sgg)}개, 총 수집기간 : {len(yearmonths)}년월 ({datetime.strptime(min(yearmonths), "%Y%m").strftime("%Y.%m")} ~ {datetime.strptime(max(yearmonths), "%Y%m").strftime("%Y.%m")})
총 시군구 코드, 년월 쌍 : {len(sgg) * len(yearmonths)}개
중 {len(sgg) * len(yearmonths) - len(remaining_sgg_yms)}개 기수집, {len(remaining_sgg_yms)}개 수집 시작
============================""")

    try:
        for lawd_cd, ym in remaining_sgg_yms:
            if stats["done"] >= daily_limit:
                print(f"[BATCH] 일일 한도 {daily_limit}건 도달. 종료.")
                return stats

            try:
                result = collect_one(conn, lawd_cd, ym)
            except RateLimitExceeded as e:
                print(f"[BATCH] 트래픽 초과 감지: {e}")
                stats["rate_limited"] = True
                return stats


            stats["processed"] += 1

            if result == "skip":
                stats["skip"] += 1
                if stats["skip"] % 100 == 0:
                    print(f"[{stats['processed']:>6}] {lawd_cd} {ym} -> skip")
            elif result.startswith("done"):
                stats["done"] += 1
                print(f"[{stats['processed']:>6}] 법정동코드: {lawd_cd}, 연월: {ym} -> {result} / (총 done: {stats['done']}) ({stats['done'] / daily_limit * 100:.1f}% 진행)")
            elif result == "fail":
                stats["fail"] += 1
                print(f"[{stats['processed']:>6}] {lawd_cd} {ym} -> FAIL")
            elif result == "empty":
                stats["empty"] += 1
                print(f"[{stats['processed']:>6}] {lawd_cd} {ym} ->EMPTY")

    finally:
        conn.close()

    print(f"[BATCH] 완료: {stats}")
    return stats


if __name__ == "__main__":
    result = run_batch(daily_limit=10000)
    print(result)
