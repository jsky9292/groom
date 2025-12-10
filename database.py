import os
from datetime import datetime
import pandas as pd
import httpx
import json

# Turso 환경 변수
TURSO_URL = os.environ.get('TURSO_URL', 'libsql://groom-sales-jsky9292.aws-ap-northeast-1.turso.io')
TURSO_AUTH_TOKEN = os.environ.get('TURSO_AUTH_TOKEN', 'eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NjUzNzg5NDcsImlkIjoiZjQyOTdiZTYtMmFmMi00YjU4LTg1ZDItN2JiOTY0ZmFiOTA5IiwicmlkIjoiNmMyZjVjMWQtNzZmYi00Y2Q5LWIxMjgtMjg1NGMwMjM3ODFlIn0.tnaTVre-q_NuyUeHK98QD6xavOtELlvHwHgbGTjMxycrhZLh8hxzVEfUU14JfPsdm3ipRt0sK0YudkKIoJeyAw')

# Turso HTTP API URL 변환
def get_turso_http_url():
    url = TURSO_URL.replace('libsql://', 'https://')
    return url

# 로컬 SQLite 폴백
IS_LOCAL = not os.environ.get('VERCEL', False) and not os.environ.get('USE_TURSO', False)

# sqlite3는 로컬 모드에서만 import
sqlite3 = None
DB_PATH = None

if IS_LOCAL:
    import sqlite3 as _sqlite3
    sqlite3 = _sqlite3
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sales_data.db')

def turso_execute(sql, params=None):
    """Turso HTTP API로 쿼리 실행"""
    url = get_turso_http_url()
    headers = {
        'Authorization': f'Bearer {TURSO_AUTH_TOKEN}',
        'Content-Type': 'application/json'
    }

    # 파라미터 변환 (? -> 배열)
    if params:
        # 튜플을 리스트로 변환하고, 값들을 적절한 형식으로 변환
        args = []
        for p in params:
            if p is None:
                args.append({"type": "null"})
            elif isinstance(p, int) and not isinstance(p, bool):
                args.append({"type": "integer", "value": str(p)})
            elif isinstance(p, float):
                # Turso API는 float 값을 숫자 그대로 보내야 함 (문자열 아님)
                args.append({"type": "float", "value": p})
            else:
                args.append({"type": "text", "value": str(p)})

        body = {
            "requests": [
                {"type": "execute", "stmt": {"sql": sql, "args": args}},
                {"type": "close"}
            ]
        }
    else:
        body = {
            "requests": [
                {"type": "execute", "stmt": {"sql": sql}},
                {"type": "close"}
            ]
        }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(f"{url}/v2/pipeline", headers=headers, json=body)
        response.raise_for_status()
        data = response.json()

    # 결과 파싱
    if 'results' in data and len(data['results']) > 0:
        result = data['results'][0]
        if 'response' in result and 'result' in result['response']:
            res = result['response']['result']
            columns = [col['name'] for col in res.get('cols', [])]
            rows = []
            for row in res.get('rows', []):
                row_data = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, dict):
                        row_data[col] = val.get('value')
                    else:
                        row_data[col] = val
                rows.append(row_data)
            return {
                'columns': columns,
                'rows': rows,
                'last_insert_rowid': res.get('last_insert_rowid', 0),
                'rows_affected': res.get('affected_row_count', 0)
            }
    return {'columns': [], 'rows': [], 'last_insert_rowid': 0, 'rows_affected': 0}

def execute_query(query, params=None):
    """쿼리 실행 (Turso/SQLite 호환)"""
    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        conn.commit()
        conn.close()
        return [dict(row) for row in result]
    else:
        result = turso_execute(query, params)
        return result['rows']

def execute_write(query, params=None):
    """쓰기 쿼리 실행"""
    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        lastrowid = cursor.lastrowid
        conn.commit()
        conn.close()
        return lastrowid
    else:
        result = turso_execute(query, params)
        return result.get('last_insert_rowid', 0)

def init_database():
    """데이터베이스 초기화 - 테이블 생성"""
    queries = [
        # 관리자 계정 테이블
        '''CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''',

        # 업로드 파일 기록 테이블
        '''CREATE TABLE IF NOT EXISTS upload_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            original_name TEXT,
            file_type TEXT,
            row_count INTEGER,
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active'
        )''',

        # 판매 데이터 테이블 (원본)
        '''CREATE TABLE IF NOT EXISTS sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            분류명 TEXT,
            카테고리 TEXT,
            업체명 TEXT,
            상품코드 TEXT,
            바코드 TEXT,
            상품명 TEXT,
            판매일 TEXT,
            주문수 REAL,
            주문건 REAL,
            주문량 REAL,
            판매단가 REAL,
            최종단가 REAL,
            수발주단가 REAL,
            판매가 REAL,
            취소수 REAL,
            취소량 REAL,
            취소금액 REAL,
            할인량 REAL,
            할인금액 REAL,
            판매량 REAL,
            실판매단가 REAL,
            실판매금액 REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''',

        # 월별 판매 데이터 테이블
        '''CREATE TABLE IF NOT EXISTS monthly_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            data_type TEXT,
            판매일자 DATE,
            매장코드 TEXT,
            매장명 TEXT,
            분류명 TEXT,
            카테고리 TEXT,
            업체명 TEXT,
            상품코드 TEXT,
            상품명 TEXT,
            판매일 REAL,
            주문수 REAL,
            주문건 REAL,
            주문량 REAL,
            판매단가 REAL,
            수발주단가 REAL,
            판매가 REAL,
            취소수 REAL,
            취소량 REAL,
            취소금액 REAL,
            할인량 REAL,
            할인금액 REAL,
            판매량 REAL,
            실판매단가 REAL,
            실판매금액 REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''',

        # 인덱스 생성
        'CREATE INDEX IF NOT EXISTS idx_sales_업체명 ON sales_data(업체명)',
        'CREATE INDEX IF NOT EXISTS idx_sales_카테고리 ON sales_data(카테고리)',
        'CREATE INDEX IF NOT EXISTS idx_sales_상품코드 ON sales_data(상품코드)',
        'CREATE INDEX IF NOT EXISTS idx_monthly_판매일자 ON monthly_sales(판매일자)',
        'CREATE INDEX IF NOT EXISTS idx_monthly_매장명 ON monthly_sales(매장명)',
        'CREATE INDEX IF NOT EXISTS idx_monthly_업체명 ON monthly_sales(업체명)'
    ]

    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        # 기본 관리자 계정 생성
        cursor.execute('SELECT COUNT(*) FROM admin_users WHERE username = ?', ('admin',))
        if cursor.fetchone()[0] == 0:
            cursor.execute('INSERT INTO admin_users (username, password) VALUES (?, ?)', ('admin', 'admin123'))
        conn.commit()
        conn.close()
        print(f"데이터베이스 초기화 완료: {DB_PATH}")
    else:
        for query in queries:
            turso_execute(query)
        # 기본 관리자 계정 생성
        result = execute_query('SELECT COUNT(*) as cnt FROM admin_users WHERE username = ?', ('admin',))
        if result and result[0].get('cnt', 0) == 0:
            execute_write('INSERT INTO admin_users (username, password) VALUES (?, ?)', ('admin', 'admin123'))
        print(f"Turso 데이터베이스 초기화 완료: {TURSO_URL}")

def parse_classification(분류명):
    """분류명에서 카테고리와 업체명 추출"""
    if pd.isna(분류명) or not 분류명:
        return None, None

    분류명 = str(분류명).strip()

    if '(' in 분류명 and ')' in 분류명:
        last_open = 분류명.rfind('(')
        last_close = 분류명.rfind(')')

        if last_open < last_close:
            카테고리 = 분류명[:last_open].strip()
            업체명 = 분류명[last_open+1:last_close].strip()
            return 카테고리, 업체명

    return 분류명, None

def clean_numeric(value):
    """숫자 값 정리"""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(',', '').strip())
    except:
        return None

def save_upload_file(filename, original_name, file_type, row_count):
    """업로드 파일 정보 저장"""
    query = '''INSERT INTO upload_files (filename, original_name, file_type, row_count)
               VALUES (?, ?, ?, ?)'''
    return execute_write(query, (filename, original_name, file_type, row_count))

def save_sales_data(df, file_id):
    """원본 판매 데이터 저장"""
    inserted = 0

    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            상품코드 = row.get('상품코드')
            상품명 = str(row.get('상품명', ''))
            if pd.isna(상품코드) or 상품코드 == '' or 'row(s)' in 상품명:
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            cursor.execute('''
                INSERT INTO sales_data (
                    file_id, 분류명, 카테고리, 업체명, 상품코드, 바코드, 상품명,
                    판매일, 주문수, 주문건, 주문량, 판매단가, 최종단가, 수발주단가,
                    판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액,
                    판매량, 실판매단가, 실판매금액
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id, 분류명, 카테고리, 업체명,
                row.get('상품코드'), row.get('바코드'), row.get('상품명'),
                clean_numeric(row.get('판매일')),
                clean_numeric(row.get('주문수')), clean_numeric(row.get('주문건')),
                clean_numeric(row.get('주문량')), clean_numeric(row.get('판매단가')),
                clean_numeric(row.get('최종단가')), clean_numeric(row.get('수발주단가')),
                clean_numeric(row.get('판매가')), clean_numeric(row.get('취소수')),
                clean_numeric(row.get('취소량')), clean_numeric(row.get('취소금액')),
                clean_numeric(row.get('할인량')), clean_numeric(row.get('할인금액')),
                clean_numeric(row.get('판매량')), clean_numeric(row.get('실판매단가')),
                clean_numeric(row.get('실판매금액'))
            ))
            inserted += 1

        conn.commit()
        conn.close()
    else:
        for _, row in df.iterrows():
            상품코드 = row.get('상품코드')
            상품명 = str(row.get('상품명', ''))
            if pd.isna(상품코드) or 상품코드 == '' or 'row(s)' in 상품명:
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            execute_write('''
                INSERT INTO sales_data (
                    file_id, 분류명, 카테고리, 업체명, 상품코드, 바코드, 상품명,
                    판매일, 주문수, 주문건, 주문량, 판매단가, 최종단가, 수발주단가,
                    판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액,
                    판매량, 실판매단가, 실판매금액
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id, 분류명, 카테고리, 업체명,
                row.get('상품코드'), row.get('바코드'), row.get('상품명'),
                clean_numeric(row.get('판매일')),
                clean_numeric(row.get('주문수')), clean_numeric(row.get('주문건')),
                clean_numeric(row.get('주문량')), clean_numeric(row.get('판매단가')),
                clean_numeric(row.get('최종단가')), clean_numeric(row.get('수발주단가')),
                clean_numeric(row.get('판매가')), clean_numeric(row.get('취소수')),
                clean_numeric(row.get('취소량')), clean_numeric(row.get('취소금액')),
                clean_numeric(row.get('할인량')), clean_numeric(row.get('할인금액')),
                clean_numeric(row.get('판매량')), clean_numeric(row.get('실판매단가')),
                clean_numeric(row.get('실판매금액'))
            ))
            inserted += 1

    return inserted

def save_monthly_data(df, file_id, data_type):
    """월별 판매 데이터 저장"""
    inserted = 0

    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            매장명 = row.get('매장명', '')
            if pd.notna(매장명) and '매장수' in str(매장명):
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            판매일자 = row.get('판매일자')
            if pd.notna(판매일자):
                if isinstance(판매일자, str):
                    판매일자 = 판매일자
                else:
                    판매일자 = pd.to_datetime(판매일자).strftime('%Y-%m-%d')
            else:
                판매일자 = None

            cursor.execute('''
                INSERT INTO monthly_sales (
                    file_id, data_type, 판매일자, 매장코드, 매장명, 분류명, 카테고리, 업체명,
                    상품코드, 상품명, 판매일, 주문수, 주문건, 주문량, 판매단가, 수발주단가,
                    판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액,
                    판매량, 실판매단가, 실판매금액
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id, data_type, 판매일자,
                row.get('매장코드'), row.get('매장명'), 분류명, 카테고리, 업체명,
                row.get('상품코드'), row.get('상품명'),
                clean_numeric(row.get('판매일')),
                clean_numeric(row.get('주문수')), clean_numeric(row.get('주문건')),
                clean_numeric(row.get('주문량')), clean_numeric(row.get('판매단가')),
                clean_numeric(row.get('수발주단가')), clean_numeric(row.get('판매가')),
                clean_numeric(row.get('취소수')), clean_numeric(row.get('취소량')),
                clean_numeric(row.get('취소금액')), clean_numeric(row.get('할인량')),
                clean_numeric(row.get('할인금액')), clean_numeric(row.get('판매량')),
                clean_numeric(row.get('실판매단가')), clean_numeric(row.get('실판매금액'))
            ))
            inserted += 1

        conn.commit()
        conn.close()
    else:
        for _, row in df.iterrows():
            매장명 = row.get('매장명', '')
            if pd.notna(매장명) and '매장수' in str(매장명):
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            판매일자 = row.get('판매일자')
            if pd.notna(판매일자):
                if isinstance(판매일자, str):
                    판매일자 = 판매일자
                else:
                    판매일자 = pd.to_datetime(판매일자).strftime('%Y-%m-%d')
            else:
                판매일자 = None

            execute_write('''
                INSERT INTO monthly_sales (
                    file_id, data_type, 판매일자, 매장코드, 매장명, 분류명, 카테고리, 업체명,
                    상품코드, 상품명, 판매일, 주문수, 주문건, 주문량, 판매단가, 수발주단가,
                    판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액,
                    판매량, 실판매단가, 실판매금액
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id, data_type, 판매일자,
                row.get('매장코드'), row.get('매장명'), 분류명, 카테고리, 업체명,
                row.get('상품코드'), row.get('상품명'),
                clean_numeric(row.get('판매일')),
                clean_numeric(row.get('주문수')), clean_numeric(row.get('주문건')),
                clean_numeric(row.get('주문량')), clean_numeric(row.get('판매단가')),
                clean_numeric(row.get('수발주단가')), clean_numeric(row.get('판매가')),
                clean_numeric(row.get('취소수')), clean_numeric(row.get('취소량')),
                clean_numeric(row.get('취소금액')), clean_numeric(row.get('할인량')),
                clean_numeric(row.get('할인금액')), clean_numeric(row.get('판매량')),
                clean_numeric(row.get('실판매단가')), clean_numeric(row.get('실판매금액'))
            ))
            inserted += 1

    return inserted

def get_upload_files():
    """업로드된 파일 목록 조회"""
    return execute_query('''
        SELECT id, filename, original_name, file_type, row_count, upload_date, status
        FROM upload_files
        WHERE status = 'active'
        ORDER BY upload_date DESC
    ''')

def delete_file_data(file_id):
    """파일 및 관련 데이터 삭제"""
    execute_write('DELETE FROM sales_data WHERE file_id = ?', (file_id,))
    execute_write('DELETE FROM monthly_sales WHERE file_id = ?', (file_id,))
    execute_write('UPDATE upload_files SET status = "deleted" WHERE id = ?', (file_id,))

# ============ 통계 조회 함수들 ============

def get_summary_stats():
    """요약 통계"""
    stats = {}

    # 원본 데이터 통계
    result = execute_query('''
        SELECT
            COUNT(*) as total_records,
            SUM(실판매금액) as total_sales,
            SUM(판매량) as total_qty,
            COUNT(DISTINCT 상품코드) as unique_products,
            COUNT(DISTINCT 업체명) as unique_suppliers,
            COUNT(DISTINCT 카테고리) as unique_categories
        FROM sales_data
    ''')
    stats['original'] = result[0] if result else {}

    # 월별 데이터 통계
    result = execute_query('''
        SELECT
            COUNT(*) as total_records,
            SUM(실판매금액) as total_sales,
            SUM(판매량) as total_qty,
            COUNT(DISTINCT 매장명) as unique_stores
        FROM monthly_sales
    ''')
    stats['monthly'] = result[0] if result else {}

    # 데이터 타입별
    result = execute_query('''
        SELECT data_type, COUNT(*) as cnt
        FROM monthly_sales
        GROUP BY data_type
    ''')
    stats['by_type'] = {row['data_type']: row['cnt'] for row in result if row.get('data_type')}

    return stats

def get_sales_by_supplier():
    """업체별 매출"""
    return execute_query('''
        SELECT
            업체명,
            SUM(실판매금액) as 매출액,
            SUM(판매량) as 판매량,
            COUNT(DISTINCT 상품코드) as 상품수
        FROM sales_data
        WHERE 업체명 IS NOT NULL AND 업체명 != ''
        GROUP BY 업체명
        ORDER BY 매출액 DESC
        LIMIT 30
    ''')

def get_sales_by_category():
    """카테고리별 매출"""
    return execute_query('''
        SELECT
            카테고리,
            SUM(실판매금액) as 매출액,
            SUM(판매량) as 판매량,
            COUNT(DISTINCT 상품코드) as 상품수
        FROM sales_data
        WHERE 카테고리 IS NOT NULL AND 카테고리 != ''
        GROUP BY 카테고리
        ORDER BY 매출액 DESC
        LIMIT 30
    ''')

def get_top_products():
    """베스트셀러 상품"""
    return execute_query('''
        SELECT
            상품코드,
            상품명,
            분류명,
            업체명,
            카테고리,
            SUM(실판매금액) as 실판매금액,
            SUM(판매량) as 판매량
        FROM sales_data
        GROUP BY 상품코드, 상품명
        ORDER BY 실판매금액 DESC
        LIMIT 30
    ''')

def get_daily_sales():
    """일별 매출"""
    return execute_query('''
        SELECT
            판매일자,
            SUM(실판매금액) as 실판매금액,
            SUM(판매량) as 판매량,
            COUNT(*) as 건수
        FROM monthly_sales
        WHERE 판매일자 IS NOT NULL
        GROUP BY 판매일자
        ORDER BY 판매일자
    ''')

def get_store_sales():
    """매장별 매출"""
    return execute_query('''
        SELECT
            매장명,
            SUM(실판매금액) as 실판매금액,
            SUM(판매량) as 판매량,
            COUNT(*) as 건수
        FROM monthly_sales
        WHERE 매장명 IS NOT NULL AND 매장명 != ''
        GROUP BY 매장명
        ORDER BY 실판매금액 DESC
        LIMIT 30
    ''')

def get_supplier_category_matrix():
    """업체-카테고리-상품 계층 구조 (드릴다운용)"""
    result = execute_query('''
        SELECT
            업체명,
            카테고리,
            상품코드,
            상품명,
            SUM(실판매금액) as 매출액,
            SUM(판매량) as 판매량
        FROM sales_data
        WHERE 업체명 IS NOT NULL AND 카테고리 IS NOT NULL
        GROUP BY 업체명, 카테고리, 상품코드, 상품명
        ORDER BY 매출액 DESC
    ''')

    # 계층 구조 생성
    hierarchy = {}

    for row in result:
        업체명 = row['업체명']
        카테고리 = row['카테고리']
        상품코드 = row['상품코드']
        상품명 = row['상품명']
        매출액 = float(row['매출액'] or 0)
        판매량 = float(row['판매량'] or 0)

        if 업체명 not in hierarchy:
            hierarchy[업체명] = {
                '업체명': 업체명,
                'total': 0,
                'total_qty': 0,
                'categories': {}
            }

        if 카테고리 not in hierarchy[업체명]['categories']:
            hierarchy[업체명]['categories'][카테고리] = {
                '카테고리': 카테고리,
                'total': 0,
                'total_qty': 0,
                'products': []
            }

        hierarchy[업체명]['categories'][카테고리]['products'].append({
            '상품코드': 상품코드,
            '상품명': 상품명,
            '매출액': 매출액,
            '판매량': 판매량
        })
        hierarchy[업체명]['categories'][카테고리]['total'] += 매출액
        hierarchy[업체명]['categories'][카테고리]['total_qty'] += 판매량
        hierarchy[업체명]['total'] += 매출액
        hierarchy[업체명]['total_qty'] += 판매량

    # 업체 정렬 (총 매출 내림차순)
    sorted_suppliers = sorted(hierarchy.values(), key=lambda x: x['total'], reverse=True)

    # 각 업체 내 카테고리와 상품 정렬
    for supplier in sorted_suppliers:
        categories_list = sorted(supplier['categories'].values(), key=lambda x: x['total'], reverse=True)
        for cat in categories_list:
            cat['products'] = sorted(cat['products'], key=lambda x: x['매출액'], reverse=True)[:20]
        supplier['categories'] = categories_list

    return sorted_suppliers

# ============ 관리자 계정 함수들 ============

def verify_admin(username, password):
    """관리자 로그인 확인"""
    result = execute_query(
        'SELECT * FROM admin_users WHERE username = ? AND password = ?',
        (username, password)
    )
    return result[0] if result else None

def change_password(username, old_password, new_password):
    """비밀번호 변경"""
    result = execute_query(
        'SELECT * FROM admin_users WHERE username = ? AND password = ?',
        (username, old_password)
    )
    if not result:
        return False, "현재 비밀번호가 일치하지 않습니다."

    execute_write('''
        UPDATE admin_users
        SET password = ?, updated_at = CURRENT_TIMESTAMP
        WHERE username = ?
    ''', (new_password, username))

    return True, "비밀번호가 변경되었습니다."

def get_admin_info(username):
    """관리자 정보 조회"""
    result = execute_query(
        'SELECT id, username, created_at, updated_at FROM admin_users WHERE username = ?',
        (username,)
    )
    return result[0] if result else None

# 초기화 실행
if __name__ == '__main__':
    init_database()
    print("데이터베이스 테이블 생성 완료!")
