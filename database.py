import os
from datetime import datetime
import pandas as pd
import httpx
import json

# Supabase 환경 변수 (strip으로 공백/줄바꿈 제거)
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xhjiokshzesnrmzppkwd.supabase.co').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhoamlva3NoemVzbnJtenBwa3dkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU0NDcwMjEsImV4cCI6MjA4MTAyMzAyMX0.pbmUsbrpM_91aPPIGw-trcXBLPGLZrPedcvVwcYbEHc').strip()

# 로컬 SQLite 폴백 (Vercel 환경변수는 문자열 '1'로 설정됨)
IS_LOCAL = not os.environ.get('VERCEL') and not os.environ.get('USE_SUPABASE')

# sqlite3는 로컬 모드에서만 import
sqlite3 = None
DB_PATH = None

if IS_LOCAL:
    import sqlite3 as _sqlite3
    sqlite3 = _sqlite3
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sales_data.db')

def get_supabase_headers():
    """Supabase API 헤더"""
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
    }

def supabase_select(table, columns='*', filters=None, order=None, limit=None):
    """Supabase REST API로 SELECT 쿼리 - 페이지네이션으로 전체 데이터 조회"""
    all_data = []
    page_size = 1000  # Supabase 기본 최대값
    offset = 0

    # limit이 지정되면 해당 수만큼만, 아니면 전체 조회
    target_count = limit if limit else float('inf')

    with httpx.Client(timeout=60.0) as client:
        while len(all_data) < target_count:
            url = f"{SUPABASE_URL}/rest/v1/{table}?select={columns}"

            if filters:
                url += f"&{filters}"
            if order:
                url += f"&order={order}"

            # 페이지네이션
            current_limit = min(page_size, int(target_count - len(all_data))) if limit else page_size
            url += f"&limit={current_limit}&offset={offset}"

            response = client.get(url, headers=get_supabase_headers())
            response.raise_for_status()
            data = response.json()

            if not data:  # 더 이상 데이터 없음
                break

            all_data.extend(data)

            if len(data) < page_size:  # 마지막 페이지
                break

            offset += page_size

    return all_data

def supabase_insert(table, data):
    """Supabase REST API로 INSERT (bulk 지원)"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"

    # 대량 데이터는 배치로 분할 (500건씩)
    if isinstance(data, list) and len(data) > 500:
        all_results = []
        for i in range(0, len(data), 500):
            batch = data[i:i+500]
            with httpx.Client(timeout=120.0) as client:
                response = client.post(url, headers=get_supabase_headers(), json=batch)
                if response.status_code >= 400:
                    print(f"Supabase INSERT error: {response.status_code} - {response.text[:500]}")
                    response.raise_for_status()
                result = response.json()
                if isinstance(result, list):
                    all_results.extend(result)
        return all_results

    with httpx.Client(timeout=120.0) as client:
        response = client.post(url, headers=get_supabase_headers(), json=data)
        if response.status_code >= 400:
            print(f"Supabase INSERT error: {response.status_code} - {response.text[:500]}")
            response.raise_for_status()
        return response.json()

def supabase_update(table, data, filters):
    """Supabase REST API로 UPDATE"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{filters}"

    with httpx.Client(timeout=30.0) as client:
        response = client.patch(url, headers=get_supabase_headers(), json=data)
        response.raise_for_status()
        return response.json()

def supabase_delete(table, filters):
    """Supabase REST API로 DELETE"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{filters}"

    with httpx.Client(timeout=30.0) as client:
        response = client.delete(url, headers=get_supabase_headers())
        response.raise_for_status()
        return True

def supabase_rpc(function_name, params=None):
    """Supabase RPC 함수 호출 (집계 쿼리용)"""
    url = f"{SUPABASE_URL}/rest/v1/rpc/{function_name}"

    with httpx.Client(timeout=30.0) as client:
        if params:
            response = client.post(url, headers=get_supabase_headers(), json=params)
        else:
            response = client.post(url, headers=get_supabase_headers())
        response.raise_for_status()
        return response.json()

def execute_query(query, params=None):
    """쿼리 실행 (Supabase/SQLite 호환) - 복잡한 쿼리는 RPC 사용"""
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
        # Supabase에서는 SQL 직접 실행 불가 - REST API 또는 RPC 사용
        # 이 함수는 로컬 테스트용으로 유지
        raise NotImplementedError("Use Supabase REST API or RPC functions")

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
        raise NotImplementedError("Use Supabase REST API")

def init_database():
    """데이터베이스 초기화 - 테이블 생성"""
    if IS_LOCAL:
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
        # Supabase는 대시보드에서 테이블 생성 (SQL Editor)
        # 기본 관리자 계정 확인
        try:
            result = supabase_select('admin_users', '*', 'username=eq.admin')
            if not result:
                supabase_insert('admin_users', {'username': 'admin', 'password': 'admin123'})
            print(f"Supabase 데이터베이스 연결 완료: {SUPABASE_URL}")
        except Exception as e:
            print(f"Supabase 초기화 중 오류 (테이블 생성 필요할 수 있음): {e}")

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

def convert_excel_date(value):
    """Excel 시리얼 날짜를 문자열 날짜로 변환
    Excel은 1900-01-01을 1로 시작하는 시리얼 넘버로 날짜를 저장
    (실제로는 1899-12-30 기준, Excel의 1900 윤년 버그 때문)
    """
    if pd.isna(value) or value is None:
        return None

    # 이미 문자열 형태의 날짜인 경우
    if isinstance(value, str):
        value = value.strip()
        # YYYY-MM-DD 또는 YYYY/MM/DD 형식인지 확인
        if len(value) >= 10 and (value[4] == '-' or value[4] == '/'):
            return value[:10].replace('/', '-')
        # 다른 문자열 형식 시도
        try:
            parsed = pd.to_datetime(value)
            return parsed.strftime('%Y-%m-%d')
        except:
            return None

    # datetime 객체인 경우
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')

    # pandas Timestamp인 경우
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d')

    # 숫자형 (Excel 시리얼 넘버) 인 경우
    if isinstance(value, (int, float)):
        try:
            # Excel 시리얼 넘버 범위 체크 (1 ~ 2958465는 1900-01-01 ~ 9999-12-31)
            # 일반적인 날짜 범위: 25569 (1970-01-01) ~ 50000 (2036년경)
            if 1 < value < 100000:  # Excel 날짜 범위
                # Excel 기준일: 1899-12-30 (Excel의 1900 윤년 버그 보정)
                excel_base = datetime(1899, 12, 30)
                result_date = excel_base + pd.Timedelta(days=int(value))
                return result_date.strftime('%Y-%m-%d')
            else:
                # Unix timestamp로 시도
                return pd.to_datetime(value, unit='s').strftime('%Y-%m-%d')
        except:
            return None

    return None

def save_upload_file(filename, original_name, file_type, row_count):
    """업로드 파일 정보 저장"""
    if IS_LOCAL:
        query = '''INSERT INTO upload_files (filename, original_name, file_type, row_count)
                   VALUES (?, ?, ?, ?)'''
        return execute_write(query, (filename, original_name, file_type, row_count))
    else:
        result = supabase_insert('upload_files', {
            'filename': filename,
            'original_name': original_name,
            'file_type': file_type,
            'row_count': row_count
        })
        return result[0]['id'] if result else 0

def save_sales_data(df, file_id):
    """원본 판매 데이터 저장 - Supabase bulk insert로 초고속"""
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
        # Supabase bulk insert - 1000건씩 한 번에 (초고속!)
        batch_size = 1000
        batch_data = []

        for _, row in df.iterrows():
            상품코드 = row.get('상품코드')
            상품명 = str(row.get('상품명', ''))
            if pd.isna(상품코드) or 상품코드 == '' or 'row(s)' in 상품명:
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            batch_data.append({
                'file_id': file_id,
                '분류명': str(분류명) if 분류명 else None,
                '카테고리': 카테고리,
                '업체명': 업체명,
                '상품코드': str(row.get('상품코드', '')) if pd.notna(row.get('상품코드')) else None,
                '바코드': str(row.get('바코드', '')) if pd.notna(row.get('바코드')) else None,
                '상품명': str(row.get('상품명', '')) if pd.notna(row.get('상품명')) else None,
                '판매일': clean_numeric(row.get('판매일')),
                '주문수': clean_numeric(row.get('주문수')),
                '주문건': clean_numeric(row.get('주문건')),
                '주문량': clean_numeric(row.get('주문량')),
                '판매단가': clean_numeric(row.get('판매단가')),
                '최종단가': clean_numeric(row.get('최종단가')),
                '수발주단가': clean_numeric(row.get('수발주단가')),
                '판매가': clean_numeric(row.get('판매가')),
                '취소수': clean_numeric(row.get('취소수')),
                '취소량': clean_numeric(row.get('취소량')),
                '취소금액': clean_numeric(row.get('취소금액')),
                '할인량': clean_numeric(row.get('할인량')),
                '할인금액': clean_numeric(row.get('할인금액')),
                '판매량': clean_numeric(row.get('판매량')),
                '실판매단가': clean_numeric(row.get('실판매단가')),
                '실판매금액': clean_numeric(row.get('실판매금액'))
            })

            if len(batch_data) >= batch_size:
                try:
                    supabase_insert('sales_data', batch_data)
                    inserted += len(batch_data)
                except Exception as e:
                    print(f"Bulk insert error: {e}")
                batch_data = []

        # 남은 데이터 처리
        if batch_data:
            try:
                supabase_insert('sales_data', batch_data)
                inserted += len(batch_data)
            except Exception as e:
                print(f"Bulk insert error: {e}")

    return inserted

def save_monthly_data(df, file_id, data_type):
    """월별 판매 데이터 저장 - Supabase bulk insert로 초고속"""
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

            # Excel 시리얼 날짜 변환 적용
            판매일자 = convert_excel_date(row.get('판매일자'))

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
        # Supabase bulk insert - 1000건씩 한 번에 (초고속!)
        batch_size = 1000
        batch_data = []

        for _, row in df.iterrows():
            매장명 = row.get('매장명', '')
            if pd.notna(매장명) and '매장수' in str(매장명):
                continue

            분류명 = row.get('분류명', '')
            카테고리, 업체명 = parse_classification(분류명)

            # Excel 시리얼 날짜 변환 적용
            판매일자_str = convert_excel_date(row.get('판매일자'))

            batch_data.append({
                'file_id': file_id,
                'data_type': data_type,
                '판매일자': 판매일자_str,
                '매장코드': str(row.get('매장코드', '')) if pd.notna(row.get('매장코드')) else None,
                '매장명': str(row.get('매장명', '')) if pd.notna(row.get('매장명')) else None,
                '분류명': str(분류명) if 분류명 else None,
                '카테고리': 카테고리,
                '업체명': 업체명,
                '상품코드': str(row.get('상품코드', '')) if pd.notna(row.get('상품코드')) else None,
                '상품명': str(row.get('상품명', '')) if pd.notna(row.get('상품명')) else None,
                '판매일': clean_numeric(row.get('판매일')),
                '주문수': clean_numeric(row.get('주문수')),
                '주문건': clean_numeric(row.get('주문건')),
                '주문량': clean_numeric(row.get('주문량')),
                '판매단가': clean_numeric(row.get('판매단가')),
                '수발주단가': clean_numeric(row.get('수발주단가')),
                '판매가': clean_numeric(row.get('판매가')),
                '취소수': clean_numeric(row.get('취소수')),
                '취소량': clean_numeric(row.get('취소량')),
                '취소금액': clean_numeric(row.get('취소금액')),
                '할인량': clean_numeric(row.get('할인량')),
                '할인금액': clean_numeric(row.get('할인금액')),
                '판매량': clean_numeric(row.get('판매량')),
                '실판매단가': clean_numeric(row.get('실판매단가')),
                '실판매금액': clean_numeric(row.get('실판매금액'))
            })

            if len(batch_data) >= batch_size:
                try:
                    supabase_insert('monthly_sales', batch_data)
                    inserted += len(batch_data)
                except Exception as e:
                    print(f"Bulk insert error: {e}")
                batch_data = []

        # 남은 데이터 처리
        if batch_data:
            try:
                supabase_insert('monthly_sales', batch_data)
                inserted += len(batch_data)
            except Exception as e:
                print(f"Bulk insert error: {e}")

    return inserted

def get_upload_files():
    """업로드된 파일 목록 조회"""
    if IS_LOCAL:
        return execute_query('''
            SELECT id, filename, original_name, file_type, row_count, upload_date, status
            FROM upload_files
            WHERE status = 'active'
            ORDER BY upload_date DESC
        ''')
    else:
        return supabase_select('upload_files', '*', 'status=eq.active', 'upload_date.desc')

def delete_file_data(file_id):
    """파일 및 관련 데이터 삭제"""
    if IS_LOCAL:
        execute_write('DELETE FROM sales_data WHERE file_id = ?', (file_id,))
        execute_write('DELETE FROM monthly_sales WHERE file_id = ?', (file_id,))
        execute_write('UPDATE upload_files SET status = "deleted" WHERE id = ?', (file_id,))
    else:
        supabase_delete('sales_data', f'file_id=eq.{file_id}')
        supabase_delete('monthly_sales', f'file_id=eq.{file_id}')
        supabase_update('upload_files', {'status': 'deleted'}, f'id=eq.{file_id}')

# ============ 통계 조회 함수들 ============

def get_summary_stats(file_id=None):
    """요약 통계 (file_id로 필터링 가능)"""
    stats = {}

    # 파일 필터 조건
    file_filter = f"WHERE file_id = {file_id}" if file_id else ""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""

    if IS_LOCAL:
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
    else:
        # Supabase - file_id 필터링 적용
        try:
            if file_id:
                sales = supabase_select('sales_data', '*', f'file_id=eq.{file_id}')
                monthly = supabase_select('monthly_sales', '*', f'file_id=eq.{file_id}')
            else:
                sales = supabase_select('sales_data', '*')
                monthly = supabase_select('monthly_sales', '*')

            stats['original'] = {
                'total_records': len(sales),
                'total_sales': sum(float(r.get('실판매금액') or 0) for r in sales),
                'total_qty': sum(float(r.get('판매량') or 0) for r in sales),
                'unique_products': len(set(r.get('상품코드') for r in sales if r.get('상품코드'))),
                'unique_suppliers': len(set(r.get('업체명') for r in sales if r.get('업체명'))),
                'unique_categories': len(set(r.get('카테고리') for r in sales if r.get('카테고리')))
            }
            stats['monthly'] = {
                'total_records': len(monthly),
                'total_sales': sum(float(r.get('실판매금액') or 0) for r in monthly),
                'total_qty': sum(float(r.get('판매량') or 0) for r in monthly),
                'unique_stores': len(set(r.get('매장명') for r in monthly if r.get('매장명')))
            }
            type_counts = {}
            for r in monthly:
                dt = r.get('data_type')
                if dt:
                    type_counts[dt] = type_counts.get(dt, 0) + 1
            stats['by_type'] = type_counts
        except:
            stats = {
                'original': {'total_records': 0, 'total_sales': 0, 'total_qty': 0, 'unique_products': 0, 'unique_suppliers': 0, 'unique_categories': 0},
                'monthly': {'total_records': 0, 'total_sales': 0, 'total_qty': 0, 'unique_stores': 0},
                'by_type': {}
            }

    return stats

def get_sales_by_supplier(file_id=None):
    """업체별 매출 (file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""

    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                업체명,
                SUM(실판매금액) as 매출액,
                SUM(판매량) as 판매량,
                COUNT(DISTINCT 상품코드) as 상품수
            FROM sales_data
            WHERE 업체명 IS NOT NULL AND 업체명 != '' {file_filter_and}
            GROUP BY 업체명
            ORDER BY 매출액 DESC
            LIMIT 30
        ''')
    else:
        try:
            # file_id 필터링 적용
            if file_id:
                sales = supabase_select('sales_data', '*', f'업체명=not.is.null&file_id=eq.{file_id}')
            else:
                sales = supabase_select('sales_data', '*', '업체명=not.is.null')
            agg = {}
            for r in sales:
                supplier = r.get('업체명')
                if not supplier:
                    continue
                if supplier not in agg:
                    agg[supplier] = {'업체명': supplier, '매출액': 0, '판매량': 0, '상품수': set()}
                agg[supplier]['매출액'] += float(r.get('실판매금액') or 0)
                agg[supplier]['판매량'] += float(r.get('판매량') or 0)
                agg[supplier]['상품수'].add(r.get('상품코드'))
            result = [{'업체명': k, '매출액': v['매출액'], '판매량': v['판매량'], '상품수': len(v['상품수'])} for k, v in agg.items()]
            return sorted(result, key=lambda x: x['매출액'], reverse=True)[:30]
        except:
            return []

def get_sales_by_category(file_id=None):
    """카테고리별 매출 (file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""

    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                카테고리,
                SUM(실판매금액) as 매출액,
                SUM(판매량) as 판매량,
                COUNT(DISTINCT 상품코드) as 상품수
            FROM sales_data
            WHERE 카테고리 IS NOT NULL AND 카테고리 != '' {file_filter_and}
            GROUP BY 카테고리
            ORDER BY 매출액 DESC
            LIMIT 30
        ''')
    else:
        try:
            # file_id 필터링 적용
            if file_id:
                sales = supabase_select('sales_data', '*', f'카테고리=not.is.null&file_id=eq.{file_id}')
            else:
                sales = supabase_select('sales_data', '*', '카테고리=not.is.null')
            agg = {}
            for r in sales:
                cat = r.get('카테고리')
                if not cat:
                    continue
                if cat not in agg:
                    agg[cat] = {'카테고리': cat, '매출액': 0, '판매량': 0, '상품수': set()}
                agg[cat]['매출액'] += float(r.get('실판매금액') or 0)
                agg[cat]['판매량'] += float(r.get('판매량') or 0)
                agg[cat]['상품수'].add(r.get('상품코드'))
            result = [{'카테고리': k, '매출액': v['매출액'], '판매량': v['판매량'], '상품수': len(v['상품수'])} for k, v in agg.items()]
            return sorted(result, key=lambda x: x['매출액'], reverse=True)[:30]
        except:
            return []

def get_top_products(file_id=None):
    """베스트셀러 상품 (file_id로 필터링 가능)"""
    file_filter = f"WHERE file_id = {file_id}" if file_id else ""

    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                상품코드,
                상품명,
                분류명,
                업체명,
                카테고리,
                SUM(실판매금액) as 실판매금액,
                SUM(판매량) as 판매량
            FROM sales_data
            {file_filter}
            GROUP BY 상품코드, 상품명
            ORDER BY 실판매금액 DESC
            LIMIT 30
        ''')
    else:
        try:
            # file_id 필터링 적용 - 전체 데이터 조회 (페이지네이션으로 자동 처리)
            if file_id:
                sales = supabase_select('sales_data', '*', f'file_id=eq.{file_id}')
            else:
                sales = supabase_select('sales_data', '*')
            agg = {}
            for r in sales:
                code = r.get('상품코드')
                if not code:
                    continue
                if code not in agg:
                    agg[code] = {
                        '상품코드': code,
                        '상품명': r.get('상품명'),
                        '분류명': r.get('분류명'),
                        '업체명': r.get('업체명'),
                        '카테고리': r.get('카테고리'),
                        '실판매금액': 0,
                        '판매량': 0
                    }
                agg[code]['실판매금액'] += float(r.get('실판매금액') or 0)
                agg[code]['판매량'] += float(r.get('판매량') or 0)
            return sorted(agg.values(), key=lambda x: x['실판매금액'], reverse=True)[:30]
        except:
            return []

def get_daily_sales(file_id=None):
    """일별 매출 (file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""
    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                판매일자,
                SUM(실판매금액) as 실판매금액,
                SUM(판매량) as 판매량,
                COUNT(*) as 건수
            FROM monthly_sales
            WHERE 판매일자 IS NOT NULL {file_filter_and}
            GROUP BY 판매일자
            ORDER BY 판매일자
        ''')
    else:
        try:
            if file_id:
                monthly = supabase_select('monthly_sales', '*', f'판매일자=not.is.null&file_id=eq.{file_id}')
            else:
                monthly = supabase_select('monthly_sales', '*', '판매일자=not.is.null')
            agg = {}
            for r in monthly:
                date = r.get('판매일자')
                if not date:
                    continue
                if date not in agg:
                    agg[date] = {'판매일자': date, '실판매금액': 0, '판매량': 0, '건수': 0}
                agg[date]['실판매금액'] += float(r.get('실판매금액') or 0)
                agg[date]['판매량'] += float(r.get('판매량') or 0)
                agg[date]['건수'] += 1
            return sorted(agg.values(), key=lambda x: x['판매일자'])
        except:
            return []

def get_monthly_sales(file_id=None):
    """월별 매출 (일별 데이터를 월 단위로 집계, file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""
    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                SUBSTR(판매일자, 1, 7) as 월,
                SUM(실판매금액) as 실판매금액,
                SUM(판매량) as 판매량,
                COUNT(*) as 건수,
                COUNT(DISTINCT 매장명) as 매장수
            FROM monthly_sales
            WHERE 판매일자 IS NOT NULL {file_filter_and}
            GROUP BY SUBSTR(판매일자, 1, 7)
            ORDER BY 월
        ''')
    else:
        try:
            if file_id:
                monthly = supabase_select('monthly_sales', '*', f'판매일자=not.is.null&file_id=eq.{file_id}')
            else:
                monthly = supabase_select('monthly_sales', '*', '판매일자=not.is.null')
            agg = {}
            for r in monthly:
                date = r.get('판매일자')
                if not date:
                    continue
                month = date[:7] if len(date) >= 7 else date
                if month not in agg:
                    agg[month] = {'월': month, '실판매금액': 0, '판매량': 0, '건수': 0, '매장수': set()}
                agg[month]['실판매금액'] += float(r.get('실판매금액') or 0)
                agg[month]['판매량'] += float(r.get('판매량') or 0)
                agg[month]['건수'] += 1
                if r.get('매장명'):
                    agg[month]['매장수'].add(r.get('매장명'))
            result = [{'월': k, '실판매금액': v['실판매금액'], '판매량': v['판매량'], '건수': v['건수'], '매장수': len(v['매장수'])} for k, v in agg.items()]
            return sorted(result, key=lambda x: x['월'])
        except:
            return []

def get_store_sales(file_id=None):
    """매장별 매출 (file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""
    if IS_LOCAL:
        return execute_query(f'''
            SELECT
                매장명,
                SUM(실판매금액) as 실판매금액,
                SUM(판매량) as 판매량,
                COUNT(*) as 건수
            FROM monthly_sales
            WHERE 매장명 IS NOT NULL AND 매장명 != '' {file_filter_and}
            GROUP BY 매장명
            ORDER BY 실판매금액 DESC
            LIMIT 30
        ''')
    else:
        try:
            if file_id:
                monthly = supabase_select('monthly_sales', '*', f'매장명=not.is.null&file_id=eq.{file_id}')
            else:
                monthly = supabase_select('monthly_sales', '*', '매장명=not.is.null')
            agg = {}
            for r in monthly:
                store = r.get('매장명')
                if not store:
                    continue
                if store not in agg:
                    agg[store] = {'매장명': store, '실판매금액': 0, '판매량': 0, '건수': 0}
                agg[store]['실판매금액'] += float(r.get('실판매금액') or 0)
                agg[store]['판매량'] += float(r.get('판매량') or 0)
                agg[store]['건수'] += 1
            return sorted(agg.values(), key=lambda x: x['실판매금액'], reverse=True)[:30]
        except:
            return []

def get_supplier_category_matrix(file_id=None):
    """업체-카테고리-상품 계층 구조 (드릴다운용, file_id로 필터링 가능)"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""
    if IS_LOCAL:
        result = execute_query(f'''
            SELECT
                업체명,
                카테고리,
                상품코드,
                상품명,
                SUM(실판매금액) as 매출액,
                SUM(판매량) as 판매량
            FROM sales_data
            WHERE 업체명 IS NOT NULL AND 카테고리 IS NOT NULL {file_filter_and}
            GROUP BY 업체명, 카테고리, 상품코드, 상품명
            ORDER BY 매출액 DESC
        ''')
    else:
        try:
            if file_id:
                sales = supabase_select('sales_data', '*', f'업체명=not.is.null&file_id=eq.{file_id}')
            else:
                sales = supabase_select('sales_data', '*', '업체명=not.is.null')
            # 집계
            agg = {}
            for r in sales:
                key = (r.get('업체명'), r.get('카테고리'), r.get('상품코드'), r.get('상품명'))
                if key not in agg:
                    agg[key] = {'업체명': r.get('업체명'), '카테고리': r.get('카테고리'), '상품코드': r.get('상품코드'), '상품명': r.get('상품명'), '매출액': 0, '판매량': 0}
                agg[key]['매출액'] += float(r.get('실판매금액') or 0)
                agg[key]['판매량'] += float(r.get('판매량') or 0)
            result = list(agg.values())
        except:
            result = []

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

def get_store_category_matrix(file_id=None):
    """매장-카테고리-상품 계층 구조 (드릴다운용, file_id로 필터링 가능) - 매장별 뭐가 많이 팔리는지 분석"""
    file_filter_and = f"AND file_id = {file_id}" if file_id else ""
    if IS_LOCAL:
        result = execute_query(f'''
            SELECT
                매장명,
                분류명,
                상품코드,
                상품명,
                SUM(실판매금액) as 매출액,
                SUM(판매량) as 판매량
            FROM monthly_sales
            WHERE 매장명 IS NOT NULL AND 분류명 IS NOT NULL {file_filter_and}
            GROUP BY 매장명, 분류명, 상품코드, 상품명
            ORDER BY 매출액 DESC
        ''')
    else:
        try:
            if file_id:
                monthly = supabase_select('monthly_sales', '*', f'매장명=not.is.null&file_id=eq.{file_id}')
            else:
                monthly = supabase_select('monthly_sales', '*', '매장명=not.is.null')
            agg = {}
            for r in monthly:
                key = (r.get('매장명'), r.get('분류명'), r.get('상품코드'), r.get('상품명'))
                if key not in agg:
                    agg[key] = {'매장명': r.get('매장명'), '분류명': r.get('분류명'), '상품코드': r.get('상품코드'), '상품명': r.get('상품명'), '매출액': 0, '판매량': 0}
                agg[key]['매출액'] += float(r.get('실판매금액') or 0)
                agg[key]['판매량'] += float(r.get('판매량') or 0)
            result = list(agg.values())
        except:
            result = []

    # 계층 구조 생성
    hierarchy = {}

    for row in result:
        매장명 = row['매장명']
        분류명 = row['분류명'] or '기타'
        상품코드 = row['상품코드']
        상품명 = row['상품명']
        매출액 = float(row['매출액'] or 0)
        판매량 = float(row['판매량'] or 0)

        if 매장명 not in hierarchy:
            hierarchy[매장명] = {
                '매장명': 매장명,
                'total': 0,
                'total_qty': 0,
                'categories': {}
            }

        if 분류명 not in hierarchy[매장명]['categories']:
            hierarchy[매장명]['categories'][분류명] = {
                '카테고리': 분류명,
                'total': 0,
                'total_qty': 0,
                'products': []
            }

        hierarchy[매장명]['categories'][분류명]['products'].append({
            '상품코드': 상품코드,
            '상품명': 상품명,
            '매출액': 매출액,
            '판매량': 판매량
        })
        hierarchy[매장명]['categories'][분류명]['total'] += 매출액
        hierarchy[매장명]['categories'][분류명]['total_qty'] += 판매량
        hierarchy[매장명]['total'] += 매출액
        hierarchy[매장명]['total_qty'] += 판매량

    # 매장 정렬 (총 매출 내림차순)
    sorted_stores = sorted(hierarchy.values(), key=lambda x: x['total'], reverse=True)[:50]  # 상위 50개 매장

    # 각 매장 내 카테고리와 상품 정렬
    for store in sorted_stores:
        categories_list = sorted(store['categories'].values(), key=lambda x: x['total'], reverse=True)
        for cat in categories_list:
            cat['products'] = sorted(cat['products'], key=lambda x: x['매출액'], reverse=True)[:15]  # 카테고리당 상위 15개
        store['categories'] = categories_list

    return sorted_stores

# ============ 관리자 계정 함수들 ============

def verify_admin(username, password):
    """관리자 로그인 확인"""
    try:
        if IS_LOCAL:
            result = execute_query(
                'SELECT * FROM admin_users WHERE username = ? AND password = ?',
                (username, password)
            )
            return result[0] if result else None
        else:
            result = supabase_select('admin_users', '*', f'username=eq.{username}&password=eq.{password}')
            return result[0] if result else None
    except Exception as e:
        print(f"verify_admin error: {e}")
        return None

def change_password(username, old_password, new_password):
    """비밀번호 변경"""
    if IS_LOCAL:
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
    else:
        result = supabase_select('admin_users', '*', f'username=eq.{username}&password=eq.{old_password}')
        if not result:
            return False, "현재 비밀번호가 일치하지 않습니다."

        supabase_update('admin_users', {'password': new_password}, f'username=eq.{username}')

    return True, "비밀번호가 변경되었습니다."

def get_admin_info(username):
    """관리자 정보 조회"""
    if IS_LOCAL:
        result = execute_query(
            'SELECT id, username, created_at, updated_at FROM admin_users WHERE username = ?',
            (username,)
        )
        return result[0] if result else None
    else:
        result = supabase_select('admin_users', 'id,username,created_at,updated_at', f'username=eq.{username}')
        return result[0] if result else None

def create_backup():
    """현재 데이터를 백업 (JSON 형태로 반환)"""
    backup_data = {
        'created_at': datetime.now().isoformat(),
        'sales_data': [],
        'monthly_sales': [],
        'upload_files': []
    }

    if IS_LOCAL:
        backup_data['sales_data'] = execute_query('SELECT * FROM sales_data')
        backup_data['monthly_sales'] = execute_query('SELECT * FROM monthly_sales')
        backup_data['upload_files'] = execute_query('SELECT * FROM upload_files WHERE status = "active"')
    else:
        backup_data['sales_data'] = supabase_select('sales_data', '*')
        backup_data['monthly_sales'] = supabase_select('monthly_sales', '*')
        backup_data['upload_files'] = supabase_select('upload_files', '*', 'status=eq.active')

    return backup_data

def restore_backup(backup_data):
    """백업 데이터 복원"""
    try:
        # 기존 데이터 삭제
        reset_all_data()

        if IS_LOCAL:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # upload_files 복원
            for row in backup_data.get('upload_files', []):
                cursor.execute('''
                    INSERT INTO upload_files (id, filename, original_name, file_type, row_count, upload_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (row['id'], row['filename'], row.get('original_name'), row.get('file_type'),
                      row.get('row_count', 0), row.get('upload_date'), 'active'))

            # sales_data 복원
            for row in backup_data.get('sales_data', []):
                cursor.execute('''
                    INSERT INTO sales_data (file_id, 분류명, 카테고리, 업체명, 상품코드, 바코드, 상품명,
                        판매일, 주문수, 주문건, 주문량, 판매단가, 최종단가, 수발주단가,
                        판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액, 판매량, 실판매단가, 실판매금액)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row.get('file_id'), row.get('분류명'), row.get('카테고리'), row.get('업체명'),
                      row.get('상품코드'), row.get('바코드'), row.get('상품명'), row.get('판매일'),
                      row.get('주문수'), row.get('주문건'), row.get('주문량'), row.get('판매단가'),
                      row.get('최종단가'), row.get('수발주단가'), row.get('판매가'), row.get('취소수'),
                      row.get('취소량'), row.get('취소금액'), row.get('할인량'), row.get('할인금액'),
                      row.get('판매량'), row.get('실판매단가'), row.get('실판매금액')))

            # monthly_sales 복원
            for row in backup_data.get('monthly_sales', []):
                cursor.execute('''
                    INSERT INTO monthly_sales (file_id, data_type, 판매일자, 매장코드, 매장명, 분류명, 카테고리, 업체명,
                        상품코드, 상품명, 판매일, 주문수, 주문건, 주문량, 판매단가, 수발주단가,
                        판매가, 취소수, 취소량, 취소금액, 할인량, 할인금액, 판매량, 실판매단가, 실판매금액)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row.get('file_id'), row.get('data_type'), row.get('판매일자'), row.get('매장코드'),
                      row.get('매장명'), row.get('분류명'), row.get('카테고리'), row.get('업체명'),
                      row.get('상품코드'), row.get('상품명'), row.get('판매일'), row.get('주문수'),
                      row.get('주문건'), row.get('주문량'), row.get('판매단가'), row.get('수발주단가'),
                      row.get('판매가'), row.get('취소수'), row.get('취소량'), row.get('취소금액'),
                      row.get('할인량'), row.get('할인금액'), row.get('판매량'), row.get('실판매단가'),
                      row.get('실판매금액')))

            conn.commit()
            conn.close()
        else:
            # Supabase 복원 - 배치로 처리
            if backup_data.get('upload_files'):
                supabase_insert('upload_files', backup_data['upload_files'])
            if backup_data.get('sales_data'):
                supabase_insert('sales_data', backup_data['sales_data'])
            if backup_data.get('monthly_sales'):
                supabase_insert('monthly_sales', backup_data['monthly_sales'])

        return True, "백업 복원 완료"
    except Exception as e:
        return False, str(e)

def get_backup_list():
    """저장된 백업 목록 조회 (로컬 파일 시스템)"""
    backup_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'backups')
    os.makedirs(backup_dir, exist_ok=True)

    backups = []
    for filename in os.listdir(backup_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(backup_dir, filename)
            stat = os.stat(filepath)
            backups.append({
                'filename': filename,
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

    return sorted(backups, key=lambda x: x['created'], reverse=True)

def save_backup_to_file(backup_data):
    """백업 데이터를 파일로 저장"""
    backup_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'backups')
    os.makedirs(backup_dir, exist_ok=True)

    filename = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(backup_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, default=str)

    return filename, filepath

def load_backup_from_file(filename):
    """파일에서 백업 데이터 로드"""
    backup_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'backups')
    filepath = os.path.join(backup_dir, filename)

    if not os.path.exists(filepath):
        return None

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def reset_all_data():
    """모든 판매 데이터 초기화 (admin_users 제외)"""
    if IS_LOCAL:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM sales_data')
        cursor.execute('DELETE FROM monthly_sales')
        cursor.execute('DELETE FROM upload_files')
        conn.commit()
        conn.close()
        return True
    else:
        # Supabase에서 모든 데이터 삭제 (neq.0은 id > 0인 모든 행 삭제)
        try:
            supabase_delete('sales_data', 'id=gt.0')
        except:
            pass
        try:
            supabase_delete('monthly_sales', 'id=gt.0')
        except:
            pass
        try:
            supabase_delete('upload_files', 'id=gt.0')
        except:
            pass
        return True

def get_data_counts():
    """각 테이블의 데이터 건수 조회"""
    if IS_LOCAL:
        sales_data = execute_query('SELECT COUNT(*) as cnt FROM sales_data')
        monthly_sales = execute_query('SELECT COUNT(*) as cnt FROM monthly_sales')
        upload_files = execute_query('SELECT COUNT(*) as cnt FROM upload_files')
        return {
            'sales_data': sales_data[0]['cnt'] if sales_data else 0,
            'monthly_sales': monthly_sales[0]['cnt'] if monthly_sales else 0,
            'upload_files': upload_files[0]['cnt'] if upload_files else 0
        }
    else:
        try:
            # Supabase에서 카운트 조회 (GET 요청 + count=exact 헤더)
            headers = get_supabase_headers()
            headers['Prefer'] = 'count=exact'
            headers['Range'] = '0-0'  # 데이터 최소화

            def parse_count(resp):
                content_range = resp.headers.get('content-range', '*/0')
                if '/' in content_range:
                    return int(content_range.split('/')[-1])
                return 0

            with httpx.Client(timeout=30.0) as client:
                # sales_data count
                resp = client.get(f"{SUPABASE_URL}/rest/v1/sales_data?select=id", headers=headers)
                sales_count = parse_count(resp)

                # monthly_sales count
                resp = client.get(f"{SUPABASE_URL}/rest/v1/monthly_sales?select=id", headers=headers)
                monthly_count = parse_count(resp)

                # upload_files count
                resp = client.get(f"{SUPABASE_URL}/rest/v1/upload_files?select=id", headers=headers)
                files_count = parse_count(resp)

            return {
                'sales_data': sales_count,
                'monthly_sales': monthly_count,
                'upload_files': files_count
            }
        except Exception as e:
            print(f"Count error: {e}")
            return {'sales_data': 0, 'monthly_sales': 0, 'upload_files': 0}

# 초기화 실행
if __name__ == '__main__':
    init_database()
    print("데이터베이스 테이블 생성 완료!")
