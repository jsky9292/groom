import sqlite3
import os
from datetime import datetime
import pandas as pd

# Vercel 환경 감지
IS_VERCEL = os.environ.get('VERCEL', False)

if IS_VERCEL:
    DB_PATH = '/tmp/sales_data.db'
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'sales_data.db')

def get_connection():
    """데이터베이스 연결"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """데이터베이스 초기화 - 테이블 생성"""
    conn = get_connection()
    cursor = conn.cursor()

    # 관리자 계정 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 기본 관리자 계정 생성 (없으면)
    cursor.execute('SELECT COUNT(*) FROM admin_users WHERE username = ?', ('admin',))
    if cursor.fetchone()[0] == 0:
        cursor.execute('INSERT INTO admin_users (username, password) VALUES (?, ?)', ('admin', 'admin123'))

    # 업로드 파일 기록 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS upload_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            original_name TEXT,
            file_type TEXT,
            row_count INTEGER,
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active'
        )
    ''')

    # 판매 데이터 테이블 (원본)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_data (
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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (file_id) REFERENCES upload_files(id)
        )
    ''')

    # 월별 판매 데이터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_sales (
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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (file_id) REFERENCES upload_files(id)
        )
    ''')

    # 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_업체명 ON sales_data(업체명)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_카테고리 ON sales_data(카테고리)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_상품코드 ON sales_data(상품코드)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_판매일자 ON monthly_sales(판매일자)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_매장명 ON monthly_sales(매장명)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_monthly_업체명 ON monthly_sales(업체명)')

    conn.commit()
    conn.close()
    print(f"데이터베이스 초기화 완료: {DB_PATH}")

def parse_classification(분류명):
    """
    분류명에서 카테고리와 업체명 추출
    예: '상의(구름과환경)' -> 카테고리='상의', 업체명='구름과환경'
    예: '장갑(신정글러브)' -> 카테고리='장갑', 업체명='신정글러브'
    예: '안전화(세이프티조거)' -> 카테고리='안전화', 업체명='세이프티조거'
    """
    if pd.isna(분류명) or not 분류명:
        return None, None

    분류명 = str(분류명).strip()

    # 괄호가 있는 경우: 카테고리(업체명)
    if '(' in 분류명 and ')' in 분류명:
        # 마지막 괄호 기준으로 분리 (중첩 괄호 대응)
        last_open = 분류명.rfind('(')
        last_close = 분류명.rfind(')')

        if last_open < last_close:
            카테고리 = 분류명[:last_open].strip()
            업체명 = 분류명[last_open+1:last_close].strip()
            return 카테고리, 업체명

    # 괄호가 없는 경우: 전체가 카테고리
    return 분류명, None

def clean_numeric(value):
    """숫자 값 정리 (쉼표 제거, 변환)"""
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
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO upload_files (filename, original_name, file_type, row_count)
        VALUES (?, ?, ?, ?)
    ''', (filename, original_name, file_type, row_count))

    file_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return file_id

def save_sales_data(df, file_id):
    """원본 판매 데이터 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
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
            file_id,
            분류명,
            카테고리,
            업체명,
            row.get('상품코드'),
            row.get('바코드'),
            row.get('상품명'),
            clean_numeric(row.get('판매일')),
            clean_numeric(row.get('주문수')),
            clean_numeric(row.get('주문건')),
            clean_numeric(row.get('주문량')),
            clean_numeric(row.get('판매단가')),
            clean_numeric(row.get('최종단가')),
            clean_numeric(row.get('수발주단가')),
            clean_numeric(row.get('판매가')),
            clean_numeric(row.get('취소수')),
            clean_numeric(row.get('취소량')),
            clean_numeric(row.get('취소금액')),
            clean_numeric(row.get('할인량')),
            clean_numeric(row.get('할인금액')),
            clean_numeric(row.get('판매량')),
            clean_numeric(row.get('실판매단가')),
            clean_numeric(row.get('실판매금액'))
        ))
        inserted += 1

    conn.commit()
    conn.close()
    return inserted

def save_monthly_data(df, file_id, data_type):
    """월별 판매 데이터 저장"""
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        # 합계 행 제외 (매장수 : xxx 형식)
        매장명 = row.get('매장명', '')
        if pd.notna(매장명) and '매장수' in str(매장명):
            continue

        분류명 = row.get('분류명', '')
        카테고리, 업체명 = parse_classification(분류명)

        # 판매일자 처리
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
            file_id,
            data_type,
            판매일자,
            row.get('매장코드'),
            row.get('매장명'),
            분류명,
            카테고리,
            업체명,
            row.get('상품코드'),
            row.get('상품명'),
            clean_numeric(row.get('판매일')),
            clean_numeric(row.get('주문수')),
            clean_numeric(row.get('주문건')),
            clean_numeric(row.get('주문량')),
            clean_numeric(row.get('판매단가')),
            clean_numeric(row.get('수발주단가')),
            clean_numeric(row.get('판매가')),
            clean_numeric(row.get('취소수')),
            clean_numeric(row.get('취소량')),
            clean_numeric(row.get('취소금액')),
            clean_numeric(row.get('할인량')),
            clean_numeric(row.get('할인금액')),
            clean_numeric(row.get('판매량')),
            clean_numeric(row.get('실판매단가')),
            clean_numeric(row.get('실판매금액'))
        ))
        inserted += 1

    conn.commit()
    conn.close()
    return inserted

def get_upload_files():
    """업로드된 파일 목록 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, filename, original_name, file_type, row_count, upload_date, status
        FROM upload_files
        WHERE status = 'active'
        ORDER BY upload_date DESC
    ''')

    files = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return files

def delete_file_data(file_id):
    """파일 및 관련 데이터 삭제"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('DELETE FROM sales_data WHERE file_id = ?', (file_id,))
    cursor.execute('DELETE FROM monthly_sales WHERE file_id = ?', (file_id,))
    cursor.execute('UPDATE upload_files SET status = "deleted" WHERE id = ?', (file_id,))

    conn.commit()
    conn.close()

# ============ 통계 조회 함수들 ============

def get_summary_stats():
    """요약 통계"""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # 원본 데이터 통계
    cursor.execute('''
        SELECT
            COUNT(*) as total_records,
            SUM(실판매금액) as total_sales,
            SUM(판매량) as total_qty,
            COUNT(DISTINCT 상품코드) as unique_products,
            COUNT(DISTINCT 업체명) as unique_suppliers,
            COUNT(DISTINCT 카테고리) as unique_categories
        FROM sales_data
    ''')
    row = cursor.fetchone()
    stats['original'] = dict(row) if row else {}

    # 월별 데이터 통계
    cursor.execute('''
        SELECT
            COUNT(*) as total_records,
            SUM(실판매금액) as total_sales,
            SUM(판매량) as total_qty,
            COUNT(DISTINCT 매장명) as unique_stores
        FROM monthly_sales
    ''')
    row = cursor.fetchone()
    stats['monthly'] = dict(row) if row else {}

    # 데이터 타입별
    cursor.execute('''
        SELECT data_type, COUNT(*) as cnt
        FROM monthly_sales
        GROUP BY data_type
    ''')
    stats['by_type'] = {row['data_type']: row['cnt'] for row in cursor.fetchall()}

    conn.close()
    return stats

def get_sales_by_supplier():
    """업체별 매출"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
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

    result = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result

def get_sales_by_category():
    """카테고리별 매출"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
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

    result = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result

def get_top_products():
    """베스트셀러 상품"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
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

    result = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result

def get_daily_sales():
    """일별 매출"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
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

    result = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result

def get_store_sales():
    """매장별 매출"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
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

    result = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result

def get_supplier_category_matrix():
    """업체-카테고리-상품 계층 구조 (드릴다운용)"""
    conn = get_connection()
    cursor = conn.cursor()

    # 업체-카테고리-상품별 데이터
    cursor.execute('''
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

    for row in cursor.fetchall():
        업체명 = row['업체명']
        카테고리 = row['카테고리']
        상품코드 = row['상품코드']
        상품명 = row['상품명']
        매출액 = row['매출액'] or 0
        판매량 = row['판매량'] or 0

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
            cat['products'] = sorted(cat['products'], key=lambda x: x['매출액'], reverse=True)[:20]  # 상위 20개만
        supplier['categories'] = categories_list

    conn.close()
    return sorted_suppliers

# ============ 관리자 계정 함수들 ============

def verify_admin(username, password):
    """관리자 로그인 확인"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM admin_users WHERE username = ? AND password = ?', (username, password))
    user = cursor.fetchone()
    conn.close()

    return dict(user) if user else None

def change_password(username, old_password, new_password):
    """비밀번호 변경"""
    conn = get_connection()
    cursor = conn.cursor()

    # 기존 비밀번호 확인
    cursor.execute('SELECT * FROM admin_users WHERE username = ? AND password = ?', (username, old_password))
    if not cursor.fetchone():
        conn.close()
        return False, "현재 비밀번호가 일치하지 않습니다."

    # 비밀번호 업데이트
    cursor.execute('''
        UPDATE admin_users
        SET password = ?, updated_at = CURRENT_TIMESTAMP
        WHERE username = ?
    ''', (new_password, username))

    conn.commit()
    conn.close()
    return True, "비밀번호가 변경되었습니다."

def get_admin_info(username):
    """관리자 정보 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT id, username, created_at, updated_at FROM admin_users WHERE username = ?', (username,))
    user = cursor.fetchone()
    conn.close()

    return dict(user) if user else None

# 초기화 실행
if __name__ == '__main__':
    init_database()
    print("데이터베이스 테이블 생성 완료!")
