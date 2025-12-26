"""
Supabase에 상품 이미지 매핑 및 재고 데이터 업로드 스크립트
"""
import pandas as pd
import os
import sys
import httpx

# Supabase 설정
SUPABASE_URL = 'https://xhjiokshzesnrmzppkwd.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inhoamlva3NoemVzbnJtenBwa3dkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjU0NDcwMjEsImV4cCI6MjA4MTAyMzAyMX0.pbmUsbrpM_91aPPIGw-trcXBLPGLZrPedcvVwcYbEHc'

headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}


def supabase_delete(table):
    """테이블의 모든 데이터 삭제"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?id=gt.0"
    with httpx.Client(timeout=60.0) as client:
        resp = client.delete(url, headers=headers)
        return resp.status_code


def supabase_insert(table, data):
    """데이터 삽입"""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(url, json=data, headers=headers)
        return resp.status_code, resp.text


def upload_product_images():
    """상품 이미지 매핑 데이터 업로드"""
    print("=" * 50)
    print("상품 이미지 매핑 데이터 업로드")
    print("=" * 50)

    file_path = r'D:\workup\전체상품목록_20251225180741_4784622.xls'

    if not os.path.exists(file_path):
        print(f"파일을 찾을 수 없습니다: {file_path}")
        return False

    try:
        # HTML 형식 XLS 파싱 (여러 인코딩 시도)
        df = None
        for enc in ['utf-8', 'cp949', 'euc-kr', 'utf-16']:
            try:
                dfs = pd.read_html(file_path, encoding=enc, header=0)
                df = dfs[0]
                # 첫번째 행이 헤더인 경우 처리
                if df.columns[0] == 0 or str(df.columns[0]).isdigit():
                    df.columns = df.iloc[0]
                    df = df.iloc[1:].reset_index(drop=True)
                print(f"인코딩 성공: {enc}")
                break
            except Exception as e:
                continue

        if df is None:
            print("모든 인코딩 실패")
            return False

        print(f"로드된 행: {len(df)}")
        print(f"컬럼 샘플: {list(df.columns)[:10]}")

        # 컬럼 매핑
        col_map = {}
        for col in df.columns:
            col_str = str(col).strip()
            if '상품코드' in col_str and '대표' not in col_str and 'product_code' not in col_map:
                col_map['product_code'] = col
            elif '상품명' in col_str and '공급처' not in col_str and 'product_name' not in col_map:
                col_map['product_name'] = col
            elif '공급처 옵션' in col_str or '공급처옵션' in col_str:
                col_map['supplier_option'] = col
            elif '바코드' in col_str:
                col_map['barcode'] = col

        print(f"매핑된 컬럼: {col_map}")

        # 기존 데이터 삭제
        print("기존 데이터 삭제 중...")
        status = supabase_delete('product_images')
        print(f"삭제 상태: {status}")

        # 데이터 준비
        records = []
        for _, row in df.iterrows():
            product_code = str(row.get(col_map.get('product_code'), '')).strip()
            supplier_option = str(row.get(col_map.get('supplier_option'), '')).strip()

            if product_code and supplier_option and supplier_option != 'nan' and product_code != 'nan':
                image_url = f'https://ga29.ezadmin.co.kr/uploads/drcamp/{product_code}_500.png'
                records.append({
                    'supplier_option': supplier_option[:200],
                    'product_code': product_code[:50],
                    'product_name': str(row.get(col_map.get('product_name'), ''))[:200].strip(),
                    'barcode': str(row.get(col_map.get('barcode'), ''))[:50].strip(),
                    'image_url': image_url
                })

        print(f"업로드할 레코드: {len(records)}")

        # 배치 업로드 (100개씩)
        batch_size = 100
        uploaded = 0
        errors = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                status, text = supabase_insert('product_images', batch)
                if status in [200, 201]:
                    uploaded += len(batch)
                else:
                    errors += len(batch)
                    print(f"오류 ({status}): {text[:100]}")

                if (i // batch_size) % 10 == 0:
                    print(f"진행: {uploaded}/{len(records)} (오류: {errors})")
            except Exception as e:
                errors += len(batch)
                print(f"배치 오류: {e}")

        print(f"✅ 상품 이미지 매핑 완료: {uploaded}건 (오류: {errors})")
        return True

    except Exception as e:
        print(f"❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        return False


def upload_inventory():
    """재고 데이터 업로드"""
    print("\n" + "=" * 50)
    print("재고 데이터 업로드")
    print("=" * 50)

    file_path = r'D:\workup\현재고조회_20251225183824_504627405.xls'

    if not os.path.exists(file_path):
        print(f"파일을 찾을 수 없습니다: {file_path}")
        return False

    try:
        # HTML 형식 XLS 파싱 (여러 인코딩 시도)
        df = None
        for enc in ['utf-8', 'cp949', 'euc-kr', 'utf-16']:
            try:
                dfs = pd.read_html(file_path, encoding=enc, header=0)
                df = dfs[0]
                # 첫번째 행이 헤더인 경우 처리
                if df.columns[0] == 0 or str(df.columns[0]).isdigit():
                    df.columns = df.iloc[0]
                    df = df.iloc[1:].reset_index(drop=True)
                print(f"인코딩 성공: {enc}")
                break
            except:
                continue

        if df is None:
            print("모든 인코딩 실패")
            return False

        print(f"로드된 행: {len(df)}")
        print(f"컬럼 샘플: {list(df.columns)[:10]}")

        # 컬럼 매핑
        col_map = {}
        for col in df.columns:
            col_str = str(col).strip()
            if '상품코드' in col_str and 'product_code' not in col_map:
                col_map['product_code'] = col
            elif col_str == '공급처' or (col_str.startswith('공급처') and '옵션' not in col_str and '상품' not in col_str and 'supplier' not in col_map):
                col_map['supplier'] = col
            elif '상품명' in col_str and '공급처' not in col_str and 'product_name' not in col_map:
                col_map['product_name'] = col
            elif '옵션명' in col_str or col_str == '옵션':
                col_map['option_name'] = col
            elif '공급가' in col_str:
                col_map['supply_price'] = col
            elif '판매가' in col_str:
                col_map['sale_price'] = col
            elif '공급처옵션코드' in col_str or '업체옵션코드' in col_str or '공급처 옵션' in col_str:
                col_map['supplier_option'] = col
            elif '바코드' in col_str:
                col_map['barcode'] = col
            elif '정상재고' in col_str:
                col_map['normal_stock'] = col
            elif '가용재고' in col_str:
                col_map['available_stock'] = col
            elif '품절여부' in col_str:
                col_map['is_soldout'] = col

        print(f"매핑된 컬럼: {list(col_map.keys())}")

        # 기존 데이터 삭제
        print("기존 데이터 삭제 중...")
        status = supabase_delete('inventory')
        print(f"삭제 상태: {status}")

        # 데이터 준비
        def safe_int(val):
            try:
                if pd.isna(val):
                    return 0
                return int(float(str(val).replace(',', '')))
            except:
                return 0

        def safe_float(val):
            try:
                if pd.isna(val):
                    return 0.0
                return float(str(val).replace(',', ''))
            except:
                return 0.0

        records = []
        for _, row in df.iterrows():
            product_code = str(row.get(col_map.get('product_code'), '')).strip()
            if not product_code or product_code == 'nan':
                continue

            records.append({
                'product_code': product_code[:50],
                'supplier': str(row.get(col_map.get('supplier'), ''))[:100].strip() if col_map.get('supplier') else '',
                'product_name': str(row.get(col_map.get('product_name'), ''))[:200].strip() if col_map.get('product_name') else '',
                'option_name': str(row.get(col_map.get('option_name'), ''))[:100].strip() if col_map.get('option_name') else '',
                'supply_price': safe_float(row.get(col_map.get('supply_price'), 0)),
                'sale_price': safe_float(row.get(col_map.get('sale_price'), 0)),
                'supplier_option': str(row.get(col_map.get('supplier_option'), ''))[:200].strip() if col_map.get('supplier_option') else '',
                'barcode': str(row.get(col_map.get('barcode'), ''))[:50].strip() if col_map.get('barcode') else '',
                'normal_stock': safe_int(row.get(col_map.get('normal_stock'), 0)),
                'available_stock': safe_int(row.get(col_map.get('available_stock'), 0)),
                'is_soldout': 1 if str(row.get(col_map.get('is_soldout'), '')).strip() in ['Y', 'y', '1', 'true', 'True'] else 0
            })

        print(f"업로드할 레코드: {len(records)}")

        # 배치 업로드 (100개씩)
        batch_size = 100
        uploaded = 0
        errors = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                status, text = supabase_insert('inventory', batch)
                if status in [200, 201]:
                    uploaded += len(batch)
                else:
                    errors += len(batch)
                    if errors < 5:
                        print(f"오류 ({status}): {text[:200]}")

                if (i // batch_size) % 20 == 0:
                    print(f"진행: {uploaded}/{len(records)} (오류: {errors})")
            except Exception as e:
                errors += len(batch)
                print(f"배치 오류: {e}")

        print(f"✅ 재고 데이터 완료: {uploaded}건 (오류: {errors})")
        return True

    except Exception as e:
        print(f"❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("Supabase 데이터 업로드 시작")
    print(f"URL: {SUPABASE_URL}")

    print("\n1. 상품 이미지 매핑 업로드...")
    upload_product_images()

    print("\n2. 재고 데이터 업로드...")
    upload_inventory()

    print("\n" + "=" * 50)
    print("업로드 완료!")
    print("=" * 50)
