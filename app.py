from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import os
from functools import wraps
from datetime import datetime
import json

# 데이터베이스 모듈 임포트
from database import (
    init_database, execute_write, IS_LOCAL, supabase_update, supabase_select,
    save_upload_file, save_sales_data, save_monthly_data,
    get_upload_files, delete_file_data,
    get_summary_stats, get_sales_by_supplier, get_sales_by_category,
    get_top_products, get_daily_sales, get_monthly_sales, get_store_sales,
    get_supplier_category_matrix, get_store_category_matrix, parse_classification,
    verify_admin, change_password, get_admin_info,
    reset_all_data, get_data_counts,
    create_backup, restore_backup, get_backup_list, save_backup_to_file, load_backup_from_file
)

app = Flask(__name__)
app.secret_key = 'workup_dashboard_secret_key_2024'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB 제한

# Vercel 환경 감지
IS_VERCEL = os.environ.get('VERCEL', False)

# 데이터 파일 경로 (Vercel에서는 /tmp 사용)
if IS_VERCEL:
    DATA_DIR = '/tmp'
    UPLOAD_DIR = '/tmp/uploads'
else:
    DATA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')

os.makedirs(UPLOAD_DIR, exist_ok=True)

# 허용 파일 확장자
ALLOWED_EXTENSIONS = {'xls', 'xlsx', 'csv'}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
def update_file_row_count(file_id, row_count, increment=False):
    """파일의 row_count를 업데이트 (로컬/Supabase 환경 모두 지원)"""
    if IS_LOCAL:
        if increment:
            execute_write('UPDATE upload_files SET row_count = row_count + ? WHERE id = ?', (row_count, file_id))
        else:
            execute_write('UPDATE upload_files SET row_count = ? WHERE id = ?', (row_count, file_id))
    else:
        if increment:
            current = supabase_select('upload_files', 'row_count', f'id=eq.{file_id}')
            current_count = current[0]['row_count'] if current else 0
            supabase_update('upload_files', {'row_count': current_count + row_count}, f'id=eq.{file_id}')
        else:
            supabase_update('upload_files', {'row_count': row_count}, f'id=eq.{file_id}')



def process_and_save_file(filepath, file_type, original_name, saved_name):
    """파일을 처리하고 데이터베이스에 저장"""
    ext = filepath.rsplit('.', 1)[1].lower()
    total_rows = 0

    try:
        if file_type == 'original':
            # 원본 데이터 로드
            if ext == 'csv':
                df = pd.read_csv(filepath, encoding='cp949')
            elif ext == 'xls':
                try:
                    df = pd.read_csv(filepath, sep='\t', encoding='cp949')
                except:
                    df = pd.read_excel(filepath, engine='xlrd')
            else:
                df = pd.read_excel(filepath, engine='openpyxl')

            # DB에 파일 정보 저장
            file_id = save_upload_file(saved_name, original_name, file_type, len(df))

            # 판매 데이터 저장
            total_rows = save_sales_data(df, file_id)

        elif file_type == 'monthly':
            # 월별 데이터 (시트별)
            xls = pd.ExcelFile(filepath, engine='openpyxl')
            sheets = xls.sheet_names

            # 먼저 파일 정보 저장 (row_count는 나중에 업데이트)
            file_id = save_upload_file(saved_name, original_name, file_type, 0)

            for sheet in sheets:
                df = pd.read_excel(xls, sheet_name=sheet)
                sheet_lower = sheet.lower()

                # 시트명으로 데이터 타입 결정
                if '의류' in sheet or 'clothing' in sheet_lower:
                    data_type = '의류'
                elif '신발' in sheet or 'shoes' in sheet_lower:
                    data_type = '신발'
                elif '잡화' in sheet or 'accessories' in sheet_lower:
                    data_type = '잡화'
                else:
                    data_type = sheet

                rows = save_monthly_data(df, file_id, data_type)
                total_rows += rows

            # row_count 업데이트
            update_file_row_count(file_id, total_rows)

        elif file_type == 'custom':
            # 커스텀 데이터
            if ext == 'csv':
                df = pd.read_csv(filepath, encoding='cp949')
            elif ext == 'xls':
                try:
                    df = pd.read_csv(filepath, sep='\t', encoding='cp949')
                except:
                    df = pd.read_excel(filepath, engine='xlrd')
            else:
                df = pd.read_excel(filepath, engine='openpyxl')

            file_id = save_upload_file(saved_name, original_name, file_type, len(df))
            total_rows = save_sales_data(df, file_id)

        return True, total_rows

    except Exception as e:
        print(f"파일 처리 오류: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e)

# ============ 라우트 ============

@app.route('/')
def index():
    if 'logged_in' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        try:
            username = request.form.get('username')
            password = request.form.get('password')

            user = verify_admin(username, password)
            if user:
                session['logged_in'] = True
                session['username'] = username
                return redirect(url_for('dashboard'))
            else:
                error = '아이디 또는 비밀번호가 올바르지 않습니다.'
        except Exception as e:
            print(f"Login error: {e}")
            import traceback
            traceback.print_exc()
            error = '로그인 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.'

    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    stats = get_summary_stats()
    return render_template('dashboard.html', stats=stats)

@app.route('/upload')
@login_required
def upload_page():
    """파일 업로드 페이지"""
    try:
        files = get_upload_files()
        return render_template('upload.html', files=files)
    except Exception as e:
        import traceback
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return f"<pre>{error_msg}</pre>", 500

@app.route('/settings')
@login_required
def settings_page():
    """설정 페이지"""
    admin_info = get_admin_info(session.get('username'))
    return render_template('settings.html', admin_info=admin_info)

@app.route('/api/change-password', methods=['POST'])
@login_required
def api_change_password():
    """비밀번호 변경 API"""
    data = request.json
    old_password = data.get('old_password')
    new_password = data.get('new_password')
    confirm_password = data.get('confirm_password')

    if not old_password or not new_password:
        return jsonify({'success': False, 'error': '모든 필드를 입력해주세요.'})

    if new_password != confirm_password:
        return jsonify({'success': False, 'error': '새 비밀번호가 일치하지 않습니다.'})

    if len(new_password) < 4:
        return jsonify({'success': False, 'error': '비밀번호는 4자 이상이어야 합니다.'})

    username = session.get('username')
    success, message = change_password(username, old_password, new_password)

    return jsonify({'success': success, 'message' if success else 'error': message})

# ============ API ============

@app.route('/api/files')
@login_required
def api_files():
    """업로드된 파일 목록 조회"""
    try:
        files = get_upload_files()
        return jsonify({'success': True, 'files': files})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/summary')
@login_required
def api_summary():
    file_id = request.args.get('file_id', type=int)
    stats = get_summary_stats(file_id)
    return jsonify(stats)

@app.route('/api/sales-by-supplier')
@login_required
def api_sales_by_supplier():
    file_id = request.args.get('file_id', type=int)
    data = get_sales_by_supplier(file_id)
    return jsonify(data)

@app.route('/api/sales-by-category')
@login_required
def api_sales_by_category():
    file_id = request.args.get('file_id', type=int)
    data = get_sales_by_category(file_id)
    return jsonify(data)

@app.route('/api/top-products')
@login_required
def api_top_products():
    file_id = request.args.get('file_id', type=int)
    data = get_top_products(file_id)
    return jsonify(data)

@app.route('/api/daily-sales')
@login_required
def api_daily_sales():
    file_id = request.args.get('file_id', type=int)
    data = get_daily_sales(file_id)
    return jsonify(data)

@app.route('/api/monthly-sales')
@login_required
def api_monthly_sales():
    file_id = request.args.get('file_id', type=int)
    data = get_monthly_sales(file_id)
    return jsonify(data)

@app.route('/api/store-sales')
@login_required
def api_store_sales():
    file_id = request.args.get('file_id', type=int)
    data = get_store_sales(file_id)
    return jsonify(data)

@app.route('/api/supplier-category')
@login_required
def api_supplier_category():
    file_id = request.args.get('file_id', type=int)
    data = get_supplier_category_matrix(file_id)
    return jsonify(data)

@app.route('/api/store-category')
@login_required
def api_store_category():
    """매장별 상세 분석 - 매장→카테고리→상품 드릴다운"""
    file_id = request.args.get('file_id', type=int)
    data = get_store_category_matrix(file_id)
    return jsonify(data)

@app.route('/api/upload', methods=['POST'])
@login_required
def api_upload():
    """파일 업로드 API"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': '파일이 없습니다.'})

    file = request.files['file']
    file_type = request.form.get('file_type', 'custom')

    if file.filename == '':
        return jsonify({'success': False, 'error': '파일이 선택되지 않았습니다.'})

    if file and allowed_file(file.filename):
        original_name = file.filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        ext = original_name.rsplit('.', 1)[1].lower()
        safe_name = f"{timestamp}_{secure_filename(original_name)}"

        filepath = os.path.join(UPLOAD_DIR, safe_name)
        file.save(filepath)

        # 데이터 처리 및 DB 저장
        success, result = process_and_save_file(filepath, file_type, original_name, safe_name)

        if success:
            return jsonify({
                'success': True,
                'filename': original_name,
                'saved_as': safe_name,
                'rows': result,
                'message': f'파일이 업로드되어 DB에 저장되었습니다. ({result}행)'
            })
        else:
            return jsonify({'success': False, 'error': f'파일 처리 오류: {result}'})

    return jsonify({'success': False, 'error': '허용되지 않는 파일 형식입니다. (xls, xlsx, csv만 가능)'})

@app.route('/api/upload-chunk', methods=['POST'])
@login_required
def api_upload_chunk():
    """청크 데이터 업로드 API (브라우저에서 파싱된 JSON 데이터 수신)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': '데이터가 없습니다.'})

        file_id = data.get('file_id')
        file_type = data.get('file_type', 'original')
        rows = data.get('rows', [])
        chunk_index = data.get('chunk_index', 0)
        total_chunks = data.get('total_chunks', 1)
        original_name = data.get('original_name', 'unknown')
        data_type = data.get('data_type', '')  # 월별 데이터용

        if not rows:
            return jsonify({'success': False, 'error': '데이터 행이 없습니다.'})

        # 첫 번째 청크면 upload_files에 기록
        if chunk_index == 0:
            file_id = save_upload_file(f"chunk_{datetime.now().strftime('%Y%m%d_%H%M%S')}", original_name, file_type, 0)

        # DataFrame으로 변환
        df = pd.DataFrame(rows)

        # 디버깅: 컬럼명과 첫 행 출력
        print(f"[DEBUG] chunk_index={chunk_index}, rows_count={len(rows)}")
        print(f"[DEBUG] columns: {list(df.columns)[:10]}")
        if len(df) > 0:
            print(f"[DEBUG] first_row: {df.iloc[0].to_dict()}")

        # 데이터 저장
        if file_type == 'original':
            inserted = save_sales_data(df, file_id)
        elif file_type == 'monthly':
            inserted = save_monthly_data(df, file_id, data_type)
        else:
            inserted = save_sales_data(df, file_id)

        # row_count 업데이트
        update_file_row_count(file_id, inserted, increment=True)

        return jsonify({
            'success': True,
            'file_id': file_id,
            'inserted': inserted,
            'chunk_index': chunk_index,
            'total_chunks': total_chunks,
            'message': f'청크 {chunk_index + 1}/{total_chunks} 저장 완료 ({inserted}건)'
        })

    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()})

@app.route('/api/delete-file', methods=['POST'])
@login_required
def api_delete_file():
    """업로드된 파일 삭제"""
    file_id = request.json.get('file_id')
    filename = request.json.get('filename')

    if not file_id:
        return jsonify({'success': False, 'error': '파일 ID가 없습니다.'})

    try:
        # DB에서 삭제
        delete_file_data(file_id)

        # 실제 파일 삭제
        if filename:
            filepath = os.path.join(UPLOAD_DIR, filename)
            if os.path.exists(filepath):
                os.remove(filepath)

        return jsonify({'success': True, 'message': '파일이 삭제되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/reset-data', methods=['POST'])
@login_required
def api_reset_data():
    """모든 판매 데이터 초기화"""
    try:
        reset_all_data()
        return jsonify({'success': True, 'message': '모든 데이터가 초기화되었습니다.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/backup', methods=['POST'])
@login_required
def api_create_backup():
    """데이터 백업 생성"""
    try:
        backup_data = create_backup()
        filename, filepath = save_backup_to_file(backup_data)

        counts = {
            'sales_data': len(backup_data.get('sales_data', [])),
            'monthly_sales': len(backup_data.get('monthly_sales', [])),
            'upload_files': len(backup_data.get('upload_files', []))
        }

        return jsonify({
            'success': True,
            'filename': filename,
            'counts': counts,
            'message': f'백업이 생성되었습니다. (원본: {counts["sales_data"]}건, 월별: {counts["monthly_sales"]}건)'
        })
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()})

@app.route('/api/backup/list')
@login_required
def api_backup_list():
    """백업 목록 조회"""
    try:
        backups = get_backup_list()
        return jsonify({'success': True, 'backups': backups})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/backup/restore', methods=['POST'])
@login_required
def api_restore_backup():
    """백업 복원"""
    try:
        filename = request.json.get('filename')
        if not filename:
            return jsonify({'success': False, 'error': '백업 파일명이 필요합니다.'})

        backup_data = load_backup_from_file(filename)
        if not backup_data:
            return jsonify({'success': False, 'error': '백업 파일을 찾을 수 없습니다.'})

        success, message = restore_backup(backup_data)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()})

@app.route('/api/backup/download/<filename>')
@login_required
def api_download_backup(filename):
    """백업 파일 다운로드"""
    backup_dir = os.path.join(DATA_DIR, 'backups')
    filepath = os.path.join(backup_dir, filename)

    if not os.path.exists(filepath):
        return jsonify({'success': False, 'error': '파일을 찾을 수 없습니다.'}), 404

    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route('/api/data-counts')
@login_required
def api_data_counts():
    """각 테이블의 데이터 건수 조회"""
    try:
        counts = get_data_counts()
        return jsonify({'success': True, **counts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def flatten_supplier_category_matrix(data):
    """업체-카테고리-상품 계층 데이터를 평탄화"""
    flat_data = []
    for supplier in data:
        업체명 = supplier.get('업체명', '')
        categories = supplier.get('categories', {})
        for cat_name, cat_data in categories.items():
            for product in cat_data.get('products', []):
                flat_data.append({
                    '업체명': 업체명,
                    '카테고리': cat_name,
                    '상품코드': product.get('상품코드', ''),
                    '상품명': product.get('상품명', ''),
                    '매출액': product.get('매출액', 0),
                    '판매량': product.get('판매량', 0)
                })
    return flat_data

@app.route('/export/<data_type>')
@login_required
def export_data(data_type):
    """데이터 Excel로 내보내기"""
    from io import BytesIO

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if data_type == 'supplier':
        data = get_sales_by_supplier()
        df = pd.DataFrame(data)
        filename = f'업체별_매출_{timestamp}.xlsx'
    elif data_type == 'category':
        data = get_sales_by_category()
        df = pd.DataFrame(data)
        filename = f'카테고리별_매출_{timestamp}.xlsx'
    elif data_type == 'products':
        data = get_top_products()
        df = pd.DataFrame(data)
        filename = f'베스트셀러_{timestamp}.xlsx'
    elif data_type == 'daily':
        data = get_daily_sales()
        df = pd.DataFrame(data)
        filename = f'일별매출_{timestamp}.xlsx'
    elif data_type == 'store':
        data = get_store_sales()
        df = pd.DataFrame(data)
        filename = f'매장별_매출_{timestamp}.xlsx'
    elif data_type == 'matrix':
        data = flatten_supplier_category_matrix(get_supplier_category_matrix())
        df = pd.DataFrame(data)
        filename = f'업체_카테고리_상품_{timestamp}.xlsx'
    elif data_type == 'monthly':
        data = get_monthly_sales()
        df = pd.DataFrame(data)
        filename = f'월별_매출_{timestamp}.xlsx'
    else:
        return jsonify({'error': 'Invalid data type'}), 400

    # 메모리에서 Excel 파일 생성 (Vercel 서버리스 호환)
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    return send_file(output, as_attachment=True, download_name=filename,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/save-report', methods=['POST', 'GET'])
@login_required
def save_report():
    """현재 대시보드 데이터를 리포트로 다운로드"""
    from io import BytesIO

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'판매현황_종합리포트_{timestamp}.xlsx'

    # 메모리에서 Excel 파일 생성 (Vercel 서버리스 호환)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame(get_sales_by_supplier()).to_excel(writer, sheet_name='업체별매출', index=False)
        pd.DataFrame(get_sales_by_category()).to_excel(writer, sheet_name='카테고리별', index=False)
        pd.DataFrame(get_top_products()).to_excel(writer, sheet_name='베스트셀러', index=False)
        pd.DataFrame(get_daily_sales()).to_excel(writer, sheet_name='일별매출', index=False)
        pd.DataFrame(get_store_sales()).to_excel(writer, sheet_name='매장별', index=False)
        pd.DataFrame(flatten_supplier_category_matrix(get_supplier_category_matrix())).to_excel(writer, sheet_name='업체_카테고리_상품', index=False)

    output.seek(0)

    # 메모리에서 직접 반환 (파일 저장 없이)
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# Vercel 서버리스에서는 앱 로드 시 DB 초기화
init_database()

if __name__ == '__main__':
    print("데이터베이스 초기화 중...")
    print(f"DB 경로: {DB_PATH if 'DB_PATH' in dir() else 'N/A'}")
    print("서버 시작: http://localhost:8080")
    app.run(debug=True, host='0.0.0.0', port=8080)
