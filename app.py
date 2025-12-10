from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import os
from functools import wraps
from datetime import datetime
import json

# 데이터베이스 모듈 임포트
from database import (
    init_database,
    save_upload_file, save_sales_data, save_monthly_data,
    get_upload_files, delete_file_data,
    get_summary_stats, get_sales_by_supplier, get_sales_by_category,
    get_top_products, get_daily_sales, get_monthly_sales, get_store_sales,
    get_supplier_category_matrix, parse_classification,
    verify_admin, change_password, get_admin_info
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
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE upload_files SET row_count = ? WHERE id = ?', (total_rows, file_id))
            conn.commit()
            conn.close()

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
        username = request.form.get('username')
        password = request.form.get('password')

        user = verify_admin(username, password)
        if user:
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            error = '아이디 또는 비밀번호가 올바르지 않습니다.'

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
    files = get_upload_files()
    return render_template('upload.html', files=files)

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

@app.route('/api/summary')
@login_required
def api_summary():
    stats = get_summary_stats()
    return jsonify(stats)

@app.route('/api/sales-by-supplier')
@login_required
def api_sales_by_supplier():
    data = get_sales_by_supplier()
    return jsonify(data)

@app.route('/api/sales-by-category')
@login_required
def api_sales_by_category():
    data = get_sales_by_category()
    return jsonify(data)

@app.route('/api/top-products')
@login_required
def api_top_products():
    data = get_top_products()
    return jsonify(data)

@app.route('/api/daily-sales')
@login_required
def api_daily_sales():
    data = get_daily_sales()
    return jsonify(data)

@app.route('/api/monthly-sales')
@login_required
def api_monthly_sales():
    data = get_monthly_sales()
    return jsonify(data)

@app.route('/api/store-sales')
@login_required
def api_store_sales():
    data = get_store_sales()
    return jsonify(data)

@app.route('/api/supplier-category')
@login_required
def api_supplier_category():
    data = get_supplier_category_matrix()
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
        data = get_supplier_category_matrix()
        df = pd.DataFrame(data)
        filename = f'업체_카테고리_매트릭스_{timestamp}.xlsx'
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
        pd.DataFrame(get_supplier_category_matrix()).to_excel(writer, sheet_name='업체_카테고리', index=False)

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
    print("서버 시작: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
