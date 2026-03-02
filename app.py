import re
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import hashlib

# --- 設定頁面 ---
st.set_page_config(page_title="工程日報與利潤分析系統", layout="wide")

# --- 密碼加密 ---
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# --- 資料庫初始化 ---
def init_database():
    conn = sqlite3.connect('construction_reports_v6.db', check_same_thread=False)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS project_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT UNIQUE NOT NULL,
        daily_wage INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS building_floors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT,
        building_name TEXT,
        floor_name TEXT,
        UNIQUE(project_name, building_name, floor_name),
        FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS floor_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT,
        building_name TEXT,
        floor_name TEXT,
        item_name TEXT,
        standard_quantity REAL,
        unit TEXT,
        unit_price REAL,
        UNIQUE(project_name, building_name, floor_name, item_name),
        FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS materials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT,
        material_name TEXT,
        unit TEXT,
        unit_price REAL,
        UNIQUE(project_name, material_name),
        FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id TEXT UNIQUE,
        date TEXT,
        project_name TEXT,
        building_name TEXT,
        floor_name TEXT,
        workers TEXT,
        worker_count REAL,
        labor_cost REAL,
        description TEXT,
        photo_count INTEGER,
        created_by TEXT,
        revenue REAL DEFAULT 0,
        material_cost REAL DEFAULT 0,
        total_cost REAL DEFAULT 0,
        profit REAL DEFAULT 0,
        efficiency REAL DEFAULT 0,
        total_quantity REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS report_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id TEXT,
        item_name TEXT,
        quantity REAL,
        unit TEXT,
        unit_price REAL,
        revenue REAL,
        completion_days REAL DEFAULT 1,
        worker_count REAL DEFAULT 0,
        is_custom BOOLEAN DEFAULT 0,
        FOREIGN KEY (report_id) REFERENCES reports(report_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS report_materials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id TEXT,
        material_name TEXT,
        quantity REAL DEFAULT 0,
        unit TEXT,
        unit_price REAL,
        cost REAL DEFAULT 0,
        FOREIGN KEY (report_id) REFERENCES reports(report_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS photos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id TEXT,
        filename TEXT,
        photo_data BLOB,
        FOREIGN KEY (report_id) REFERENCES reports(report_id)
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS extra_work_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT,
        item_name TEXT,
        UNIQUE(project_name, item_name),
        FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
    )''')
    
    # 項目完成鎖定表
    c.execute('''CREATE TABLE IF NOT EXISTS completed_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_name TEXT,
        building_name TEXT,
        floor_name TEXT,
        item_name TEXT,
        locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        locked_by TEXT,
        UNIQUE(project_name, building_name, floor_name, item_name)
    )''')
    
    # Migrate: Add remark column
    try:
        c.execute('ALTER TABLE report_items ADD COLUMN remark TEXT DEFAULT ""')
    except sqlite3.OperationalError:
        pass
    
    # 預設管理員
    c.execute('SELECT COUNT(*) FROM users WHERE role = "admin"')
    if c.fetchone()[0] == 0:
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                 ('admin', hash_password('admin123'), 'admin'))
    
    conn.commit()
    return conn

conn = init_database()

# --- 用戶認證函數 ---
def authenticate(username, password):
    c = conn.cursor()
    c.execute('SELECT role FROM users WHERE username = ? AND password = ?', 
             (username, hash_password(password)))
    result = c.fetchone()
    return result[0] if result else None

def create_user(username, password, role):
    c = conn.cursor()
    try:
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                 (username, hash_password(password), role))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def get_all_users():
    c = conn.cursor()
    c.execute('SELECT id, username, role, created_at FROM users ORDER BY created_at DESC')
    return c.fetchall()

def delete_user(user_id):
    c = conn.cursor()
    c.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()

def change_password(username, new_password):
    c = conn.cursor()
    c.execute('UPDATE users SET password = ? WHERE username = ?',
             (hash_password(new_password), username))
    conn.commit()

# --- 案場管理函數 ---
def get_all_projects():
    c = conn.cursor()
    c.execute('SELECT project_name FROM project_settings ORDER BY created_at')
    return [row[0] for row in c.fetchall()]

def get_project_buildings(project_name):
    c = conn.cursor()
    c.execute('SELECT DISTINCT building_name FROM building_floors WHERE project_name = ? ORDER BY building_name', (project_name,))
    return [row[0] for row in c.fetchall()]

def get_building_floors(project_name, building_name):
    c = conn.cursor()
    c.execute('SELECT floor_name FROM building_floors WHERE project_name = ? AND building_name = ? ORDER BY id', (project_name, building_name))
    return [row[0] for row in c.fetchall()]

def get_floor_items(project_name, building_name, floor_name):
    c = conn.cursor()
    c.execute('''SELECT item_name, standard_quantity, unit, unit_price 
                FROM floor_items WHERE project_name = ? AND building_name = ? AND floor_name = ?
                ORDER BY item_name''', (project_name, building_name, floor_name))
    return c.fetchall()

def get_project_materials(project_name):
    c = conn.cursor()
    c.execute('SELECT material_name, unit, unit_price FROM materials WHERE project_name = ? ORDER BY material_name', (project_name,))
    return c.fetchall()

def get_project_wage(project_name):
    c = conn.cursor()
    c.execute('SELECT daily_wage FROM project_settings WHERE project_name = ?', (project_name,))
    result = c.fetchone()
    return result[0] if result else 2500

def create_project(project_name, copy_from=None):
    c = conn.cursor()
    try:
        wage = get_project_wage(copy_from) if copy_from else 2500
        c.execute('INSERT INTO project_settings (project_name, daily_wage) VALUES (?, ?)', (project_name, wage))
        if copy_from:
            c.execute('''INSERT INTO building_floors (project_name, building_name, floor_name)
                        SELECT ?, building_name, floor_name FROM building_floors WHERE project_name = ?''', (project_name, copy_from))
            c.execute('''INSERT INTO floor_items (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                        SELECT ?, building_name, floor_name, item_name, standard_quantity, unit, unit_price FROM floor_items WHERE project_name = ?''', (project_name, copy_from))
            c.execute('''INSERT INTO materials (project_name, material_name, unit, unit_price)
                        SELECT ?, material_name, unit, unit_price FROM materials WHERE project_name = ?''', (project_name, copy_from))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_project(project_name):
    c = conn.cursor()
    c.execute('SELECT report_id FROM reports WHERE project_name = ?', (project_name,))
    for (rid,) in c.fetchall():
        c.execute('DELETE FROM photos WHERE report_id = ?', (rid,))
        c.execute('DELETE FROM report_items WHERE report_id = ?', (rid,))
        c.execute('DELETE FROM report_materials WHERE report_id = ?', (rid,))
    c.execute('DELETE FROM reports WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM building_floors WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM floor_items WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM materials WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM completed_items WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM extra_work_items WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM project_settings WHERE project_name = ?', (project_name,))
    conn.commit()

def add_building_floor(project_name, building_name, floor_name):
    c = conn.cursor()
    try:
        c.execute('INSERT INTO building_floors (project_name, building_name, floor_name) VALUES (?, ?, ?)',
                 (project_name, building_name, floor_name))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def batch_add_floors(project_name, building_name, start_floor, end_floor, copy_items_from_floor=None):
    c = conn.cursor()
    success_count = 0
    source_items = get_floor_items(project_name, building_name, copy_items_from_floor) if copy_items_from_floor else []
    try:
        for i in range(start_floor, end_floor + 1):
            floor_name = f"{i}F"
            c.execute('SELECT COUNT(*) FROM building_floors WHERE project_name=? AND building_name=? AND floor_name=?',
                     (project_name, building_name, floor_name))
            if c.fetchone()[0] == 0:
                c.execute('INSERT INTO building_floors (project_name, building_name, floor_name) VALUES (?, ?, ?)',
                         (project_name, building_name, floor_name))
                for item_name, std_qty, unit, unit_price in source_items:
                    c.execute('''INSERT INTO floor_items (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                                VALUES (?, ?, ?, ?, ?, ?, ?)''', (project_name, building_name, floor_name, item_name, std_qty, unit, unit_price))
                success_count += 1
        conn.commit()
        return success_count
    except Exception as e:
        print(f"Error: {e}")
        return 0

def copy_materials_from_project(target_project, source_project):
    c = conn.cursor()
    try:
        c.execute('''INSERT OR IGNORE INTO materials (project_name, material_name, unit, unit_price)
                    SELECT ?, material_name, unit, unit_price FROM materials WHERE project_name = ?''', (target_project, source_project))
        conn.commit()
        return True
    except:
        return False

def delete_building_floor(project_name, building_name, floor_name):
    c = conn.cursor()
    c.execute('DELETE FROM floor_items WHERE project_name = ? AND building_name = ? AND floor_name = ?', (project_name, building_name, floor_name))
    c.execute('DELETE FROM building_floors WHERE project_name = ? AND building_name = ? AND floor_name = ?', (project_name, building_name, floor_name))
    conn.commit()

def add_floor_item(project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price):
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO floor_items (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''', (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_floor_item(project_name, building_name, floor_name, item_name):
    c = conn.cursor()
    c.execute('DELETE FROM floor_items WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?',
             (project_name, building_name, floor_name, item_name))
    conn.commit()

def add_material(project_name, material_name, unit, unit_price):
    c = conn.cursor()
    try:
        c.execute('INSERT INTO materials (project_name, material_name, unit, unit_price) VALUES (?, ?, ?, ?)',
                 (project_name, material_name, unit, unit_price))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_material(project_name, material_name):
    c = conn.cursor()
    c.execute('DELETE FROM materials WHERE project_name = ? AND material_name = ?', (project_name, material_name))
    conn.commit()

def update_project_wage(project_name, wage):
    c = conn.cursor()
    c.execute('UPDATE project_settings SET daily_wage = ? WHERE project_name = ?', (wage, project_name))
    conn.commit()

# --- 項目鎖定函數 ---
def is_item_locked(project_name, building_name, floor_name, item_name):
    c = conn.cursor()
    c.execute('''SELECT COUNT(*) FROM completed_items WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?''',
             (project_name, building_name, floor_name, item_name))
    return c.fetchone()[0] > 0

def lock_item(project_name, building_name, floor_name, item_name, locked_by):
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO completed_items (project_name, building_name, floor_name, item_name, locked_by) VALUES (?, ?, ?, ?, ?)''',
                 (project_name, building_name, floor_name, item_name, locked_by))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def unlock_item(project_name, building_name, floor_name, item_name):
    c = conn.cursor()
    c.execute('''DELETE FROM completed_items WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?''',
             (project_name, building_name, floor_name, item_name))
    conn.commit()

def get_locked_items(project_name, building_name, floor_name):
    c = conn.cursor()
    c.execute('''SELECT item_name FROM completed_items WHERE project_name = ? AND building_name = ? AND floor_name = ?''',
             (project_name, building_name, floor_name))
    return [row[0] for row in c.fetchall()]

# --- 額外工項 ---
def get_extra_work_items(project_name):
    c = conn.cursor()
    c.execute('SELECT item_name FROM extra_work_items WHERE project_name = ? ORDER BY item_name', (project_name,))
    return [row[0] for row in c.fetchall()]

def add_extra_work_item(project_name, item_name):
    c = conn.cursor()
    try:
        c.execute('INSERT INTO extra_work_items (project_name, item_name) VALUES (?, ?)', (project_name, item_name))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_extra_work_item(project_name, item_name):
    c = conn.cursor()
    c.execute('DELETE FROM extra_work_items WHERE project_name = ? AND item_name = ?', (project_name, item_name))
    conn.commit()

# --- 日報操作函數 ---
def save_report(report_data, items, materials, photos=None):
    c = conn.cursor()
    total_quantity = sum(item['quantity'] for item in items)
    c.execute('''INSERT INTO reports (report_id, date, project_name, building_name, floor_name, workers, 
                   worker_count, labor_cost, description, photo_count, created_by, 
                   revenue, material_cost, total_cost, profit, efficiency, total_quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (report_data['report_id'], report_data['date'], report_data['project_name'],
         report_data['building_name'], report_data['floor_name'], report_data['workers'],
         report_data['worker_count'], report_data['labor_cost'], report_data['description'],
         report_data['photo_count'], report_data['created_by'], report_data['revenue'],
         report_data['material_cost'], report_data['total_cost'], report_data['profit'],
         report_data['efficiency'], total_quantity))
    
    for item in items:
        remark = item.get('remark', '')
        c.execute('''INSERT INTO report_items (report_id, item_name, quantity, unit, unit_price, revenue, 
                        completion_days, worker_count, is_custom, remark) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (report_data['report_id'], item['item_name'], item['quantity'], item['unit'],
             item['unit_price'], item['revenue'], item['completion_days'], item['worker_count'],
             item.get('is_custom', False), remark))
    
    for mat in materials:
        c.execute('''INSERT INTO report_materials (report_id, material_name, quantity, unit, unit_price, cost)
            VALUES (?, ?, ?, ?, ?, ?)''',
            (report_data['report_id'], mat['material_name'], mat['quantity'], mat['unit'], mat['unit_price'], mat['cost']))
    
    if photos:
        for photo in photos:
            c.execute('INSERT INTO photos (report_id, filename, photo_data) VALUES (?, ?, ?)',
                     (report_data['report_id'], photo['name'], photo['data']))
    conn.commit()

def update_report(report_id, report_data, items, materials, photos=None):
    c = conn.cursor()
    total_quantity = sum(item['quantity'] for item in items)
    c.execute('''UPDATE reports SET date=?, building_name=?, floor_name=?, workers=?, worker_count=?, labor_cost=?,
                description=?, photo_count=?, revenue=?, material_cost=?, total_cost=?, profit=?, 
                efficiency=?, total_quantity=?, updated_at=CURRENT_TIMESTAMP WHERE report_id=?''',
        (report_data['date'], report_data['building_name'], report_data['floor_name'],
         report_data['workers'], report_data['worker_count'], report_data['labor_cost'],
         report_data['description'], report_data['photo_count'], report_data['revenue'],
         report_data['material_cost'], report_data['total_cost'], report_data['profit'],
         report_data['efficiency'], total_quantity, report_id))
    
    c.execute('DELETE FROM report_items WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM report_materials WHERE report_id = ?', (report_id,))
    
    for item in items:
        remark = item.get('remark', '')
        c.execute('''INSERT INTO report_items (report_id, item_name, quantity, unit, unit_price, revenue,
                        completion_days, worker_count, is_custom, remark) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (report_id, item['item_name'], item['quantity'], item['unit'], item['unit_price'],
             item['revenue'], item['completion_days'], item['worker_count'], item.get('is_custom', False), remark))
    
    for mat in materials:
        c.execute('''INSERT INTO report_materials (report_id, material_name, quantity, unit, unit_price, cost)
            VALUES (?, ?, ?, ?, ?, ?)''',
            (report_id, mat['material_name'], mat['quantity'], mat['unit'], mat['unit_price'], mat['cost']))
    
    if photos:
        for photo in photos:
            c.execute('INSERT INTO photos (report_id, filename, photo_data) VALUES (?, ?, ?)',
                     (report_id, photo['name'], photo['data']))
    conn.commit()

def load_all_reports(project_filter=None, building_filter=None, date_from=None, date_to=None, creator_filter=None):
    c = conn.cursor()
    query = '''SELECT report_id, date, project_name, building_name, floor_name, workers, worker_count,
               labor_cost, revenue, material_cost, total_cost, profit, efficiency, total_quantity,
               description, photo_count, created_by, updated_at FROM reports WHERE 1=1'''
    params = []
    if project_filter:
        query += ' AND project_name = ?'; params.append(project_filter)
    if building_filter:
        query += ' AND building_name = ?'; params.append(building_filter)
    if date_from:
        query += ' AND date >= ?'; params.append(date_from)
    if date_to:
        query += ' AND date <= ?'; params.append(date_to)
    if creator_filter:
        query += ' AND created_by = ?'; params.append(creator_filter)
    query += ' ORDER BY date DESC, created_at DESC'
    c.execute(query, params)
    rows = c.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=[
        'id', '日期', '案場', '棟別', '樓層', '人員', '工數',
        '人力成本', '產值', '材料成本', '總成本', '利潤', '工率', '總數量',
        '施工描述', '照片數', '建立者', '更新時間'])

def load_report_items(report_id):
    c = conn.cursor()
    c.execute('''SELECT item_name, quantity, unit, unit_price, revenue, completion_days, worker_count, is_custom, remark 
                FROM report_items WHERE report_id = ?''', (report_id,))
    rows = c.fetchall()
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows, columns=['項目名稱', '數量', '單位', '單價', '產值', '完成天數', '計工', 'is_custom', '備註'])

def load_report_materials(report_id):
    c = conn.cursor()
    c.execute('SELECT material_name, quantity, unit, unit_price, cost FROM report_materials WHERE report_id = ?', (report_id,))
    rows = c.fetchall()
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows, columns=['材料名稱', '數量', '單位', '單價', '成本'])

def get_report_detail(report_id):
    c = conn.cursor()
    c.execute('''SELECT date, project_name, building_name, floor_name, workers, worker_count,
                        labor_cost, description, photo_count, revenue, material_cost, total_cost, profit
                FROM reports WHERE report_id = ?''', (report_id,))
    return c.fetchone()

def load_photos(report_id):
    c = conn.cursor()
    c.execute('SELECT filename, photo_data FROM photos WHERE report_id = ?', (report_id,))
    return [{'name': fn, 'data': data} for fn, data in c.fetchall()]

def delete_report(report_id):
    c = conn.cursor()
    c.execute('DELETE FROM photos WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM report_items WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM report_materials WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM reports WHERE report_id = ?', (report_id,))
    conn.commit()

def delete_all_reports():
    c = conn.cursor()
    c.execute('DELETE FROM photos')
    c.execute('DELETE FROM report_items')
    c.execute('DELETE FROM report_materials')
    c.execute('DELETE FROM reports')
    conn.commit()


# ============================================================
# [核心修正] 樓層+工項 累計成本與利潤計算函數
# ============================================================
def calc_floor_item_stats(project_name, building_name=None, floor_name=None, 
                          date_from=None, date_to=None):
    """
    以「棟別 + 樓層 + 工項」為單位計算累計統計。
    
    核心邏輯（使用者回饋）：
    - 數量(quantity): 取 MAX（樓層固定量，不因多天填報累加）
    - 工數(worker_count): 取 SUM（所有天數的實際投入累加）
    - 產值 = MAX(quantity) * unit_price
    - 累計工成本 = SUM(worker_count) * daily_wage（包含施工中的天數）
    - 利潤 = 產值 - 累計工成本（完成時一次結算，扣除所有施工中天數的成本）
    
    施工中（quantity=0）: 只累計工成本，產值=0，利潤為負（預支成本）
    完成（quantity>0）: 產值認列，利潤 = 產值 - 全部累計工成本
    """
    c = conn.cursor()
    
    daily_wage = get_project_wage(project_name)
    
    # 查詢工項統計
    query = '''
        SELECT 
            r.building_name,
            r.floor_name,
            ri.item_name,
            ri.unit,
            MAX(ri.quantity) as max_qty,
            SUM(ri.worker_count) as total_workers,
            MAX(ri.unit_price) as unit_price,
            SUM(CASE WHEN ri.quantity = 0 AND ri.worker_count > 0 THEN 1 ELSE 0 END) as in_progress_count,
            COUNT(DISTINCT r.date) as work_days,
            ri.is_custom
        FROM reports r
        JOIN report_items ri ON r.report_id = ri.report_id
        WHERE r.project_name = ?
    '''
    params = [project_name]
    
    if building_name:
        query += ' AND r.building_name = ?'; params.append(building_name)
    if floor_name:
        query += ' AND r.floor_name LIKE ?'; params.append(f'%{floor_name}%')
    if date_from:
        query += ' AND r.date >= ?'; params.append(date_from)
    if date_to:
        query += ' AND r.date <= ?'; params.append(date_to)
    
    query += ' GROUP BY r.building_name, r.floor_name, ri.item_name ORDER BY r.building_name, r.floor_name, ri.item_name'
    
    c.execute(query, params)
    item_data = c.fetchall()
    
    # 查詢材料費（依樓層彙總）
    mat_query = '''
        SELECT r.building_name, r.floor_name, SUM(rm.quantity * rm.unit_price) as total_mat_cost
        FROM reports r
        JOIN report_materials rm ON r.report_id = rm.report_id
        WHERE r.project_name = ?
    '''
    mat_params = [project_name]
    if building_name:
        mat_query += ' AND r.building_name = ?'; mat_params.append(building_name)
    if floor_name:
        mat_query += ' AND r.floor_name LIKE ?'; mat_params.append(f'%{floor_name}%')
    if date_from:
        mat_query += ' AND r.date >= ?'; mat_params.append(date_from)
    if date_to:
        mat_query += ' AND r.date <= ?'; mat_params.append(date_to)
    mat_query += ' GROUP BY r.building_name, r.floor_name'
    
    c.execute(mat_query, mat_params)
    mat_cost_map = {}
    for b, f, cost in c.fetchall():
        mat_cost_map[(b, f)] = cost
    
    if not item_data:
        return None, mat_cost_map, daily_wage
    
    rows = []
    for bldg, floor, item, unit, max_qty, total_workers, price, in_prog, work_days, is_custom in item_data:
        revenue = max_qty * price  # 產值 = MAX數量 * 單價
        labor_cost = total_workers * daily_wage  # 累計工成本 = 所有天數工數之和 * 日薪
        
        # 狀態判斷
        if max_qty > 0 and in_prog == 0:
            status = "✅ 已完成"
        elif max_qty > 0 and in_prog > 0:
            status = "⚠️ 部分完成"
        else:
            status = "🔄 施工中"
        
        # 項目利潤（僅扣工，不含料）
        item_profit = revenue - labor_cost
        
        rows.append({
            '棟別': bldg, '樓層': floor, '工項': item, '單位': unit,
            '數量': max_qty, '累計工數': total_workers, '單價': price,
            '累計產值': revenue, '累計工成本': labor_cost,
            '項目利潤(僅扣工)': item_profit,
            '狀態': status, '施工天數': work_days, 'is_custom': is_custom
        })
    
    df = pd.DataFrame(rows)
    return df, mat_cost_map, daily_wage


# ============================================================
# 日報表單渲染
# ============================================================
def render_report_form(is_editing, edit_data=None):
    current_role = st.session_state.get('role', 'user')
    project_names = get_all_projects()
    
    if not project_names:
        st.warning("請先新增案場" if current_role == 'admin' else "請聯絡管理員新增案場")
        return None
    
    if is_editing and edit_data:
        (edit_date, edit_project, edit_building, edit_floor, edit_workers, 
         edit_worker_count, edit_labor_cost, edit_desc, edit_photo_count,
         edit_revenue, edit_material_cost, edit_total_cost, edit_profit) = edit_data['report_detail']
        edit_items_df = edit_data['items']
        edit_materials_df = edit_data['materials']
        edit_photos = edit_data['photos']
        report_id = edit_data['report_id']
        edit_floor_list = [f.strip() for f in edit_floor.split(',')] if ',' in edit_floor else [edit_floor]
    else:
        edit_date = edit_project = edit_building = None
        edit_floor_list = []
        edit_workers = st.session_state.username
        edit_desc = ""
        edit_items_df = edit_materials_df = pd.DataFrame()
        edit_photos = []
        report_id = None
    
    selected_project = st.selectbox(
        "選擇案場", project_names,
        index=project_names.index(edit_project) if is_editing and edit_project in project_names else 0,
        key=f"project_{report_id if report_id else 'new'}")
    
    if not selected_project: return None
    buildings = get_project_buildings(selected_project)
    if not buildings:
        st.warning("此案場尚未設定棟別樓層"); return None
    
    col1, col2 = st.columns(2)
    with col1:
        date = st.date_input("日期", datetime.strptime(edit_date, "%Y-%m-%d") if is_editing else datetime.now())
        selected_building = st.selectbox("棟別", buildings,
            index=buildings.index(edit_building) if is_editing and edit_building in buildings else 0)
        
        if selected_building:
            floors = get_building_floors(selected_project, selected_building)
            if floors:
                default_floors = [f for f in edit_floor_list if f in floors] if is_editing else []
                selected_floors = st.multiselect("樓層 (可多選)", floors, default=default_floors)
                reference_floor = selected_floors[0] if selected_floors else (floors[0] if floors else None)
                selected_floor_str = ", ".join(selected_floors)
            else:
                st.warning("此棟別尚未設定樓層"); return None
        else:
            selected_floors = []; reference_floor = None; selected_floor_str = ""
        
        workers = st.text_input("施工人員姓名（用逗號分隔）", 
            edit_workers if is_editing else st.session_state.username,
            help="請填寫今日出工人名，例如：王大明, 李小華")
    
    with col2:
        progress_desc = st.text_area("施工進度描述", value=edit_desc if is_editing else "",
            height=200, placeholder="例如：\n- A棟2F浴室防水完成\n- 素地整理完成")
    
    rt_total_qty = 0; rt_total_workers = 0; rt_total_revenue = 0
    selected_items = []
    
    if reference_floor:
        st.write("---")
        locked_items = get_locked_items(selected_project, selected_building, reference_floor)
        
        if current_role == 'admin':
            st.subheader("完成項目與計工 (管理員模式)")
            if locked_items:
                st.info(f"🔒 已鎖定項目：{', '.join(locked_items)}（如需修改請先解鎖）")
        else:
            st.subheader("今日施工項目回報")
            st.caption("請勾選今日有施作的項目，並填寫投入工數。勾選「施工中」表示尚未完成，僅記錄工數。")
        
        floor_items = get_floor_items(selected_project, selected_building, reference_floor)
        
        if floor_items:
            edit_items_dict = {}
            if is_editing and not edit_items_df.empty:
                for _, item_row in edit_items_df[edit_items_df['is_custom'] == 0].iterrows():
                    edit_items_dict[item_row['項目名稱']] = {
                        'quantity': item_row['數量'], 'completion_days': item_row['完成天數'],
                        'worker_count': item_row['計工']}
            
            if current_role == 'admin':
                st.markdown("標準項目清單")
                h1, h2, h3, h5, h6 = st.columns([2, 1, 1.2, 1, 0.8])
                h1.caption("項目名稱"); h2.caption("狀態"); h3.caption("數量"); h5.caption("今日計工"); h6.caption("鎖定")
            else:
                st.markdown("今日施作項目")
                h1, h2, h5 = st.columns([3, 1.5, 1.5])
                h1.caption("勾選施作項目"); h2.caption("狀態"); h5.caption("投入工數 (人天)")

            for item_name, std_qty, unit, unit_price in floor_items:
                default_checked = item_name in edit_items_dict
                item_is_locked = item_name in locked_items
                default_qty = edit_items_dict[item_name]['quantity'] if default_checked else (std_qty if current_role == 'admin' else 0.0)
                default_workers = edit_items_dict[item_name]['worker_count'] if default_checked else 0.0
                is_prep_default = default_checked and edit_items_dict[item_name]['quantity'] == 0

                if current_role == 'admin':
                    col_check, col_prep, col_qty, col_workers, col_lock = st.columns([2, 1, 1.2, 1, 0.8])
                    with col_check:
                        label = f"🔒 {item_name}" if item_is_locked else item_name
                        checked = st.checkbox(label, value=default_checked or item_is_locked,
                                            key=f"item_{item_name}_{report_id}", disabled=item_is_locked)
                    
                    if checked or item_is_locked:
                        with col_prep:
                            if item_is_locked:
                                st.write("✅ 已完成"); is_prep = False
                            else:
                                is_prep = st.checkbox("施工中", value=is_prep_default, key=f"prep_{item_name}_{report_id}")
                        with col_qty:
                            if item_is_locked:
                                st.write(f"{default_qty} {unit}"); qty = default_qty
                            else:
                                qty = st.number_input(f"數量({unit})", value=0.0 if is_prep else float(default_qty),
                                    step=0.5, disabled=is_prep, key=f"qty_{item_name}_{report_id}", label_visibility="collapsed")
                        with col_workers:
                            if item_is_locked:
                                st.write(f"{default_workers}"); item_workers = default_workers
                            else:
                                item_workers = st.number_input("計工", value=float(default_workers), step=0.5,
                                    key=f"workers_{item_name}_{report_id}", label_visibility="collapsed")
                        with col_lock:
                            if item_is_locked:
                                if st.button("🔓", key=f"unlock_{item_name}_{report_id}", help="解鎖此項目"):
                                    unlock_item(selected_project, selected_building, reference_floor, item_name); st.rerun()
                            elif qty > 0 and not is_prep:
                                if st.button("🔒", key=f"lock_{item_name}_{report_id}", help="鎖定（完成後不可修改）"):
                                    lock_item(selected_project, selected_building, reference_floor, item_name, st.session_state.username); st.rerun()
                    else:
                        continue
                else:
                    col_check, col_prep, col_workers = st.columns([3, 1.5, 1.5])
                    with col_check:
                        label = f"🔒 {item_name} (已完成)" if item_is_locked else item_name
                        checked = st.checkbox(label, value=default_checked, key=f"item_{item_name}_{report_id}", disabled=item_is_locked)
                    if checked:
                        with col_prep:
                            is_prep = st.checkbox("施工中 (未完工)", value=is_prep_default, key=f"prep_{item_name}_{report_id}")
                        with col_workers:
                            item_workers = st.number_input("投入工數", min_value=0.0, value=float(default_workers),
                                step=0.5, key=f"workers_{item_name}_{report_id}", label_visibility="collapsed")
                        qty = 0.0 if is_prep else float(default_qty)
                    elif item_is_locked:
                        continue
                    else:
                        continue

                if checked or item_is_locked:
                    # 只要是施工中，數量一律歸零，避免 Streamlit 暫存值累加
                    if not item_is_locked and is_prep:
                        qty = 0.0
                    revenue = qty * unit_price
                    rt_total_qty += qty; rt_total_workers += item_workers; rt_total_revenue += revenue
                    selected_items.append({
                        'item_name': item_name, 'quantity': qty, 'unit': unit, 'unit_price': unit_price,
                        'revenue': revenue, 'completion_days': 1.0, 'worker_count': item_workers, 'is_custom': False})
        else:
            st.info(f"{reference_floor} 尚未設定標準項目")

        # --- 額外施作項目 ---
        st.write("---")
        st.markdown("額外施作項目 (點工/雜項)")
        extra_items_list = get_extra_work_items(selected_project)
        has_predefined_items = len(extra_items_list) > 0
        
        custom_key = f"custom_items_count_{report_id if report_id else 'new'}"
        if custom_key not in st.session_state:
            init_count = 1
            if is_editing and not edit_items_df.empty:
                custom_df = edit_items_df[edit_items_df['is_custom'] == 1]
                if not custom_df.empty: init_count = len(custom_df) + 1
            st.session_state[custom_key] = init_count

        if st.button("＋ 增加項目", key=f"add_custom_btn_{report_id}"):
            st.session_state[custom_key] += 1; st.rerun()

        custom_data_list = []
        if is_editing and not edit_items_df.empty:
            for _, row in edit_items_df[edit_items_df['is_custom'] == 1].iterrows():
                custom_data_list.append(row)

        for i in range(st.session_state[custom_key]):
            if current_role == 'admin':
                cc1, cc2, cc3, cc4, cc5, cc6 = st.columns([2, 1, 0.8, 0.8, 0.8, 1.5])
            else:
                cc1, cc6, cc5 = st.columns([2.5, 2, 1.5])

            d_name, d_qty, d_unit, d_price, d_workers, d_remark = "", 0.0, "式", 0.0, 0.0, ""
            if i < len(custom_data_list):
                row = custom_data_list[i]
                d_name, d_qty, d_unit, d_price, d_workers = row['項目名稱'], row['數量'], row['單位'], row['單價'], row['計工']
                d_remark = row.get('備註', '')

            with cc1:
                if has_predefined_items:
                    options = [""] + extra_items_list + ["自訂..."]
                    default_idx = 0
                    if d_name in extra_items_list: default_idx = options.index(d_name)
                    elif d_name: default_idx = len(options) - 1
                    selected_item = st.selectbox("選擇工項", options, index=default_idx, key=f"c_select_{i}_{report_id}", label_visibility="collapsed")
                    if selected_item == "自訂...":
                        c_name = st.text_input("自訂項目", value=d_name if d_name not in extra_items_list else "", key=f"c_custom_name_{i}_{report_id}")
                    elif selected_item: c_name = selected_item
                    else: c_name = ""
                else:
                    c_name = st.text_input("項目名稱", value=d_name, key=f"c_name_{i}_{report_id}", placeholder="例如：清理現場")
            
            if current_role == 'admin':
                with cc2: c_qty = st.number_input("數量", value=float(d_qty), min_value=0.0, step=0.5, key=f"c_qty_{i}_{report_id}")
                with cc3: c_unit = st.text_input("單位", value=d_unit, key=f"c_unit_{i}_{report_id}")
                with cc4: c_price = st.number_input("單價", value=float(d_price), min_value=0.0, step=100.0, key=f"c_price_{i}_{report_id}")
                with cc5: c_workers = st.number_input("計工", value=float(d_workers), min_value=0.0, step=0.5, key=f"c_workers_{i}_{report_id}")
                with cc6: c_remark = st.text_input("備註", value=d_remark, key=f"c_remark_{i}_{report_id}", placeholder="例如：試水、抽水")
            else:
                with cc6: c_remark = st.text_input("備註", value=d_remark, key=f"c_remark_{i}_{report_id}", placeholder="例如：試水、抽水")
                c_qty, c_price, c_unit = 0.0, 0.0, "式"
                with cc5: c_workers = st.number_input("投入工數", value=float(d_workers), min_value=0.0, step=0.5, key=f"c_workers_{i}_{report_id}")

            if c_name:
                c_revenue = c_qty * c_price
                rt_total_qty += c_qty; rt_total_workers += c_workers; rt_total_revenue += c_revenue
                selected_items.append({
                    'item_name': c_name, 'quantity': c_qty, 'unit': c_unit, 'unit_price': c_price,
                    'revenue': c_revenue, 'completion_days': 1.0, 'worker_count': c_workers,
                    'is_custom': True, 'remark': c_remark})
        
        # --- 材料 ---
        st.write("---")
        st.subheader("使用材料")
        st.caption("請勾選今日使用的材料")
        
        materials = get_project_materials(selected_project)
        selected_materials = []
        
        if materials:
            edit_materials_dict = {}
            if is_editing and not edit_materials_df.empty:
                for _, mat_row in edit_materials_df.iterrows():
                    edit_materials_dict[mat_row['材料名稱']] = mat_row['數量']
            
            for i in range(0, len(materials), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(materials):
                        mat_name, mat_unit, mat_price = materials[i + j]
                        default_mat_checked = mat_name in edit_materials_dict
                        default_mat_qty = edit_materials_dict.get(mat_name, 0.0)
                        with cols[j]:
                            mat_checked = st.checkbox(f"{mat_name}", value=default_mat_checked,
                                key=f"mat_{mat_name}_{report_id if report_id else 'new'}")
                            if mat_checked:
                                if current_role == 'admin':
                                    mat_qty = st.number_input(f"數量 ({mat_unit})", min_value=0.0, value=float(default_mat_qty),
                                        step=0.5, key=f"mat_qty_{mat_name}_{report_id if report_id else 'new'}")
                                else:
                                    mat_qty = 0.0; st.caption(f"已記錄 ({mat_unit})")
                                selected_materials.append({
                                    'material_name': mat_name, 'quantity': mat_qty,
                                    'unit': mat_unit, 'unit_price': mat_price, 'cost': mat_qty * mat_price})
        else:
            st.warning("此案場尚未設定材料")
        
        st.write("---")
        uploaded_files = st.file_uploader("上傳施工照片", accept_multiple_files=True, type=['png', 'jpg', 'jpeg'],
            key=f"photos_{report_id if report_id else 'new'}")
        
        if is_editing and edit_photos:
            st.write("現有照片：")
            cols = st.columns(min(3, len(edit_photos)))
            for i, photo in enumerate(edit_photos):
                with cols[i % 3]: st.image(photo['data'], caption=photo['name'], use_container_width=True)
        
        st.write("---")
        submitted = st.button("更新日報表" if is_editing else "提交日報表", use_container_width=True, type="primary")
        
        if submitted:
            if not workers.strip():
                st.error("請填寫施工人員！"); return None
            elif not selected_items and not progress_desc:
                st.error("請至少勾選一個項目或填寫施工描述！"); return None
            else:
                worker_list = [w.strip() for w in re.split(r'[,，、]', workers) if w.strip()]
                actual_worker_count = rt_total_workers if rt_total_workers > 0 else len(worker_list)
                wage = get_project_wage(selected_project)
                labor_cost = actual_worker_count * wage
                total_revenue = rt_total_revenue
                total_mat_cost = sum(m['cost'] for m in selected_materials)
                total_cost = labor_cost + total_mat_cost
                profit = total_revenue - total_cost
                efficiency = rt_total_qty / actual_worker_count if actual_worker_count > 0 else 0
                
                photo_list = [{'name': f.name, 'data': f.read()} for f in uploaded_files] if uploaded_files else []
                
                report_data = {
                    'date': date.strftime("%Y-%m-%d"), 'project_name': selected_project,
                    'building_name': selected_building, 'floor_name': selected_floor_str,
                    'workers': workers, 'worker_count': actual_worker_count,
                    'labor_cost': labor_cost, 'description': progress_desc,
                    'photo_count': (len(uploaded_files) if uploaded_files else 0) + (len(edit_photos) if is_editing else 0),
                    'created_by': st.session_state.username,
                    'revenue': total_revenue, 'material_cost': total_mat_cost,
                    'total_cost': total_cost, 'profit': profit, 'efficiency': efficiency}
                
                if is_editing:
                    update_report(report_id, report_data, selected_items, selected_materials, photo_list or None)
                    st.success("日報表已更新成功！")
                    if 'editing_report' in st.session_state: del st.session_state.editing_report
                else:
                    report_data['report_id'] = f"{date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
                    save_report(report_data, selected_items, selected_materials, photo_list or None)
                    st.success("日報表已提交成功！")
                
                if current_role == 'admin':
                    st.caption("⚠️ 以上為本筆日報的當日數據。樓層累計利潤請至「查看報表」的樓層統計查看。")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("當日產值", f"${total_revenue:,.0f}")
                    c2.metric("當日成本", f"${total_cost:,.0f}")
                    c3.metric("當日利潤", f"${profit:,.0f}")
                else:
                    st.info("資料已上傳，待管理員審核計算數量。")
                st.rerun()


# ============================================================
# 登入系統
# ============================================================
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.role = None

if not st.session_state.logged_in:
    st.title("工程日報與利潤分析系統 - 登入")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            username = st.text_input("帳號")
            password = st.text_input("密碼", type="password")
            if st.form_submit_button("登入", use_container_width=True):
                role = authenticate(username, password)
                if role:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.role = role
                    st.success(f"歡迎 {username} ({role})"); st.rerun()
                else:
                    st.error("帳號或密碼錯誤！")
else:
    col_nav1, col_nav2, col_nav3 = st.columns([3, 1, 1])
    with col_nav1: st.title("施工日報與利潤分析系統")
    with col_nav2: st.write(f"{st.session_state.username} ({st.session_state.role})")
    with col_nav3:
        if st.button("登出", use_container_width=True):
            st.session_state.logged_in = False; st.session_state.username = None; st.session_state.role = None; st.rerun()
    
    project_names = get_all_projects()
    
    if st.session_state.role == 'admin':
        tab1, tab2, tab_analysis, tab3, tab4 = st.tabs(["填寫/修改日報", "查看報表", "單項成本分析", "案場管理", "用戶管理"])
        
        # ========== Tab 1: 填寫/修改日報 ==========
        with tab1:
            st.subheader("我的日報表")
            my_reports = load_all_reports(creator_filter=st.session_state.username)
            if not my_reports.empty:
                st.info(f"共 {len(my_reports)} 筆日報")
                for idx, row in my_reports.head(10).iterrows():
                    with st.expander(f"{row['日期']} - {row['案場']} {row['棟別']} {row['樓層']}"):
                        ci, ce = st.columns([3, 1])
                        with ci:
                            st.write(f"人員：{row['人員']} ({row['工數']:.1f}工)")
                            st.write(f"當日產值：${row['產值']:,.0f} | 當日成本：${row['總成本']:,.0f}")
                            if row['更新時間']: st.caption(f"更新：{row['更新時間']}")
                        with ce:
                            if st.button("修改", key=f"edit_{row['id']}"):
                                st.session_state.editing_report = row['id']; st.rerun()
                st.write("---")
            
            is_editing = 'editing_report' in st.session_state and st.session_state.editing_report
            if is_editing:
                st.subheader("修改日報表")
                report_detail = get_report_detail(st.session_state.editing_report)
                if not report_detail:
                    st.error("找不到此日報"); del st.session_state.editing_report; st.rerun()
                edit_data = {
                    'report_id': st.session_state.editing_report, 'report_detail': report_detail,
                    'items': load_report_items(st.session_state.editing_report),
                    'materials': load_report_materials(st.session_state.editing_report),
                    'photos': load_photos(st.session_state.editing_report)}
                if st.button("取消修改"): del st.session_state.editing_report; st.rerun()
                render_report_form(True, edit_data)
            else:
                st.subheader("填寫新日報")
                render_report_form(False)
        
        # ========== Tab 2: 查看報表 ==========
        with tab2:
            st.subheader("查看與修正報表")
            st.caption("管理員可直接修改報表，修改後請點「保存修改」。")
            
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                filter_project_view = st.selectbox("案場", ["全部"] + project_names, key="filter_view")
            with col_f2:
                if filter_project_view != "全部":
                    filter_building_view = st.selectbox("棟別", ["全部"] + get_project_buildings(filter_project_view), key="filter_building_view")
                else:
                    filter_building_view = "全部"
            with col_f3:
                date_from = st.date_input("開始日期", None, key="date_from")
            with col_f4:
                date_to = st.date_input("結束日期", None, key="date_to")
            
            df = load_all_reports(
                None if filter_project_view == "全部" else filter_project_view,
                None if filter_building_view == "全部" else filter_building_view,
                date_from.strftime("%Y-%m-%d") if date_from else None,
                date_to.strftime("%Y-%m-%d") if date_to else None)
            
            if not df.empty:
                st.write("---")
                
                # ========== [核心修正] 樓層產值與工項統計 ==========
                st.subheader("📊 樓層累計統計（以樓層+工項為單位）")
                st.caption("""
                📌 **計算邏輯說明**：
                - **施工中**（數量=0）：只累計工成本，不計產值
                - **完成時**（數量>0）：產值一次認列，利潤 = 產值 - **所有天數累計工成本** - 材料費
                - 例：9F 防水施工 3 天(每天 3 工)，完成時產值 66,487 → 利潤 = 66,487 - (9工×2,500) - 材料 3,760 = **40,227**
                """)
                
                if filter_project_view != "全部":
                    floor_df, mat_cost_map, daily_wage = calc_floor_item_stats(
                        filter_project_view,
                        None if filter_building_view == "全部" else filter_building_view,
                        date_from=date_from.strftime("%Y-%m-%d") if date_from else None,
                        date_to=date_to.strftime("%Y-%m-%d") if date_to else None)
                    
                    if floor_df is not None and not floor_df.empty:
                        for building in floor_df['棟別'].unique():
                            building_data = floor_df[floor_df['棟別'] == building]
                            
                            with st.expander(f"🏢 {building} 統計明細", expanded=True):
                                for floor in building_data['樓層'].unique():
                                    fdata = building_data[building_data['樓層'] == floor]
                                    
                                    st.markdown(f"**{floor}**")
                                    
                                    # 顯示表格
                                    display_cols = ['工項', '數量', '單位', '累計工數', '累計產值', '累計工成本', '項目利潤(僅扣工)', '狀態', '施工天數']
                                    st.dataframe(
                                        fdata[display_cols],
                                        column_config={
                                            "累計產值": st.column_config.NumberColumn(format="$%d"),
                                            "累計工成本": st.column_config.NumberColumn(format="$%d", help="所有施工天數的工數 × 日薪"),
                                            "項目利潤(僅扣工)": st.column_config.NumberColumn(format="$%d", label="利潤(僅扣工)"),
                                            "累計工數": st.column_config.NumberColumn(format="%.1f"),
                                            "數量": st.column_config.NumberColumn(format="%.1f", help="樓層固定量(MAX)"),
                                            "施工天數": st.column_config.NumberColumn(help="有填報此工項的天數"),
                                        },
                                        use_container_width=True, hide_index=True)
                                    
                                    # ===== 樓層合計 =====
                                    f_rev = fdata['累計產值'].sum()
                                    f_labor = fdata['累計工成本'].sum()
                                    f_workers = fdata['累計工數'].sum()
                                    f_qty_sum = fdata['數量'].sum()
                                    f_mat_cost = mat_cost_map.get((building, floor), 0)
                                    
                                    # [核心] 樓層淨利 = 產值 - 累計工成本(含施工中天數) - 材料費
                                    f_net_profit = f_rev - f_labor - f_mat_cost
                                    f_efficiency = f_qty_sum / f_workers if f_workers > 0 else 0

                                    c1, c2, c3, c4, c5 = st.columns(5)
                                    c1.metric("樓層產值", f"${f_rev:,.0f}")
                                    c2.metric("累計工成本", f"${f_labor:,.0f}", help=f"總工數 {f_workers:.1f} × 日薪 {daily_wage}")
                                    c3.metric("材料成本", f"${f_mat_cost:,.0f}")
                                    c4.metric("樓層淨利潤", f"${f_net_profit:,.0f}", 
                                             delta=f"{(f_net_profit/f_rev*100) if f_rev>0 else 0:.1f}%",
                                             help="產值 - 累計工成本(含施工中) - 材料費")
                                    c5.metric("綜合工率", f"{f_efficiency:.2f}", help="所有項目數量和 / 總工數")
                                    st.write("---")
                                
                                # 棟別總計
                                bldg_rev = building_data['累計產值'].sum()
                                bldg_labor = building_data['累計工成本'].sum()
                                bldg_mat = sum(cost for (b, f), cost in mat_cost_map.items() if b == building)
                                bldg_profit = bldg_rev - bldg_labor - bldg_mat
                                
                                cb1, cb2, cb3 = st.columns(3)
                                cb1.metric(f"{building} 總產值", f"${bldg_rev:,.0f}")
                                cb2.metric(f"{building} 總成本", f"${bldg_labor + bldg_mat:,.0f}")
                                cb3.metric(f"{building} 總淨利", f"${bldg_profit:,.0f}")
                    else:
                        st.info("無工項統計資料")
                else:
                    st.info("請先選擇案場以查看樓層統計")
                
                st.write("---")
                
                # 整體績效
                st.write("整體績效概覽（依日報逐筆加總）")
                cm1, cm2, cm3, cm4 = st.columns(4)
                cm1.metric("日報總產值", f"${df['產值'].sum():,.0f}")
                cm2.metric("日報人力成本", f"${df['人力成本'].sum():,.0f}")
                cm3.metric("日報材料成本", f"${df['材料成本'].sum():,.0f}")
                cm4.metric("日報總利潤", f"${df['利潤'].sum():,.0f}")
                st.caption("⚠️ 上方為每筆日報的加總（施工中天數利潤為負），樓層實際利潤請看上方統計表。")
                
                st.write("---")

                # --- 報表清單 ---
                for (project, building), group_df in df.groupby(['案場', '棟別']):
                    st.markdown(f"{project} - {building}")
                    for idx, row in group_df.iterrows():
                        report_id = row['id']
                        with st.expander(f"{row['日期']} | {row['樓層']} | {row['人員']} | 當日利潤: ${row['利潤']:,.0f}", expanded=False):
                            with st.form(key=f"edit_form_{report_id}"):
                                st.markdown("1. 基本資訊")
                                ce1, ce2, ce3 = st.columns(3)
                                with ce1: new_date = st.date_input("日期", datetime.strptime(row['日期'], "%Y-%m-%d"))
                                with ce2: new_floor = st.text_input("樓層", row['樓層'])
                                with ce3: new_workers = st.text_input("施工人員", row['人員'])
                                new_desc = st.text_area("施工描述", row['施工描述'], height=100)

                                st.markdown("2. 完成項目")
                                items_df = load_report_items(report_id)
                                display_items = items_df.drop(columns=['完成天數'], errors='ignore') if not items_df.empty else items_df
                                edited_items_df = st.data_editor(display_items,
                                    column_config={"產值": st.column_config.NumberColumn(disabled=True),
                                                   "is_custom": st.column_config.CheckboxColumn("手動", disabled=True)},
                                    num_rows="dynamic", use_container_width=True, key=f"editor_items_{report_id}")

                                st.markdown("3. 材料使用")
                                materials_df = load_report_materials(report_id)
                                edited_materials_df = st.data_editor(materials_df,
                                    column_config={"成本": st.column_config.NumberColumn(disabled=True)},
                                    num_rows="dynamic", use_container_width=True, key=f"editor_materials_{report_id}")

                                if row['照片數'] > 0:
                                    st.markdown("4. 照片")
                                    photos = load_photos(report_id)
                                    if photos:
                                        pcols = st.columns(min(3, len(photos)))
                                        for i, p in enumerate(photos):
                                            with pcols[i % 3]: st.image(p['data'], caption=p['name'], use_container_width=True)

                                st.write("---")
                                cb1, cb2 = st.columns([1, 4])
                                with cb1: delete_submitted = st.form_submit_button("刪除", type="secondary")
                                with cb2: save_submitted = st.form_submit_button("保存修改並重新計算", type="primary", use_container_width=True)

                                if save_submitted:
                                    new_items_list = []; total_revenue = 0; total_item_workers = 0; total_qty = 0
                                    for _, ir in edited_items_df.iterrows():
                                        q = float(ir['數量'] or 0); p = float(ir['單價'] or 0)
                                        new_items_list.append({
                                            'item_name': ir['項目名稱'], 'quantity': q, 'unit': ir['單位'],
                                            'unit_price': p, 'revenue': q*p, 'completion_days': 1.0,
                                            'worker_count': float(ir['計工']), 'is_custom': ir.get('is_custom', False)})
                                        total_revenue += q*p; total_item_workers += float(ir['計工']); total_qty += q

                                    new_mats = []; total_mat_cost = 0
                                    for _, mr in edited_materials_df.iterrows():
                                        q = float(mr['數量'] or 0); p = float(mr['單價'] or 0)
                                        new_mats.append({'material_name': mr['材料名稱'], 'quantity': q,
                                            'unit': mr['單位'], 'unit_price': p, 'cost': q*p})
                                        total_mat_cost += q*p

                                    wl = [w.strip() for w in re.split(r'[,，、]', new_workers) if w.strip()]
                                    awc = total_item_workers if total_item_workers > 0 else len(wl)
                                    wage = get_project_wage(project)
                                    lc = awc * wage; tc = lc + total_mat_cost; pr = total_revenue - tc
                                    eff = total_qty / awc if awc > 0 else 0

                                    update_report(report_id, {
                                        'date': new_date.strftime("%Y-%m-%d"), 'project_name': project,
                                        'building_name': building, 'floor_name': new_floor,
                                        'workers': new_workers, 'worker_count': awc, 'labor_cost': lc,
                                        'description': new_desc, 'photo_count': row['照片數'],
                                        'revenue': total_revenue, 'material_cost': total_mat_cost,
                                        'total_cost': tc, 'profit': pr, 'efficiency': eff
                                    }, new_items_list, new_mats)
                                    st.success(f"已更新！當日利潤：${pr:,.0f}"); st.rerun()

                                if delete_submitted:
                                    delete_report(report_id); st.warning("已刪除"); st.rerun()
                
                st.write("---")
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("下載報表（CSV）", csv, f"工程日報_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
                
                chart_data = df.groupby('日期').agg({'產值': 'sum', '總成本': 'sum', '利潤': 'sum'}).reset_index()
                st.line_chart(chart_data.set_index('日期'))
                
                st.write("---")
                if st.button("清空所有日報", type="secondary"):
                    if st.checkbox("確認刪除？無法復原！"):
                        delete_all_reports(); st.success("已清空"); st.rerun()
            else:
                st.info("尚無日報")

        # ========== Tab: 單項成本分析 ==========
        with tab_analysis:
            st.subheader("單項工程累計分析")
            st.caption("以「樓層＋工項」為單位，數量取 MAX（固定量），工數取 SUM（累計投入）。利潤 = 產值 - 全部累計工成本。")

            ca1, ca2 = st.columns(2)
            with ca1: ana_project = st.selectbox("案場", project_names, key="ana_proj")
            with ca2:
                ana_bldg = st.selectbox("棟別", ["全部"] + get_project_buildings(ana_project), key="ana_bldg") if ana_project else "全部"
            
            ca3, ca4 = st.columns(2)
            with ca3:
                if ana_project and ana_bldg != "全部":
                    ana_floor = st.selectbox("樓層", ["全部"] + get_building_floors(ana_project, ana_bldg), key="ana_floor")
                else:
                    ana_floor = "全部"
            with ca4:
                if ana_project:
                    c = conn.cursor()
                    iq = 'SELECT DISTINCT ri.item_name FROM report_items ri JOIN reports r ON ri.report_id=r.report_id WHERE r.project_name=?'
                    ip = [ana_project]
                    if ana_bldg != "全部": iq += " AND r.building_name=?"; ip.append(ana_bldg)
                    if ana_floor != "全部": iq += " AND r.floor_name LIKE ?"; ip.append(f"%{ana_floor}%")
                    c.execute(iq + " ORDER BY ri.item_name", ip)
                    ana_item = st.selectbox("工項", ["全部"] + [r[0] for r in c.fetchall()], key="ana_item")
                else:
                    ana_item = "全部"

            if ana_project:
                ana_df, ana_mat_map, ana_wage = calc_floor_item_stats(
                    ana_project,
                    None if ana_bldg == "全部" else ana_bldg,
                    None if ana_floor == "全部" else ana_floor)
                
                if ana_df is not None and not ana_df.empty:
                    if ana_item != "全部":
                        ana_df = ana_df[ana_df['工項'] == ana_item]
                    
                    # 加入工率欄
                    ana_df['工率'] = ana_df.apply(lambda x: x['數量']/x['累計工數'] if x['累計工數']>0 else 0, axis=1)
                    
                    st.dataframe(ana_df[['樓層', '工項', '數量', '單位', '累計工數', '累計產值', '累計工成本', '項目利潤(僅扣工)', '工率', '狀態', '施工天數']],
                        column_config={
                            "數量": st.column_config.NumberColumn(format="%.1f", help="樓層固定量(MAX)"),
                            "累計產值": st.column_config.NumberColumn(format="$%d"),
                            "累計工成本": st.column_config.NumberColumn(format="$%d", help=f"日薪 ${ana_wage}"),
                            "項目利潤(僅扣工)": st.column_config.NumberColumn(format="$%d"),
                            "工率": st.column_config.NumberColumn(format="%.2f", help="數量/累計工數"),
                        }, use_container_width=True)
                    
                    st.write("---")
                    ct1, ct2, ct3 = st.columns(3)
                    tr = ana_df['累計產值'].sum(); tc = ana_df['累計工成本'].sum(); tp = ana_df['項目利潤(僅扣工)'].sum()
                    ct1.metric("總產值", f"${tr:,.0f}")
                    ct2.metric("總工成本", f"${tc:,.0f}")
                    ct3.metric("總毛利(不含料)", f"${tp:,.0f}", delta=f"{(tp/tr*100) if tr>0 else 0:.1f}%")
                    st.caption("此處成本僅含人力，不含材料費。材料費請至「查看報表」的樓層統計查看。")
                else:
                    st.info("無符合條件的資料")

        # ========== Tab 3: 案場管理 ==========
        with tab3:
            st.subheader("案場管理")
            with st.expander("新增案場", expanded=False):
                with st.form("create_project_form"):
                    new_pn = st.text_input("案場名稱")
                    copy_opt = st.checkbox("複製既有案場")
                    copy_from = st.selectbox("複製來源", project_names) if copy_opt and project_names else None
                    if st.form_submit_button("新增", use_container_width=True):
                        if new_pn:
                            if create_project(new_pn, copy_from if copy_opt else None):
                                st.success(f"已新增：{new_pn}"); st.rerun()
                            else: st.error("名稱已存在")
                        else: st.error("請輸入名稱")
            
            st.write("---")
            if project_names:
                smp = st.selectbox("管理案場", project_names, key="manage_project")
                if smp:
                    cw = get_project_wage(smp)
                    nw = st.number_input("每人每日工資", min_value=0, value=cw, step=100)
                    if nw != cw and st.button("更新工資"):
                        update_project_wage(smp, nw); st.success("已更新"); st.rerun()
                    
                    st.write("---")
                    tb, tm, te = st.tabs(["棟別樓層項目", "材料設定", "額外工項設定"])
                    
                    with tb:
                        with st.expander("新增棟別樓層", expanded=True):
                            cb1, cb2 = st.columns(2)
                            with cb1: nb = st.text_input("棟別", placeholder="A棟")
                            mode = st.radio("模式", ["單一樓層", "批量樓層"], horizontal=True)
                            if mode == "單一樓層":
                                with cb2: nf = st.text_input("樓層", placeholder="1F")
                                if st.button("新增"):
                                    if nb and nf:
                                        if add_building_floor(smp, nb, nf): st.success(f"已新增：{nb} {nf}"); st.rerun()
                                        else: st.error("已存在")
                            else:
                                with cb2:
                                    cs, ce = st.columns(2)
                                    with cs: sf = st.number_input("起始", min_value=1, value=2, step=1)
                                    with ce: ef = st.number_input("結束", min_value=1, value=10, step=1)
                                exf = get_building_floors(smp, nb) if nb else []
                                cps = st.selectbox("複製項目自", ["無"] + exf)
                                if st.button("批量新增"):
                                    if nb and sf <= ef:
                                        cnt = batch_add_floors(smp, nb, int(sf), int(ef), cps if cps != "無" else None)
                                        if cnt > 0: st.success(f"新增 {cnt} 個"); st.rerun()
                        
                        st.write("---")
                        for bldg in get_project_buildings(smp):
                            with st.expander(f"{bldg}", expanded=False):
                                for flr in get_building_floors(smp, bldg):
                                    st.write(f"{flr}")
                                    locked = get_locked_items(smp, bldg, flr)
                                    if locked: st.caption(f"🔒 {', '.join(locked)}")
                                    fi = get_floor_items(smp, bldg, flr)
                                    if fi:
                                        idf = pd.DataFrame(fi, columns=['項目', '數量', '單位', '單價'])
                                        idf['產值'] = idf['數量'] * idf['單價']
                                        st.dataframe(idf, use_container_width=True, hide_index=True)
                                        itd = st.selectbox("刪除項目", [i[0] for i in fi], key=f"di_{bldg}_{flr}")
                                        if st.button(f"刪除：{itd}", key=f"dib_{bldg}_{flr}"):
                                            delete_floor_item(smp, bldg, flr, itd); st.rerun()
                                    with st.form(f"ai_{bldg}_{flr}"):
                                        st.write("新增項目")
                                        ci1,ci2,ci3,ci4 = st.columns(4)
                                        with ci1: ain = st.text_input("名稱", key=f"in_{bldg}_{flr}")
                                        with ci2: aiq = st.number_input("數量", min_value=0.0, step=0.5, key=f"iq_{bldg}_{flr}")
                                        with ci3: aiu = st.text_input("單位", key=f"iu_{bldg}_{flr}")
                                        with ci4: aip = st.number_input("單價", min_value=0, step=50, key=f"ip_{bldg}_{flr}")
                                        if st.form_submit_button("新增"):
                                            if ain and aiq > 0 and aiu and aip > 0:
                                                if add_floor_item(smp, bldg, flr, ain, aiq, aiu, aip): st.success(f"已新增：{ain}"); st.rerun()
                                                else: st.error("已存在")
                                    if st.button(f"刪除 {bldg} {flr}", key=f"df_{bldg}_{flr}"):
                                        delete_building_floor(smp, bldg, flr); st.rerun()
                                    st.write("---")
                    
                    with tm:
                        with st.expander("從其他案場匯入"):
                            op = [p for p in project_names if p != smp]
                            if op:
                                sp = st.selectbox("來源", op)
                                if st.button("匯入"):
                                    if copy_materials_from_project(smp, sp): st.success(f"已匯入"); st.rerun()
                        with st.expander("新增材料"):
                            cm1,cm2,cm3 = st.columns(3)
                            with cm1: nmn = st.text_input("名稱", placeholder="500")
                            with cm2: nmu = st.text_input("單位", placeholder="組")
                            with cm3: nmp = st.number_input("單價", min_value=0, step=50, key="nmp")
                            if st.button("新增材料"):
                                if nmn and nmu and nmp > 0:
                                    if add_material(smp, nmn, nmu, nmp): st.success(f"已新增：{nmn}"); st.rerun()
                        st.write("---")
                        for mn, mu, mp in get_project_materials(smp):
                            c1,c2,c3,c4 = st.columns([2,1,1,1])
                            c1.write(mn); c2.write(mu); c3.write(f"${mp:,.0f}")
                            with c4:
                                if st.button("刪除", key=f"dm_{mn}"): delete_material(smp, mn); st.rerun()
                    
                    with te:
                        st.caption("設定「額外施作項目」下拉選單")
                        nei = st.text_input("工項名稱", placeholder="試水、抽水")
                        if st.button("新增工項", key="aew"):
                            if nei:
                                if add_extra_work_item(smp, nei): st.success(f"已新增：{nei}"); st.rerun()
                        st.write("---")
                        for ei in get_extra_work_items(smp):
                            c1, c2 = st.columns([4,1])
                            c1.write(ei)
                            with c2:
                                if st.button("刪除", key=f"de_{ei}"): delete_extra_work_item(smp, ei); st.rerun()
                    
                    st.write("---")
                    st.write("危險操作")
                    dk = f"dc_{smp}"
                    if dk not in st.session_state: st.session_state[dk] = False
                    if st.button(f"刪除案場：{smp}", type="secondary"): st.session_state[dk] = True; st.rerun()
                    if st.session_state[dk]:
                        st.warning(f"確定刪除 {smp}？無法復原！")
                        d1,d2 = st.columns(2)
                        with d1:
                            if st.button("確認刪除", type="primary", key=f"cd_{smp}"):
                                delete_project(smp); st.session_state[dk] = False; st.rerun()
                        with d2:
                            if st.button("取消", key=f"cc_{smp}"): st.session_state[dk] = False; st.rerun()

        # ========== Tab 4: 用戶管理 ==========
        with tab4:
            st.subheader("用戶管理")
            with st.expander("新增用戶"):
                with st.form("create_user_form"):
                    cu1,cu2,cu3 = st.columns(3)
                    with cu1: nun = st.text_input("帳號")
                    with cu2: nup = st.text_input("密碼", type="password")
                    with cu3: nur = st.selectbox("角色", ["user", "admin"])
                    if st.form_submit_button("新增", use_container_width=True):
                        if nun and nup:
                            if create_user(nun, nup, nur): st.success(f"已新增：{nun}"); st.rerun()
                            else: st.error("帳號已存在")
            st.write("---")
            users = get_all_users()
            for uid, un, ur, uca in users:
                c1,c2,c3,c4 = st.columns([2,1,2,1])
                c1.write(un); c2.write("管理員" if ur=="admin" else "用戶"); c3.write(uca[:10])
                with c4:
                    if un != st.session_state.username:
                        if st.button("刪除", key=f"du_{uid}"): delete_user(uid); st.rerun()
                    else: st.write("（本人）")
            st.write("---")
            with st.form("change_pwd"):
                tu = st.selectbox("用戶", [u[1] for u in users])
                np = st.text_input("新密碼", type="password")
                if st.form_submit_button("修改密碼", use_container_width=True):
                    if np: change_password(tu, np); st.success(f"已修改 {tu}")
    
    else:
        # ========== 一般用戶 ==========
        st.subheader("我的日報表")
        my_reports = load_all_reports(creator_filter=st.session_state.username)
        if not my_reports.empty:
            for idx, row in my_reports.head(10).iterrows():
                with st.expander(f"{row['日期']} - {row['案場']} {row['棟別']} {row['樓層']}"):
                    ci, ce = st.columns([3,1])
                    with ci:
                        st.write(f"人員：{row['人員']} ({row['工數']:.1f}工)")
                        if row['更新時間']: st.caption(f"更新：{row['更新時間']}")
                    with ce:
                        if st.button("修改", key=f"ue_{row['id']}"):
                            st.session_state.editing_report = row['id']; st.rerun()
        st.write("---")
        is_editing = 'editing_report' in st.session_state and st.session_state.editing_report
        if is_editing:
            st.subheader("修改日報表")
            rd = get_report_detail(st.session_state.editing_report)
            if not rd: st.error("找不到"); del st.session_state.editing_report; st.rerun()
            ed = {'report_id': st.session_state.editing_report, 'report_detail': rd,
                  'items': load_report_items(st.session_state.editing_report),
                  'materials': load_report_materials(st.session_state.editing_report),
                  'photos': load_photos(st.session_state.editing_report)}
            if st.button("取消修改"): del st.session_state.editing_report; st.rerun()
            render_report_form(True, ed)
        else:
            st.subheader("填寫新日報")
            render_report_form(False)
