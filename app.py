import re
import json
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime
import hashlib

# --- 設定頁面 ---
st.set_page_config(page_title="工程日報與利潤分析系統", layout="wide")

# --- 密碼加密 ---
def hash_password(password):
    """使用 SHA256 加密密碼"""
    return hashlib.sha256(password.encode()).hexdigest()

# --- 資料庫初始化 ---
def init_database():
    """初始化 SQLite 資料庫"""
    conn = sqlite3.connect('construction_reports_v6.db', check_same_thread=False)
    c = conn.cursor()
    
    # 創建用戶表
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 創建案場設定表
    c.execute('''
        CREATE TABLE IF NOT EXISTS project_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT UNIQUE NOT NULL,
            daily_wage INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 創建棟別樓層表
    c.execute('''
        CREATE TABLE IF NOT EXISTS building_floors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            building_name TEXT,
            floor_name TEXT,
            UNIQUE(project_name, building_name, floor_name),
            FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
        )
    ''')
    
    # 創建項目設定表
    c.execute('''
        CREATE TABLE IF NOT EXISTS floor_items (
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
        )
    ''')
    
    # 創建材料設定表
    c.execute('''
        CREATE TABLE IF NOT EXISTS materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            material_name TEXT,
            unit TEXT,
            unit_price REAL,
            UNIQUE(project_name, material_name),
            FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
        )
    ''')
    
    # 創建日報表
    c.execute('''
        CREATE TABLE IF NOT EXISTS reports (
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
        )
    ''')
    
    # 創建日報完成項目表
    c.execute('''
        CREATE TABLE IF NOT EXISTS report_items (
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
        )
    ''')
    
    # 創建日報材料使用表
    c.execute('''
        CREATE TABLE IF NOT EXISTS report_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT,
            material_name TEXT,
            quantity REAL DEFAULT 0,
            unit TEXT,
            unit_price REAL,
            cost REAL DEFAULT 0,
            FOREIGN KEY (report_id) REFERENCES reports(report_id)
        )
    ''')
    
    # 創建照片表
    c.execute('''
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT,
            filename TEXT,
            photo_data BLOB,
            FOREIGN KEY (report_id) REFERENCES reports(report_id)
        )
    ''')
    
    # 創建額外工項設定表
    c.execute('''
        CREATE TABLE IF NOT EXISTS extra_work_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            item_name TEXT,
            UNIQUE(project_name, item_name),
            FOREIGN KEY (project_name) REFERENCES project_settings(project_name)
        )
    ''')
    
    # ========== [需求1] 創建項目完成鎖定表 ==========
    c.execute('''
        CREATE TABLE IF NOT EXISTS completed_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT,
            building_name TEXT,
            floor_name TEXT,
            item_name TEXT,
            locked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            locked_by TEXT,
            UNIQUE(project_name, building_name, floor_name, item_name)
        )
    ''')
    
    # Migrate: Add remark column to report_items if not exists
    try:
        c.execute('ALTER TABLE report_items ADD COLUMN remark TEXT DEFAULT ""')
    except sqlite3.OperationalError:
        pass
    
    # 檢查是否有預設管理員帳號
    c.execute('SELECT COUNT(*) FROM users WHERE role = "admin"')
    if c.fetchone()[0] == 0:
        c.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                 ('admin', hash_password('admin123'), 'admin'))
    
    conn.commit()
    return conn

# 初始化資料庫連線
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
    c.execute('SELECT DISTINCT building_name FROM building_floors WHERE project_name = ? ORDER BY building_name', 
             (project_name,))
    return [row[0] for row in c.fetchall()]

def get_building_floors(project_name, building_name):
    c = conn.cursor()
    c.execute('SELECT floor_name FROM building_floors WHERE project_name = ? AND building_name = ? ORDER BY id', 
             (project_name, building_name))
    return [row[0] for row in c.fetchall()]

def get_floor_items(project_name, building_name, floor_name):
    c = conn.cursor()
    c.execute('''SELECT item_name, standard_quantity, unit, unit_price 
                FROM floor_items 
                WHERE project_name = ? AND building_name = ? AND floor_name = ?
                ORDER BY item_name''', 
             (project_name, building_name, floor_name))
    return c.fetchall()

def get_project_materials(project_name):
    c = conn.cursor()
    c.execute('SELECT material_name, unit, unit_price FROM materials WHERE project_name = ? ORDER BY material_name', 
             (project_name,))
    return c.fetchall()

def get_project_wage(project_name):
    c = conn.cursor()
    c.execute('SELECT daily_wage FROM project_settings WHERE project_name = ?', (project_name,))
    result = c.fetchone()
    return result[0] if result else 2500

def create_project(project_name, copy_from=None):
    c = conn.cursor()
    try:
        if copy_from:
            wage = get_project_wage(copy_from)
        else:
            wage = 2500
        
        c.execute('INSERT INTO project_settings (project_name, daily_wage) VALUES (?, ?)', 
                 (project_name, wage))
        
        if copy_from:
            c.execute('''INSERT INTO building_floors (project_name, building_name, floor_name)
                        SELECT ?, building_name, floor_name FROM building_floors WHERE project_name = ?''',
                     (project_name, copy_from))
            c.execute('''INSERT INTO floor_items (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                        SELECT ?, building_name, floor_name, item_name, standard_quantity, unit, unit_price 
                        FROM floor_items WHERE project_name = ?''',
                     (project_name, copy_from))
            c.execute('''INSERT INTO materials (project_name, material_name, unit, unit_price)
                        SELECT ?, material_name, unit, unit_price FROM materials WHERE project_name = ?''',
                     (project_name, copy_from))
        
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_project(project_name):
    c = conn.cursor()
    c.execute('SELECT report_id FROM reports WHERE project_name = ?', (project_name,))
    report_ids = [row[0] for row in c.fetchall()]
    for report_id in report_ids:
        c.execute('DELETE FROM photos WHERE report_id = ?', (report_id,))
        c.execute('DELETE FROM report_items WHERE report_id = ?', (report_id,))
        c.execute('DELETE FROM report_materials WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM reports WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM building_floors WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM floor_items WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM materials WHERE project_name = ?', (project_name,))
    c.execute('DELETE FROM completed_items WHERE project_name = ?', (project_name,))
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
    source_items = []
    if copy_items_from_floor:
        source_items = get_floor_items(project_name, building_name, copy_items_from_floor)
    try:
        for i in range(start_floor, end_floor + 1):
            floor_name = f"{i}F"
            c.execute('SELECT COUNT(*) FROM building_floors WHERE project_name=? AND building_name=? AND floor_name=?',
                     (project_name, building_name, floor_name))
            if c.fetchone()[0] == 0:
                c.execute('INSERT INTO building_floors (project_name, building_name, floor_name) VALUES (?, ?, ?)',
                         (project_name, building_name, floor_name))
                if source_items:
                    for item_name, std_qty, unit, unit_price in source_items:
                        c.execute('''INSERT INTO floor_items 
                                    (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                 (project_name, building_name, floor_name, item_name, std_qty, unit, unit_price))
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
                    SELECT ?, material_name, unit, unit_price FROM materials WHERE project_name = ?''',
                 (target_project, source_project))
        conn.commit()
        return True
    except Exception as e:
        print(e)
        return False

def delete_building_floor(project_name, building_name, floor_name):
    c = conn.cursor()
    c.execute('DELETE FROM floor_items WHERE project_name = ? AND building_name = ? AND floor_name = ?',
             (project_name, building_name, floor_name))
    c.execute('DELETE FROM building_floors WHERE project_name = ? AND building_name = ? AND floor_name = ?',
             (project_name, building_name, floor_name))
    conn.commit()

def add_floor_item(project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price):
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO floor_items 
                    (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                 (project_name, building_name, floor_name, item_name, standard_quantity, unit, unit_price))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def update_floor_item(project_name, building_name, floor_name, item_name, standard_quantity, unit_price):
    c = conn.cursor()
    c.execute('''UPDATE floor_items 
                SET standard_quantity = ?, unit_price = ?
                WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?''',
             (standard_quantity, unit_price, project_name, building_name, floor_name, item_name))
    conn.commit()

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

def update_material(project_name, material_name, unit_price):
    c = conn.cursor()
    c.execute('UPDATE materials SET unit_price = ? WHERE project_name = ? AND material_name = ?',
             (unit_price, project_name, material_name))
    conn.commit()

def delete_material(project_name, material_name):
    c = conn.cursor()
    c.execute('DELETE FROM materials WHERE project_name = ? AND material_name = ?',
             (project_name, material_name))
    conn.commit()

def update_project_wage(project_name, wage):
    c = conn.cursor()
    c.execute('UPDATE project_settings SET daily_wage = ? WHERE project_name = ?',
             (wage, project_name))
    conn.commit()

# --- [需求1] 項目鎖定函數 ---
def is_item_locked(project_name, building_name, floor_name, item_name):
    """檢查項目是否已鎖定（完成後不可修改）"""
    c = conn.cursor()
    c.execute('''SELECT COUNT(*) FROM completed_items 
                WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?''',
             (project_name, building_name, floor_name, item_name))
    return c.fetchone()[0] > 0

def lock_item(project_name, building_name, floor_name, item_name, locked_by):
    """鎖定已完成的項目"""
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO completed_items (project_name, building_name, floor_name, item_name, locked_by)
                    VALUES (?, ?, ?, ?, ?)''',
                 (project_name, building_name, floor_name, item_name, locked_by))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def unlock_item(project_name, building_name, floor_name, item_name):
    """解鎖項目（僅管理員可用）"""
    c = conn.cursor()
    c.execute('''DELETE FROM completed_items 
                WHERE project_name = ? AND building_name = ? AND floor_name = ? AND item_name = ?''',
             (project_name, building_name, floor_name, item_name))
    conn.commit()

def get_locked_items(project_name, building_name, floor_name):
    """獲取該樓層所有已鎖定的項目"""
    c = conn.cursor()
    c.execute('''SELECT item_name FROM completed_items 
                WHERE project_name = ? AND building_name = ? AND floor_name = ?''',
             (project_name, building_name, floor_name))
    return [row[0] for row in c.fetchall()]

# --- 額外工項管理函數 ---
def get_extra_work_items(project_name):
    c = conn.cursor()
    c.execute('SELECT item_name FROM extra_work_items WHERE project_name = ? ORDER BY item_name',
             (project_name,))
    return [row[0] for row in c.fetchall()]

def add_extra_work_item(project_name, item_name):
    c = conn.cursor()
    try:
        c.execute('INSERT INTO extra_work_items (project_name, item_name) VALUES (?, ?)',
                 (project_name, item_name))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_extra_work_item(project_name, item_name):
    c = conn.cursor()
    c.execute('DELETE FROM extra_work_items WHERE project_name = ? AND item_name = ?',
             (project_name, item_name))
    conn.commit()

# --- 日報表操作函數 ---
def save_report(report_data, items, materials, photos=None):
    c = conn.cursor()
    total_quantity = sum(item['quantity'] for item in items)
    
    c.execute('''
        INSERT INTO reports (report_id, date, project_name, building_name, floor_name, workers, 
                           worker_count, labor_cost, description, photo_count, created_by, 
                           revenue, material_cost, total_cost, profit, efficiency, total_quantity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        report_data['report_id'], report_data['date'], report_data['project_name'],
        report_data['building_name'], report_data['floor_name'], report_data['workers'],
        report_data['worker_count'], report_data['labor_cost'], report_data['description'],
        report_data['photo_count'], report_data['created_by'], report_data['revenue'],
        report_data['material_cost'], report_data['total_cost'], report_data['profit'],
        report_data['efficiency'], total_quantity
    ))
    
    for item in items:
        is_custom = item.get('is_custom', False)
        remark = item.get('remark', '')
        c.execute('''
            INSERT INTO report_items (report_id, item_name, quantity, unit, unit_price, revenue, 
                                    completion_days, worker_count, is_custom, remark)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            report_data['report_id'], item['item_name'], item['quantity'],
            item['unit'], item['unit_price'], item['revenue'],
            item['completion_days'], item['worker_count'], is_custom, remark
        ))
    
    for material in materials:
        c.execute('''
            INSERT INTO report_materials (report_id, material_name, quantity, unit, unit_price, cost)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            report_data['report_id'], material['material_name'], material['quantity'],
            material['unit'], material['unit_price'], material['cost']
        ))
    
    if photos:
        for photo in photos:
            c.execute('INSERT INTO photos (report_id, filename, photo_data) VALUES (?, ?, ?)',
                     (report_data['report_id'], photo['name'], photo['data']))
    
    conn.commit()

def update_report(report_id, report_data, items, materials, photos=None):
    c = conn.cursor()
    total_quantity = sum(item['quantity'] for item in items)
    
    c.execute('''
        UPDATE reports 
        SET date = ?, building_name = ?, floor_name = ?, workers = ?, 
            worker_count = ?, labor_cost = ?, description = ?, photo_count = ?,
            revenue = ?, material_cost = ?, total_cost = ?, profit = ?, 
            efficiency = ?, total_quantity = ?, updated_at = CURRENT_TIMESTAMP
        WHERE report_id = ?
    ''', (
        report_data['date'], report_data['building_name'], report_data['floor_name'],
        report_data['workers'], report_data['worker_count'], report_data['labor_cost'],
        report_data['description'], report_data['photo_count'], report_data['revenue'],
        report_data['material_cost'], report_data['total_cost'], report_data['profit'],
        report_data['efficiency'], total_quantity, report_id
    ))
    
    c.execute('DELETE FROM report_items WHERE report_id = ?', (report_id,))
    c.execute('DELETE FROM report_materials WHERE report_id = ?', (report_id,))
    
    for item in items:
        is_custom = item.get('is_custom', False)
        remark = item.get('remark', '')
        c.execute('''
            INSERT INTO report_items (report_id, item_name, quantity, unit, unit_price, revenue,
                                    completion_days, worker_count, is_custom, remark)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            report_id, item['item_name'], item['quantity'], item['unit'],
            item['unit_price'], item['revenue'], item['completion_days'],
            item['worker_count'], is_custom, remark
        ))
    
    for material in materials:
        c.execute('''
            INSERT INTO report_materials (report_id, material_name, quantity, unit, unit_price, cost)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            report_id, material['material_name'], material['quantity'],
            material['unit'], material['unit_price'], material['cost']
        ))
    
    if photos:
        for photo in photos:
            c.execute('INSERT INTO photos (report_id, filename, photo_data) VALUES (?, ?, ?)',
                     (report_id, photo['name'], photo['data']))
    
    conn.commit()

def load_all_reports(project_filter=None, building_filter=None, date_from=None, date_to=None, creator_filter=None):
    c = conn.cursor()
    query = '''
        SELECT report_id, date, project_name, building_name, floor_name, workers, worker_count,
               labor_cost, revenue, material_cost, total_cost, profit, efficiency, total_quantity,
               description, photo_count, created_by, updated_at
        FROM reports WHERE 1=1
    '''
    params = []
    if project_filter:
        query += ' AND project_name = ?'
        params.append(project_filter)
    if building_filter:
        query += ' AND building_name = ?'
        params.append(building_filter)
    if date_from:
        query += ' AND date >= ?'
        params.append(date_from)
    if date_to:
        query += ' AND date <= ?'
        params.append(date_to)
    if creator_filter:
        query += ' AND created_by = ?'
        params.append(creator_filter)
    query += ' ORDER BY date DESC, created_at DESC'
    c.execute(query, params)
    rows = c.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=[
        'id', '日期', '案場', '棟別', '樓層', '人員', '工數',
        '人力成本', '產值', '材料成本', '總成本', '利潤', '工率', '總數量',
        '施工描述', '照片數', '建立者', '更新時間'
    ])
    return df

def load_report_items(report_id):
    c = conn.cursor()
    c.execute('''SELECT item_name, quantity, unit, unit_price, revenue, completion_days, worker_count, is_custom, remark 
                FROM report_items WHERE report_id = ?''',
             (report_id,))
    rows = c.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=['項目名稱', '數量', '單位', '單價', '產值', '完成天數', '計工', 'is_custom', '備註'])

def load_report_materials(report_id):
    c = conn.cursor()
    c.execute('SELECT material_name, quantity, unit, unit_price, cost FROM report_materials WHERE report_id = ?',
             (report_id,))
    rows = c.fetchall()
    if not rows:
        return pd.DataFrame()
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
    rows = c.fetchall()
    photos = []
    for filename, photo_data in rows:
        photos.append({'name': filename, 'data': photo_data})
    return photos

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

def render_report_form(is_editing, edit_data=None):
    """
    渲染日報表單 (修改版 v3)
    修正重點：
    - [需求1] 已鎖定項目不可修改
    - [需求2] 施工中不計算產值 (已有)
    - [需求4] 移除預計天數、一般用戶隱藏數量 (已有)
    - [需求5] 額外工項下拉 + 備註 (已有)
    - [用戶回饋] 數量代表樓層固定量，不因多天填報而累加
    """
    current_role = st.session_state.get('role', 'user')
    project_names = get_all_projects()
    
    if not project_names:
        st.warning("請先新增案場" if current_role == 'admin' else "請聯絡管理員新增案場")
        return None
    
    # --- 預設值處理 ---
    if is_editing and edit_data:
        (edit_date, edit_project, edit_building, edit_floor, edit_workers, 
         edit_worker_count, edit_labor_cost, edit_desc, edit_photo_count,
         edit_revenue, edit_material_cost, edit_total_cost, edit_profit) = edit_data['report_detail']
        edit_items_df = edit_data['items']
        edit_materials_df = edit_data['materials']
        edit_photos = edit_data['photos']
        report_id = edit_data['report_id']
        if ',' in edit_floor:
            edit_floor_list = [f.strip() for f in edit_floor.split(',')]
        else:
            edit_floor_list = [edit_floor]
    else:
        edit_date = None
        edit_project = None
        edit_building = None
        edit_floor_list = []
        edit_workers = st.session_state.username
        edit_desc = ""
        edit_items_df = pd.DataFrame()
        edit_materials_df = pd.DataFrame()
        edit_photos = []
        report_id = None
    
    # --- 案場選擇 ---
    selected_project = st.selectbox(
        "選擇案場", project_names,
        index=project_names.index(edit_project) if is_editing and edit_project in project_names else 0,
        key=f"project_{report_id if report_id else 'new'}"
    )
    
    if not selected_project:
        return None
    
    buildings = get_project_buildings(selected_project)
    if not buildings:
        st.warning("此案場尚未設定棟別樓層")
        return None
    
    # --- 基本資料 ---
    col1, col2 = st.columns(2)
    with col1:
        date = st.date_input(
            "日期", 
            datetime.strptime(edit_date, "%Y-%m-%d") if is_editing else datetime.now()
        )
        selected_building = st.selectbox(
            "棟別", buildings,
            index=buildings.index(edit_building) if is_editing and edit_building in buildings else 0
        )
        if selected_building:
            floors = get_building_floors(selected_project, selected_building)
            if floors:
                default_floors = []
                if is_editing:
                    default_floors = [f for f in edit_floor_list if f in floors]
                selected_floors = st.multiselect("樓層 (可多選)", floors, default=default_floors)
                reference_floor = selected_floors[0] if selected_floors else (floors[0] if floors else None)
                selected_floor_str = ", ".join(selected_floors)
            else:
                st.warning("此棟別尚未設定樓層")
                return None
        else:
            selected_floors = []
            reference_floor = None
            selected_floor_str = ""
        
        workers = st.text_input(
            "施工人員姓名（用逗號分隔）", 
            edit_workers if is_editing else st.session_state.username,
            help="請填寫今日出工人名，例如：王大明, 李小華"
        )
    
    with col2:
        progress_desc = st.text_area(
            "施工進度描述", 
            value=edit_desc if is_editing else "",
            height=200,
            placeholder="例如：\n- A棟2F浴室防水完成\n- 素地整理完成\n- 材料搬運"
        )
    
    # --- 初始化 ---
    rt_total_qty = 0
    rt_total_workers = 0
    rt_total_revenue = 0
    selected_items = []
    
    if reference_floor:
        st.write("---")
        
        # ========== [需求1] 取得已鎖定項目清單 ==========
        locked_items = get_locked_items(selected_project, selected_building, reference_floor)
        
        if current_role == 'admin':
            st.subheader("完成項目與計工 (管理員模式：可輸入數量)")
            if locked_items:
                st.info(f"🔒 已鎖定項目：{', '.join(locked_items)}（如需修改請先解鎖）")
        else:
            st.subheader("今日施工項目回報")
            st.caption("請勾選今日有施作的項目，並填寫投入工數。")
        
        floor_items = get_floor_items(selected_project, selected_building, reference_floor)
        
        # --- 標準項目區域 ---
        if floor_items:
            edit_items_dict = {}
            if is_editing and not edit_items_df.empty:
                std_items_df = edit_items_df[edit_items_df['is_custom'] == 0]
                for _, item_row in std_items_df.iterrows():
                    edit_items_dict[item_row['項目名稱']] = {
                        'quantity': item_row['數量'],
                        'completion_days': item_row['完成天數'],
                        'worker_count': item_row['計工']
                    }
            
            if current_role == 'admin':
                st.markdown("標準項目清單")
                h1, h2, h3, h5, h6 = st.columns([2, 1, 1.2, 1, 0.8])
                h1.caption("項目名稱")
                h2.caption("狀態")
                h3.caption("數量")
                h5.caption("今日計工")
                h6.caption("鎖定")
            else:
                st.markdown("今日施作項目")
                h1, h2, h5 = st.columns([3, 1.5, 1.5])
                h1.caption("勾選施作項目")
                h2.caption("狀態")
                h5.caption("投入工數 (人天)")

            for item_name, std_qty, unit, unit_price in floor_items:
                default_checked = item_name in edit_items_dict
                
                # [需求1] 檢查是否已鎖定
                item_is_locked = item_name in locked_items
                
                default_qty = edit_items_dict[item_name]['quantity'] if default_checked else (std_qty if current_role == 'admin' else 0.0)
                default_days = edit_items_dict[item_name]['completion_days'] if default_checked else 1.0
                default_workers = edit_items_dict[item_name]['worker_count'] if default_checked else 0.0
                
                is_prep_default = False
                if default_checked and edit_items_dict[item_name]['quantity'] == 0:
                    is_prep_default = True

                if current_role == 'admin':
                    col_check, col_prep, col_qty, col_workers, col_lock = st.columns([2, 1, 1.2, 1, 0.8])
                    with col_check:
                        # [需求1] 已鎖定項目顯示鎖定標記
                        label = f"🔒 {item_name}" if item_is_locked else item_name
                        checked = st.checkbox(label, value=default_checked or item_is_locked, 
                                            key=f"item_{item_name}_{report_id}",
                                            disabled=item_is_locked)
                    
                    if checked or item_is_locked:
                        with col_prep:
                            if item_is_locked:
                                st.write("✅ 已完成")
                                is_prep = False
                            else:
                                is_prep = st.checkbox("施工中", value=is_prep_default, key=f"prep_{item_name}_{report_id}")
                        with col_qty:
                            if item_is_locked:
                                st.write(f"{default_qty} {unit}")
                                qty = default_qty
                            else:
                                qty_val = 0.0 if is_prep else float(default_qty)
                                qty = st.number_input(f"數量({unit})", value=qty_val, step=0.5, 
                                                     disabled=is_prep, key=f"qty_{item_name}_{report_id}", 
                                                     label_visibility="collapsed")
                        with col_workers:
                            if item_is_locked:
                                st.write(f"{default_workers}")
                                item_workers = default_workers
                            else:
                                item_workers = st.number_input("計工", value=float(default_workers), step=0.5, 
                                                              key=f"workers_{item_name}_{report_id}", 
                                                              label_visibility="collapsed")
                        with col_lock:
                            # [需求1] 鎖定/解鎖按鈕
                            if item_is_locked:
                                if st.button("🔓", key=f"unlock_{item_name}_{report_id}", help="解鎖此項目"):
                                    unlock_item(selected_project, selected_building, reference_floor, item_name)
                                    st.rerun()
                            else:
                                if qty > 0 and not is_prep:
                                    if st.button("🔒", key=f"lock_{item_name}_{report_id}", help="鎖定此項目（完成後不可修改）"):
                                        lock_item(selected_project, selected_building, reference_floor, item_name, st.session_state.username)
                                        st.rerun()
                        
                        comp_days = 1.0
                    else:
                        # 未勾選，跳過
                        continue
                else:
                    # 一般用戶
                    col_check, col_prep, col_workers = st.columns([3, 1.5, 1.5])
                    with col_check:
                        label = f"🔒 {item_name} (已完成)" if item_is_locked else item_name
                        checked = st.checkbox(label, value=default_checked, 
                                            key=f"item_{item_name}_{report_id}",
                                            disabled=item_is_locked)
                    
                    if checked:
                        with col_prep:
                            is_prep = st.checkbox("僅施工 (未完工)", value=is_prep_default, 
                                                 help="若尚未完成全部工項請勾選", 
                                                 key=f"prep_{item_name}_{report_id}")
                        with col_workers:
                            item_workers = st.number_input("投入工數", min_value=0.0, value=float(default_workers), 
                                                          step=0.5, key=f"workers_{item_name}_{report_id}", 
                                                          label_visibility="collapsed")
                        qty = float(default_qty)
                        comp_days = 1.0
                    elif item_is_locked:
                        # 已鎖定的項目，用戶不能操作
                        continue
                    else:
                        continue

                if checked or item_is_locked:
                    revenue = qty * unit_price
                    rt_total_qty += qty
                    rt_total_workers += item_workers
                    rt_total_revenue += revenue
                    
                    selected_items.append({
                        'item_name': item_name,
                        'quantity': qty,
                        'unit': unit,
                        'unit_price': unit_price,
                        'revenue': revenue,
                        'completion_days': comp_days if 'comp_days' in dir() else 1.0,
                        'worker_count': item_workers,
                        'is_custom': False
                    })
        else:
            st.info(f"{reference_floor} 尚未設定標準項目")

        # --- 自定義/手動項目區域 ---
        st.write("---")
        st.markdown("額外施作項目 (點工/雜項)")
        
        extra_items_list = get_extra_work_items(selected_project)
        has_predefined_items = len(extra_items_list) > 0
        
        custom_key = f"custom_items_count_{report_id if report_id else 'new'}"
        if custom_key not in st.session_state:
            init_count = 1
            if is_editing and not edit_items_df.empty:
                custom_df = edit_items_df[edit_items_df['is_custom'] == 1]
                if not custom_df.empty:
                    init_count = len(custom_df) + 1
            st.session_state[custom_key] = init_count

        if st.button("＋ 增加項目", key=f"add_custom_btn_{report_id}"):
            st.session_state[custom_key] += 1
            st.rerun()

        custom_data_list = []
        if is_editing and not edit_items_df.empty:
            custom_df = edit_items_df[edit_items_df['is_custom'] == 1]
            for _, row in custom_df.iterrows():
                custom_data_list.append(row)

        for i in range(st.session_state[custom_key]):
            if current_role == 'admin':
                cc1, cc2, cc3, cc4, cc5, cc6 = st.columns([2, 1, 0.8, 0.8, 0.8, 1.5])
            else:
                cc1, cc6, cc5 = st.columns([2.5, 2, 1.5])

            d_name = ""
            d_qty = 0.0
            d_unit = "式"
            d_price = 0.0
            d_workers = 0.0
            d_remark = ""
            
            if i < len(custom_data_list):
                row = custom_data_list[i]
                d_name = row['項目名稱']
                d_qty = row['數量']
                d_unit = row['單位']
                d_price = row['單價']
                d_workers = row['計工']
                d_remark = row.get('備註', '')

            with cc1:
                if has_predefined_items:
                    options = [""] + extra_items_list + ["自訂..."]
                    default_idx = 0
                    if d_name in extra_items_list:
                        default_idx = options.index(d_name)
                    elif d_name and d_name not in extra_items_list:
                        default_idx = len(options) - 1
                    
                    selected_item = st.selectbox(
                        f"選擇工項", options, index=default_idx,
                        key=f"c_select_{i}_{report_id}", label_visibility="collapsed"
                    )
                    if selected_item == "自訂...":
                        c_name = st.text_input("自訂項目名稱", 
                                              value=d_name if d_name not in extra_items_list else "", 
                                              key=f"c_custom_name_{i}_{report_id}", placeholder="輸入自訂項目")
                    elif selected_item:
                        c_name = selected_item
                    else:
                        c_name = ""
                else:
                    c_name = st.text_input(f"項目名稱", value=d_name, key=f"c_name_{i}_{report_id}", placeholder="例如：清理現場")
            
            if current_role == 'admin':
                with cc2:
                    c_qty = st.number_input(f"數量", value=float(d_qty), min_value=0.0, step=0.5, key=f"c_qty_{i}_{report_id}")
                with cc3:
                    c_unit = st.text_input(f"單位", value=d_unit, key=f"c_unit_{i}_{report_id}")
                with cc4:
                    c_price = st.number_input(f"單價", value=float(d_price), min_value=0.0, step=100.0, key=f"c_price_{i}_{report_id}")
                with cc5:
                    c_workers = st.number_input(f"計工", value=float(d_workers), min_value=0.0, step=0.5, key=f"c_workers_{i}_{report_id}")
                with cc6:
                    c_remark = st.text_input(f"備註", value=d_remark, key=f"c_remark_{i}_{report_id}", placeholder="例如：試水、抽水")
            else:
                with cc6:
                    c_remark = st.text_input(f"備註", value=d_remark, key=f"c_remark_{i}_{report_id}", placeholder="例如：試水、抽水")
                    c_qty = 0.0
                    c_price = 0.0
                    c_unit = "式"
                with cc5:
                    c_workers = st.number_input(f"投入工數", value=float(d_workers), min_value=0.0, step=0.5, key=f"c_workers_{i}_{report_id}")

            if c_name:
                c_revenue = c_qty * c_price
                rt_total_qty += c_qty
                rt_total_workers += c_workers
                rt_total_revenue += c_revenue
                
                selected_items.append({
                    'item_name': c_name,
                    'quantity': c_qty,
                    'unit': c_unit,
                    'unit_price': c_price,
                    'revenue': c_revenue,
                    'completion_days': 1.0,
                    'worker_count': c_workers,
                    'is_custom': True,
                    'remark': c_remark
                })
        
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
            
            cols_per_row = 3
            for i in range(0, len(materials), cols_per_row):
                cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    if i + j < len(materials):
                        mat_name, mat_unit, mat_price = materials[i + j]
                        default_mat_checked = mat_name in edit_materials_dict
                        default_mat_qty = edit_materials_dict[mat_name] if default_mat_checked else 0.0
                        
                        with cols[j]:
                            mat_checked = st.checkbox(
                                f"{mat_name}", value=default_mat_checked,
                                key=f"mat_{mat_name}_{report_id if report_id else 'new'}"
                            )
                            if mat_checked:
                                if current_role == 'admin':
                                    mat_qty = st.number_input(
                                        f"數量 ({mat_unit})", min_value=0.0, value=float(default_mat_qty),
                                        step=0.5, key=f"mat_qty_{mat_name}_{report_id if report_id else 'new'}"
                                    )
                                else:
                                    mat_qty = 0.0
                                    st.caption(f"已記錄 ({mat_unit})")
                                
                                mat_cost = mat_qty * mat_price
                                selected_materials.append({
                                    'material_name': mat_name, 'quantity': mat_qty,
                                    'unit': mat_unit, 'unit_price': mat_price, 'cost': mat_cost
                                })
        else:
            st.warning("此案場尚未設定材料")
        
        st.write("---")
        
        uploaded_files = st.file_uploader(
            "上傳施工照片", accept_multiple_files=True, type=['png', 'jpg', 'jpeg'],
            help="可上傳多張照片", key=f"photos_{report_id if report_id else 'new'}"
        )
        
        if is_editing and edit_photos:
            st.write("現有照片：")
            cols = st.columns(min(3, len(edit_photos)))
            for i, photo in enumerate(edit_photos):
                with cols[i % 3]:
                    st.image(photo['data'], caption=photo['name'], use_container_width=True)
        
        st.write("---")
        
        submitted = st.button(
            "更新日報表" if is_editing else "提交日報表", 
            use_container_width=True, type="primary"
        )
        
        if submitted:
            if not workers.strip():
                st.error("請填寫施工人員！")
                return None
            elif not selected_items and not progress_desc:
                st.error("請至少勾選一個項目或填寫施工描述！")
                return None
            else:
                worker_list = [w.strip() for w in re.split(r'[,，、]', workers) if w.strip()]
                actual_worker_count = rt_total_workers
                if actual_worker_count == 0:
                    actual_worker_count = len(worker_list)
                
                wage = get_project_wage(selected_project)
                labor_cost = actual_worker_count * wage
                total_revenue = rt_total_revenue
                total_mat_cost = sum(mat['cost'] for mat in selected_materials)
                total_cost = labor_cost + total_mat_cost
                profit = total_revenue - total_cost
                total_qty = rt_total_qty
                efficiency = total_qty / actual_worker_count if actual_worker_count > 0 else 0
                
                photo_list = []
                if uploaded_files:
                    for file in uploaded_files:
                        photo_bytes = file.read()
                        photo_list.append({'name': file.name, 'data': photo_bytes})
                
                report_data = {
                    'date': date.strftime("%Y-%m-%d"),
                    'project_name': selected_project,
                    'building_name': selected_building,
                    'floor_name': selected_floor_str,
                    'workers': workers,
                    'worker_count': actual_worker_count,
                    'labor_cost': labor_cost,
                    'description': progress_desc,
                    'photo_count': (len(uploaded_files) if uploaded_files else 0) + (len(edit_photos) if is_editing else 0),
                    'created_by': st.session_state.username,
                    'revenue': total_revenue,
                    'material_cost': total_mat_cost,
                    'total_cost': total_cost,
                    'profit': profit,
                    'efficiency': efficiency
                }
                
                if is_editing:
                    update_report(report_id, report_data, selected_items, selected_materials, photo_list if photo_list else None)
                    st.success("日報表已更新成功！")
                    if 'editing_report' in st.session_state:
                        del st.session_state.editing_report
                else:
                    new_report_id = f"{date.strftime('%Y%m%d')}_{int(datetime.now().timestamp())}"
                    report_data['report_id'] = new_report_id
                    save_report(report_data, selected_items, selected_materials, photo_list if photo_list else None)
                    st.success("日報表已提交成功！")
                
                if current_role == 'admin':
                    col_s1, col_s2, col_s3 = st.columns(3)
                    col_s1.metric("產值", f"${total_revenue:,.0f}")
                    col_s2.metric("總成本", f"${total_cost:,.0f}")
                    col_s3.metric("利潤", f"${profit:,.0f}")
                else:
                    st.info("資料已上傳，待管理員審核計算數量。")
                
                st.rerun()

# --- 登入系統 ---
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
            login_button = st.form_submit_button("登入", use_container_width=True)
            if login_button:
                role = authenticate(username, password)
                if role:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.role = role
                    st.success(f"歡迎 {username} ({role})")
                    st.rerun()
                else:
                    st.error("帳號或密碼錯誤！")
else:
    # 頂部導覽列
    col_nav1, col_nav2, col_nav3 = st.columns([3, 1, 1])
    with col_nav1:
        st.title("施工日報與利潤分析系統")
    with col_nav2:
        st.write(f"{st.session_state.username} ({st.session_state.role})")
    with col_nav3:
        if st.button("登出", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.username = None
            st.session_state.role = None
            st.rerun()
    
    project_names = get_all_projects()
    
    if st.session_state.role == 'admin':
        tab1, tab2, tab_analysis, tab3, tab4 = st.tabs(["填寫/修改日報", "查看報表", "單項成本分析", "案場管理", "用戶管理"])
        
        # ========== 分頁 1: 填寫/修改日報表 ==========
        with tab1:
            st.subheader("我的日報表")
            my_reports = load_all_reports(creator_filter=st.session_state.username)
            if not my_reports.empty:
                st.info(f"共 {len(my_reports)} 筆日報，點選可進行修改")
                for idx, row in my_reports.head(10).iterrows():
                    with st.expander(f"{row['日期']} - {row['案場']} {row['棟別']} {row['樓層']}"):
                        col_info, col_edit = st.columns([3, 1])
                        with col_info:
                            st.write(f"人員： {row['人員']} ({row['工數']:.1f}工)")
                            st.write(f"產值： ${row['產值']:,.0f}")
                            st.write(f"成本： ${row['總成本']:,.0f}")
                            st.write(f"利潤： ${row['利潤']:,.0f}")
                            st.write(f"工率： {row['工率']:.2f}")
                            if row['更新時間']:
                                st.caption(f"最後更新：{row['更新時間']}")
                        with col_edit:
                            if st.button("修改", key=f"edit_{row['id']}"):
                                st.session_state.editing_report = row['id']
                                st.rerun()
                st.write("---")
            
            is_editing = 'editing_report' in st.session_state and st.session_state.editing_report
            if is_editing:
                st.subheader("修改日報表")
                report_detail = get_report_detail(st.session_state.editing_report)
                if not report_detail:
                    st.error("找不到此日報")
                    del st.session_state.editing_report
                    st.rerun()
                edit_data = {
                    'report_id': st.session_state.editing_report,
                    'report_detail': report_detail,
                    'items': load_report_items(st.session_state.editing_report),
                    'materials': load_report_materials(st.session_state.editing_report),
                    'photos': load_photos(st.session_state.editing_report)
                }
                if st.button("取消修改"):
                    del st.session_state.editing_report
                    st.rerun()
                render_report_form(True, edit_data)
            else:
                st.subheader("填寫新日報")
                render_report_form(False)
        
        # ========== 分頁 2: 查看報表 ==========
        with tab2:
            st.subheader("查看與修正報表")
            st.caption("提示：管理員可直接修改下方欄位與表格，修改後請務必點擊該報表底部的「保存修改」按鈕。")
            
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            with col_f1:
                filter_project_view = st.selectbox("案場", ["全部"] + project_names, key="filter_view")
            with col_f2:
                if filter_project_view != "全部":
                    buildings_view = get_project_buildings(filter_project_view)
                    filter_building_view = st.selectbox("棟別", ["全部"] + buildings_view, key="filter_building_view")
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
                date_to.strftime("%Y-%m-%d") if date_to else None
            )
            
            if not df.empty:
                st.write("---")
                st.write("整體績效")
                
                total_revenue = df['產值'].sum()
                total_labor = df['人力成本'].sum()
                total_material = df['材料成本'].sum()
                total_cost = df['總成本'].sum()
                total_profit = df['利潤'].sum()
                total_workers = df['工數'].sum()
                total_quantity = df['總數量'].sum()
                avg_efficiency = total_quantity / total_workers if total_workers > 0 else 0
                
                col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                col_m1.metric("累計產值", f"${total_revenue:,.0f}")
                col_m2.metric("人力成本", f"${total_labor:,.0f}")
                col_m3.metric("材料成本", f"${total_material:,.0f}")
                col_m4.metric("累計利潤", f"${total_profit:,.0f}")
                
                col_m5, col_m6, col_m7, col_m8 = st.columns(4)
                col_m5.metric("總成本", f"${total_cost:,.0f}")
                col_m6.metric("總工數", f"{total_workers:,.1f}")
                col_m7.metric("平均工率", f"{avg_efficiency:.2f}", help="計算公式：總完成數量 / 總工數")
                profit_rate = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
                col_m8.metric("利潤率", f"{profit_rate:.1f}%")
                
                st.write("---")
                
                # ========== [用戶回饋修正] 樓層產值與工項統計 ==========
                # 核心修正：數量取 MAX（樓層固定量不隨天數累加），工數取 SUM（實際投入工數累加）
                st.subheader("📊 樓層產值與工項統計")
                st.caption("數量 = 該樓層固定量（取最大值，不因多天填報而累加）；工數 = 實際投入累計")
                
                # 工項統計：數量/產值取 MAX，工數取 SUM
                floor_stats_query = '''
                    SELECT 
                        r.building_name,
                        r.floor_name,
                        ri.item_name,
                        ri.unit,
                        MAX(ri.quantity) as max_qty,
                        SUM(ri.worker_count) as total_workers,
                        MAX(ri.unit_price) as unit_price,
                        SUM(CASE WHEN ri.quantity = 0 AND ri.worker_count > 0 THEN 1 ELSE 0 END) as in_progress_count,
                        ri.is_custom
                    FROM reports r
                    JOIN report_items ri ON r.report_id = ri.report_id
                    WHERE 1=1
                '''
                floor_params = []
                
                # 材料費統計
                mat_stats_query = '''
                    SELECT 
                        r.building_name,
                        r.floor_name,
                        SUM(rm.quantity * rm.unit_price) as total_mat_cost
                    FROM reports r
                    JOIN report_materials rm ON r.report_id = rm.report_id
                    WHERE 1=1
                '''
                mat_params = []
                
                filter_cond = ""
                if filter_project_view != "全部":
                    filter_cond += " AND r.project_name = ?"
                    floor_params.append(filter_project_view)
                    mat_params.append(filter_project_view)
                if filter_building_view != "全部":
                    filter_cond += " AND r.building_name = ?"
                    floor_params.append(filter_building_view)
                    mat_params.append(filter_building_view)
                if date_from:
                    filter_cond += " AND r.date >= ?"
                    floor_params.append(date_from.strftime("%Y-%m-%d"))
                    mat_params.append(date_from.strftime("%Y-%m-%d"))
                if date_to:
                    filter_cond += " AND r.date <= ?"
                    floor_params.append(date_to.strftime("%Y-%m-%d"))
                    mat_params.append(date_to.strftime("%Y-%m-%d"))

                floor_stats_query += filter_cond + " GROUP BY r.building_name, r.floor_name, ri.item_name ORDER BY r.building_name, r.floor_name, ri.item_name"
                mat_stats_query += filter_cond + " GROUP BY r.building_name, r.floor_name"

                c = conn.cursor()
                c.execute(floor_stats_query, floor_params)
                floor_data = c.fetchall()
                c.execute(mat_stats_query, mat_params)
                mat_data = c.fetchall()
                
                mat_cost_map = {}
                for m_bldg, m_floor, m_cost in mat_data:
                    mat_cost_map[(m_bldg, m_floor)] = m_cost

                if floor_data:
                    # ========== [用戶回饋核心修正] ==========
                    # 產值 = MAX(數量) * 單價，而非 SUM(revenue)
                    floor_df = pd.DataFrame(floor_data, columns=[
                        '棟別', '樓層', '工項', '單位', '數量(MAX)', '累計工數', '單價', '施工中數', 'is_custom'
                    ])
                    # 正確產值 = 數量(MAX) * 單價
                    floor_df['累計產值'] = floor_df['數量(MAX)'] * floor_df['單價']
                    
                    current_wage = 0
                    if filter_project_view != "全部":
                        current_wage = get_project_wage(filter_project_view)

                    floor_df['預估人力成本'] = floor_df['累計工數'] * current_wage
                    floor_df['項目利潤(僅扣工)'] = floor_df['累計產值'] - floor_df['預估人力成本']
                    
                    floor_df['狀態'] = floor_df.apply(
                        lambda x: "✅ 已完成" if x['施工中數'] == 0 and x['數量(MAX)'] > 0 else "🔄 施工中",
                        axis=1
                    )
                    
                    for building in floor_df['棟別'].unique():
                        building_data = floor_df[floor_df['棟別'] == building]
                        
                        with st.expander(f"🏢 {building} 統計明細", expanded=True):
                            for floor in building_data['樓層'].unique():
                                floor_item_data = building_data[building_data['樓層'] == floor]
                                
                                st.markdown(f"**{floor}**")
                                st.dataframe(
                                    floor_item_data[['工項', '數量(MAX)', '單位', '累計工數', '累計產值', '項目利潤(僅扣工)', '狀態']],
                                    column_config={
                                        "累計產值": st.column_config.NumberColumn(format="$%d"),
                                        "項目利潤(僅扣工)": st.column_config.NumberColumn(format="$%d", label="利潤 (含工不含料)"),
                                        "累計工數": st.column_config.NumberColumn(format="%.1f"),
                                        "數量(MAX)": st.column_config.NumberColumn(format="%.1f", label="數量"),
                                    },
                                    use_container_width=True,
                                    hide_index=True
                                )
                                
                                # ========== [用戶回饋] 樓層合計：工率公式修正 ==========
                                # 工率 = (所有項目數量MAX之和) / 總工數
                                # 例：(200.33 + 245.97) / 9 = 49.58
                                f_rev = floor_item_data['累計產值'].sum()
                                f_labor_cost = floor_item_data['預估人力成本'].sum()
                                f_workers = floor_item_data['累計工數'].sum()
                                f_qty_sum = floor_item_data['數量(MAX)'].sum()
                                f_mat_cost = mat_cost_map.get((building, floor), 0)
                                f_total_profit = f_rev - f_labor_cost - f_mat_cost
                                f_efficiency = f_qty_sum / f_workers if f_workers > 0 else 0

                                c1, c2, c3, c4, c5 = st.columns(5)
                                c1.metric("樓層總產值", f"${f_rev:,.0f}")
                                c2.metric("總人工成本", f"${f_labor_cost:,.0f}", help=f"總工數 {f_workers} x 日薪 {current_wage}")
                                c3.metric("總材料成本", f"${f_mat_cost:,.0f}")
                                c4.metric("樓層淨利潤", f"${f_total_profit:,.0f}", delta=f"{(f_total_profit/f_rev*100) if f_rev>0 else 0:.1f}%")
                                c5.metric("綜合工率", f"{f_efficiency:.2f}", help="所有項目數量總和 / 總工數")
                                
                                st.write("---")
                            
                            bldg_rev = building_data['累計產值'].sum()
                            bldg_labor = building_data['預估人力成本'].sum()
                            bldg_mat = sum([cost for (b, f), cost in mat_cost_map.items() if b == building])
                            bldg_profit = bldg_rev - bldg_labor - bldg_mat
                            
                            col_b1, col_b2, col_b3 = st.columns(3)
                            col_b1.metric(f"{building} 總產值", f"${bldg_rev:,.0f}")
                            col_b2.metric(f"{building} 總材料費", f"${bldg_mat:,.0f}")
                            col_b3.metric(f"{building} 總淨利", f"${bldg_profit:,.0f}")
                else:
                    st.info("無統計資料")

                st.write("---")

                # --- 報表清單 ---
                for (project, building), group_df in df.groupby(['案場', '棟別']):
                    st.markdown(f"{project} - {building}")
                    
                    for idx, row in group_df.iterrows():
                        report_id = row['id']
                        expander_title = f"{row['日期']} | {row['樓層']} | {row['人員']} | 利潤: ${row['利潤']:,.0f}"
                        
                        with st.expander(expander_title, expanded=False):
                            with st.form(key=f"edit_form_{report_id}"):
                                st.markdown("1. 基本資訊")
                                col_e1, col_e2, col_e3 = st.columns(3)
                                with col_e1:
                                    new_date = st.date_input("日期", datetime.strptime(row['日期'], "%Y-%m-%d"))
                                with col_e2:
                                    new_floor = st.text_input("樓層", row['樓層'])
                                with col_e3:
                                    new_workers = st.text_input("施工人員", row['人員'])
                                
                                new_desc = st.text_area("施工描述", row['施工描述'], height=100)

                                # [需求4] 項目編輯：隱藏完成天數欄位
                                st.markdown("2. 完成項目 (可直接修改數值)")
                                items_df = load_report_items(report_id)
                                
                                # 移除「完成天數」欄位顯示
                                display_items_df = items_df.copy()
                                if '完成天數' in display_items_df.columns:
                                    display_items_df = display_items_df.drop(columns=['完成天數'])
                                
                                edited_items_df = st.data_editor(
                                    display_items_df,
                                    column_config={
                                        "產值": st.column_config.NumberColumn(disabled=True, help="自動計算"),
                                        "is_custom": st.column_config.CheckboxColumn("手動項目", disabled=True)
                                    },
                                    num_rows="dynamic",
                                    use_container_width=True,
                                    key=f"editor_items_{report_id}"
                                )

                                st.markdown("3. 材料使用 (可直接修改數值)")
                                materials_df = load_report_materials(report_id)
                                
                                edited_materials_df = st.data_editor(
                                    materials_df,
                                    column_config={
                                        "成本": st.column_config.NumberColumn(disabled=True, help="自動計算")
                                    },
                                    num_rows="dynamic",
                                    use_container_width=True,
                                    key=f"editor_materials_{report_id}"
                                )

                                if row['照片數'] > 0:
                                    st.markdown("4. 施工照片")
                                    photos = load_photos(report_id)
                                    if photos:
                                        p_cols = st.columns(min(3, len(photos)))
                                        for i, photo in enumerate(photos):
                                            with p_cols[i % 3]:
                                                st.image(photo['data'], caption=photo['name'], use_container_width=True)

                                st.write("---")
                                
                                col_btn1, col_btn2 = st.columns([1, 4])
                                with col_btn1:
                                    delete_submitted = st.form_submit_button("刪除此日報", type="secondary")
                                with col_btn2:
                                    save_submitted = st.form_submit_button("保存修改並重新計算", type="primary", use_container_width=True)

                                if save_submitted:
                                    new_items_list = []
                                    total_revenue = 0
                                    total_item_workers = 0
                                    total_qty = 0
                                    
                                    for _, item_row in edited_items_df.iterrows():
                                        q = float(item_row['數量']) if item_row['數量'] else 0
                                        p = float(item_row['單價']) if item_row['單價'] else 0
                                        rev = q * p
                                        
                                        new_items_list.append({
                                            'item_name': item_row['項目名稱'],
                                            'quantity': q,
                                            'unit': item_row['單位'],
                                            'unit_price': p,
                                            'revenue': rev,
                                            'completion_days': 1.0,  # [需求4] 固定為 1
                                            'worker_count': float(item_row['計工']),
                                            'is_custom': item_row.get('is_custom', False)
                                        })
                                        total_revenue += rev
                                        total_item_workers += float(item_row['計工'])
                                        total_qty += q

                                    new_materials_list = []
                                    total_mat_cost = 0
                                    
                                    for _, mat_row in edited_materials_df.iterrows():
                                        q = float(mat_row['數量']) if mat_row['數量'] else 0
                                        p = float(mat_row['單價']) if mat_row['單價'] else 0
                                        c_val = q * p
                                        
                                        new_materials_list.append({
                                            'material_name': mat_row['材料名稱'],
                                            'quantity': q,
                                            'unit': mat_row['單位'],
                                            'unit_price': p,
                                            'cost': c_val
                                        })
                                        total_mat_cost += c_val

                                    worker_list = [w.strip() for w in re.split(r'[,，、]', new_workers) if w.strip()]
                                    actual_worker_count = total_item_workers if total_item_workers > 0 else len(worker_list)
                                    
                                    wage = get_project_wage(project)
                                    labor_cost = actual_worker_count * wage
                                    total_cost = labor_cost + total_mat_cost
                                    profit = total_revenue - total_cost
                                    efficiency = total_qty / actual_worker_count if actual_worker_count > 0 else 0

                                    update_data = {
                                        'date': new_date.strftime("%Y-%m-%d"),
                                        'project_name': project,
                                        'building_name': building,
                                        'floor_name': new_floor,
                                        'workers': new_workers,
                                        'worker_count': actual_worker_count,
                                        'labor_cost': labor_cost,
                                        'description': new_desc,
                                        'photo_count': row['照片數'],
                                        'revenue': total_revenue,
                                        'material_cost': total_mat_cost,
                                        'total_cost': total_cost,
                                        'profit': profit,
                                        'efficiency': efficiency
                                    }

                                    update_report(report_id, update_data, new_items_list, new_materials_list, photos=None)
                                    st.success(f"已更新！新利潤：${profit:,.0f}")
                                    st.rerun()

                                if delete_submitted:
                                    delete_report(report_id)
                                    st.warning("已刪除該筆資料")
                                    st.rerun()
                
                st.write("---")
                
                st.write("匯出資料")
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="下載完整報表（CSV）",
                    data=csv,
                    file_name=f"工程日報_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
                
                st.write("趨勢圖表")
                chart_data = df.groupby('日期').agg({
                    '產值': 'sum', '總成本': 'sum', '利潤': 'sum'
                }).reset_index()
                st.line_chart(chart_data.set_index('日期'))
                
                st.write("---")
                st.write("危險操作")
                if st.button("清空所有日報資料", type="secondary"):
                    if st.checkbox("確認要刪除所有資料？此動作無法復原！"):
                        delete_all_reports()
                        st.success("已清空所有資料")
                        st.rerun()
            else:
                st.info("尚無符合條件的日報表")

        # ========== [需求6 修正] 單項成本分析：以樓層+工項為主，非日期 ==========
        with tab_analysis:
            st.subheader("單項工程累計分析")
            st.caption("依樓層與工項統計。數量取最大值（樓層固定量），工數取累加值（實際投入）。")

            col_a1, col_a2 = st.columns(2)
            with col_a1:
                ana_project = st.selectbox("選擇案場", project_names, key="ana_proj")
            with col_a2:
                if ana_project:
                    ana_buildings = get_project_buildings(ana_project)
                    ana_bldg = st.selectbox("選擇棟別", ["全部"] + ana_buildings, key="ana_bldg")
                else:
                    ana_bldg = "全部"
            
            col_a3, col_a4 = st.columns(2)
            with col_a3:
                if ana_project and ana_bldg != "全部":
                    ana_floors = get_building_floors(ana_project, ana_bldg)
                    ana_floor = st.selectbox("選擇樓層", ["全部"] + ana_floors, key="ana_floor")
                else:
                    ana_floor = "全部"
            with col_a4:
                if ana_project:
                    c = conn.cursor()
                    item_query = '''
                        SELECT DISTINCT ri.item_name 
                        FROM report_items ri 
                        JOIN reports r ON ri.report_id = r.report_id 
                        WHERE r.project_name = ?
                    '''
                    item_params = [ana_project]
                    if ana_bldg != "全部":
                        item_query += " AND r.building_name = ?"
                        item_params.append(ana_bldg)
                    if ana_floor != "全部":
                        item_query += " AND r.floor_name LIKE ?"
                        item_params.append(f"%{ana_floor}%")
                    item_query += " ORDER BY ri.item_name"
                    c.execute(item_query, item_params)
                    available_items = [row[0] for row in c.fetchall()]
                    ana_item = st.selectbox("選擇工項", ["全部"] + available_items, key="ana_item")
                else:
                    ana_item = "全部"

            if ana_project:
                # ========== [用戶回饋核心修正] 數量取 MAX 而非 SUM ==========
                query = '''
                    SELECT 
                        r.floor_name,
                        ri.item_name,
                        ri.unit,
                        MAX(ri.quantity) as max_qty,
                        SUM(ri.worker_count) as total_workers,
                        MAX(ri.unit_price) as unit_price,
                        COUNT(DISTINCT r.date) as work_days
                    FROM report_items ri
                    JOIN reports r ON ri.report_id = r.report_id
                    WHERE r.project_name = ?
                '''
                params = [ana_project]

                if ana_bldg != "全部":
                    query += " AND r.building_name = ?"
                    params.append(ana_bldg)
                if ana_floor != "全部":
                    query += " AND r.floor_name LIKE ?"
                    params.append(f"%{ana_floor}%")
                if ana_item != "全部":
                    query += " AND ri.item_name = ?"
                    params.append(ana_item)
                
                query += " GROUP BY r.floor_name, ri.item_name ORDER BY r.floor_name, ri.item_name"
                
                c = conn.cursor()
                c.execute(query, params)
                ana_data = c.fetchall()

                if ana_data:
                    daily_wage = get_project_wage(ana_project)
                    
                    ana_df = pd.DataFrame(ana_data, columns=['樓層', '項目名稱', '單位', '數量', '累計工數', '單價', '施工天數'])
                    
                    # ========== [用戶回饋] 產值 = MAX(數量) * 單價 ==========
                    ana_df['累計產值'] = ana_df['數量'] * ana_df['單價']
                    ana_df['預估人力成本'] = ana_df['累計工數'] * daily_wage
                    ana_df['項目利潤'] = ana_df['累計產值'] - ana_df['預估人力成本']
                    ana_df['利潤率'] = ana_df.apply(lambda x: (x['項目利潤'] / x['累計產值'] * 100) if x['累計產值'] > 0 else 0, axis=1)
                    # 工率 = 數量(MAX) / 累計工數
                    ana_df['平均工率'] = ana_df.apply(lambda x: (x['數量'] / x['累計工數']) if x['累計工數'] > 0 else 0, axis=1)

                    st.dataframe(
                        ana_df,
                        column_config={
                            "數量": st.column_config.NumberColumn(format="%.1f", help="樓層固定量（取最大值）"),
                            "累計產值": st.column_config.NumberColumn(format="$%d"),
                            "預估人力成本": st.column_config.NumberColumn(format="$%d", help=f"以日薪 ${daily_wage} 計算"),
                            "項目利潤": st.column_config.NumberColumn(format="$%d"),
                            "利潤率": st.column_config.NumberColumn(format="%.1f%%"),
                            "平均工率": st.column_config.NumberColumn(format="%.2f", help="數量 / 累計工數"),
                            "施工天數": st.column_config.NumberColumn(help="有紀錄該項目的天數")
                        },
                        use_container_width=True
                    )

                    st.write("---")
                    col_tot1, col_tot2, col_tot3 = st.columns(3)
                    total_proj_rev = ana_df['累計產值'].sum()
                    total_proj_cost = ana_df['預估人力成本'].sum()
                    total_proj_profit = ana_df['項目利潤'].sum()
                    
                    col_tot1.metric("篩選條件總產值", f"${total_proj_rev:,.0f}")
                    col_tot2.metric("篩選條件人力成本", f"${total_proj_cost:,.0f}")
                    col_tot3.metric("篩選條件總毛利", f"${total_proj_profit:,.0f}", delta=f"{(total_proj_profit/total_proj_rev*100) if total_proj_rev>0 else 0:.1f}%")
                    st.caption("注意：此處成本僅計算該項目歸屬的人力成本，不包含額外填寫的材料費。")

                else:
                    st.info("無符合條件的資料")

        # ========== 分頁 3: 案場管理 ==========
        with tab3:
            st.subheader("案場管理")
            
            with st.expander("新增案場", expanded=False):
                with st.form("create_project_form"):
                    new_project_name = st.text_input("案場名稱")
                    copy_option = st.checkbox("複製既有案場的設定")
                    copy_from = None
                    if copy_option and project_names:
                        copy_from = st.selectbox("選擇要複製的案場", project_names)
                    create_btn = st.form_submit_button("新增", use_container_width=True)
                    if create_btn:
                        if new_project_name:
                            if create_project(new_project_name, copy_from if copy_option else None):
                                if copy_from:
                                    st.success(f"已新增案場：{new_project_name}（已複製 {copy_from} 的設定）")
                                else:
                                    st.success(f"已新增案場：{new_project_name}")
                                st.rerun()
                            else:
                                st.error("案場名稱已存在")
                        else:
                            st.error("請輸入案場名稱")
            
            st.write("---")
            
            if project_names:
                selected_manage_project = st.selectbox("選擇要管理的案場", project_names, key="manage_project")
                
                if selected_manage_project:
                    st.write(f"{selected_manage_project}")
                    
                    current_wage = get_project_wage(selected_manage_project)
                    new_wage = st.number_input("每人每日工資", min_value=0, value=current_wage, step=100)
                    if new_wage != current_wage:
                        if st.button("更新工資"):
                            update_project_wage(selected_manage_project, new_wage)
                            st.success("工資已更新")
                            st.rerun()
                    
                    st.write("---")
                    
                    tab_building, tab_material, tab_extra_work = st.tabs(["棟別樓層項目", "材料設定", "額外工項設定"])
                    
                    with tab_building:
                        st.write("棟別樓層項目管理")
                        
                        with st.expander("新增棟別樓層 / 批量新增", expanded=True):
                            col_b1, col_b2 = st.columns(2)
                            with col_b1:
                                new_building = st.text_input("棟別名稱", placeholder="例如：A棟")
                            
                            add_mode = st.radio("模式", ["單一樓層", "批量樓層 (如 3F-15F)"], horizontal=True)
                            
                            if add_mode == "單一樓層":
                                with col_b2:
                                    new_floor = st.text_input("樓層名稱", placeholder="例如：1F")
                                if st.button("新增棟別樓層"):
                                    if new_building and new_floor:
                                        if add_building_floor(selected_manage_project, new_building, new_floor):
                                            st.success(f"已新增：{new_building} {new_floor}")
                                            st.rerun()
                                        else:
                                            st.error("此棟別樓層已存在")
                                    else:
                                        st.error("請填寫完整資訊")
                            else:
                                with col_b2:
                                    col_start, col_end = st.columns(2)
                                    with col_start:
                                        start_floor = st.number_input("起始樓層", min_value=1, value=2, step=1)
                                    with col_end:
                                        end_floor = st.number_input("結束樓層", min_value=1, value=10, step=1)
                                
                                existing_floors = []
                                if new_building:
                                    existing_floors = get_building_floors(selected_manage_project, new_building)
                                copy_source = st.selectbox("複製項目自 (可選)", ["無"] + existing_floors)
                                
                                if st.button("批量新增"):
                                    if new_building and start_floor <= end_floor:
                                        source = copy_source if copy_source != "無" else None
                                        count = batch_add_floors(selected_manage_project, new_building, int(start_floor), int(end_floor), source)
                                        if count > 0:
                                            st.success(f"已成功新增 {count} 個樓層")
                                            st.rerun()
                                        else:
                                            st.warning("沒有樓層被新增 (可能已存在)")
                                    else:
                                        st.error("請檢查輸入資訊")
                        
                        st.write("---")
                        
                        buildings = get_project_buildings(selected_manage_project)
                        
                        if buildings:
                            for building in buildings:
                                with st.expander(f"{building}", expanded=False):
                                    floors = get_building_floors(selected_manage_project, building)
                                    
                                    for floor in floors:
                                        st.write(f"{floor}")
                                        
                                        # [需求1] 顯示鎖定狀態
                                        locked = get_locked_items(selected_manage_project, building, floor)
                                        if locked:
                                            st.caption(f"🔒 已鎖定項目：{', '.join(locked)}")
                                        
                                        floor_items = get_floor_items(selected_manage_project, building, floor)
                                        
                                        if floor_items:
                                            items_df = pd.DataFrame(floor_items, columns=['項目名稱', '標準數量', '單位', '單價'])
                                            items_df['產值'] = items_df['標準數量'] * items_df['單價']
                                            st.dataframe(items_df, use_container_width=True, hide_index=True)
                                            
                                            st.write("刪除項目")
                                            item_to_delete = st.selectbox(
                                                "選擇要刪除的項目", 
                                                [item[0] for item in floor_items],
                                                key=f"del_item_select_{building}_{floor}"
                                            )
                                            if st.button(f"刪除項目：{item_to_delete}", key=f"del_item_btn_{building}_{floor}"):
                                                delete_floor_item(selected_manage_project, building, floor, item_to_delete)
                                                st.success(f"已刪除項目：{item_to_delete}")
                                                st.rerun()
                                        else:
                                            st.info("此樓層尚無項目")
                                        
                                        with st.form(f"add_item_{building}_{floor}"):
                                            st.write("新增項目")
                                            col_i1, col_i2, col_i3, col_i4 = st.columns(4)
                                            with col_i1:
                                                item_name = st.text_input("項目名稱", key=f"item_name_{building}_{floor}")
                                            with col_i2:
                                                item_qty = st.number_input("標準數量", min_value=0.0, step=0.5, key=f"item_qty_{building}_{floor}")
                                            with col_i3:
                                                item_unit = st.text_input("單位", key=f"item_unit_{building}_{floor}")
                                            with col_i4:
                                                item_price = st.number_input("單價", min_value=0, step=50, key=f"item_price_{building}_{floor}")
                                            
                                            add_item_btn = st.form_submit_button("新增")
                                            if add_item_btn:
                                                if item_name and item_qty > 0 and item_unit and item_price > 0:
                                                    if add_floor_item(selected_manage_project, building, floor, item_name, item_qty, item_unit, item_price):
                                                        st.success(f"已新增項目：{item_name}")
                                                        st.rerun()
                                                    else:
                                                        st.error("此項目已存在")
                                                else:
                                                    st.error("請填寫完整資訊")
                                        
                                        if st.button(f"刪除 {building} {floor}", key=f"del_floor_{building}_{floor}"):
                                            delete_building_floor(selected_manage_project, building, floor)
                                            st.success(f"已刪除 {building} {floor}")
                                            st.rerun()
                                        
                                        st.write("---")
                        else:
                            st.info("此案場尚未設定棟別樓層")
                    
                    with tab_material:
                        st.write("材料設定")
                        
                        with st.expander("從其他案場匯入材料"):
                            other_projects = [p for p in project_names if p != selected_manage_project]
                            if other_projects:
                                source_proj = st.selectbox("選擇來源案場", other_projects)
                                if st.button("匯入材料"):
                                    if copy_materials_from_project(selected_manage_project, source_proj):
                                        st.success(f"已從 {source_proj} 匯入材料")
                                        st.rerun()
                                    else:
                                        st.error("匯入失敗")
                            else:
                                st.info("沒有其他案場可供匯入")

                        with st.expander("新增材料"):
                            col_m1, col_m2, col_m3 = st.columns(3)
                            with col_m1:
                                new_mat_name = st.text_input("材料名稱", placeholder="例如：500")
                            with col_m2:
                                new_mat_unit = st.text_input("單位", placeholder="例如：組")
                            with col_m3:
                                new_mat_price = st.number_input("單價", min_value=0, step=50, key="new_mat_price")
                            if st.button("新增材料"):
                                if new_mat_name and new_mat_unit and new_mat_price > 0:
                                    if add_material(selected_manage_project, new_mat_name, new_mat_unit, new_mat_price):
                                        st.success(f"已新增材料：{new_mat_name}")
                                        st.rerun()
                                    else:
                                        st.error("此材料已存在")
                                else:
                                    st.error("請填寫完整資訊")
                        
                        st.write("---")
                        
                        materials = get_project_materials(selected_manage_project)
                        if materials:
                            st.write("材料清單")
                            for mat_name, mat_unit, mat_price in materials:
                                col_m1, col_m2, col_m3, col_m4 = st.columns([2, 1, 1, 1])
                                with col_m1:
                                    st.write(f"{mat_name}")
                                with col_m2:
                                    st.write(f"{mat_unit}")
                                with col_m3:
                                    st.write(f"${mat_price:,.0f}")
                                with col_m4:
                                    if st.button("刪除", key=f"del_mat_{mat_name}"):
                                        delete_material(selected_manage_project, mat_name)
                                        st.success(f"已刪除材料：{mat_name}")
                                        st.rerun()
                        else:
                            st.info("此案場尚未設定材料")
                    
                    with tab_extra_work:
                        st.write("額外工項設定")
                        st.caption("在此設定日報報表「額外施作項目」的下拉選單選項")
                        
                        with st.expander("新增額外工項", expanded=True):
                            new_extra_item = st.text_input("工項名稱", placeholder="例如：試水、抽水、清理現場")
                            if st.button("新增工項", key="add_extra_work_btn"):
                                if new_extra_item:
                                    if add_extra_work_item(selected_manage_project, new_extra_item):
                                        st.success(f"已新增工項：{new_extra_item}")
                                        st.rerun()
                                    else:
                                        st.error("此工項已存在")
                                else:
                                    st.error("請輸入工項名稱")
                        
                        st.write("---")
                        
                        extra_items = get_extra_work_items(selected_manage_project)
                        if extra_items:
                            st.write("工項清單")
                            for item_name in extra_items:
                                col_e1, col_e2 = st.columns([4, 1])
                                with col_e1:
                                    st.write(f"{item_name}")
                                with col_e2:
                                    if st.button("刪除", key=f"del_extra_{item_name}"):
                                        delete_extra_work_item(selected_manage_project, item_name)
                                        st.success(f"已刪除工項：{item_name}")
                                        st.rerun()
                        else:
                            st.info("此案場尚未設定額外工項")
                    
                    st.write("---")
                    
                    st.write("危險操作")
                    delete_confirm_key = f"delete_confirm_{selected_manage_project}"
                    if delete_confirm_key not in st.session_state:
                        st.session_state[delete_confirm_key] = False
                    if st.button(f"刪除案場：{selected_manage_project}", type="secondary"):
                        st.session_state[delete_confirm_key] = True
                        st.rerun()
                    if st.session_state[delete_confirm_key]:
                        st.warning(f"確定要刪除 {selected_manage_project}？此動作將刪除所有相關資料且無法復原！")
                        col_del1, col_del2 = st.columns(2)
                        with col_del1:
                            if st.button("確認刪除", type="primary", key=f"confirm_del_{selected_manage_project}"):
                                delete_project(selected_manage_project)
                                st.session_state[delete_confirm_key] = False
                                st.success(f"已刪除案場：{selected_manage_project}")
                                st.rerun()
                        with col_del2:
                            if st.button("取消", key=f"cancel_del_{selected_manage_project}"):
                                st.session_state[delete_confirm_key] = False
                                st.rerun()
            else:
                st.info("尚無案場，請先新增案場")
        
        # ========== 分頁 4: 用戶管理 ==========
        with tab4:
            st.subheader("用戶管理")
            
            with st.expander("新增用戶"):
                with st.form("create_user_form"):
                    col_u1, col_u2, col_u3 = st.columns(3)
                    with col_u1:
                        new_username = st.text_input("帳號")
                    with col_u2:
                        new_password = st.text_input("密碼", type="password")
                    with col_u3:
                        new_role = st.selectbox("角色", ["user", "admin"])
                    create_user_btn = st.form_submit_button("新增", use_container_width=True)
                    if create_user_btn:
                        if new_username and new_password:
                            if create_user(new_username, new_password, new_role):
                                st.success(f"已新增用戶：{new_username} ({new_role})")
                                st.rerun()
                            else:
                                st.error("帳號已存在")
                        else:
                            st.error("請填寫完整資訊")
            
            st.write("---")
            
            st.write("用戶列表")
            users = get_all_users()
            for user_id, username, role, created_at in users:
                col_u1, col_u2, col_u3, col_u4 = st.columns([2, 1, 2, 1])
                with col_u1:
                    st.write(f"{username}")
                with col_u2:
                    role_badge = "管理員" if role == "admin" else "用戶"
                    st.write(role_badge)
                with col_u3:
                    st.write(f"{created_at[:10]}")
                with col_u4:
                    if username != st.session_state.username:
                        if st.button("刪除", key=f"del_user_{user_id}"):
                            delete_user(user_id)
                            st.success(f"已刪除用戶：{username}")
                            st.rerun()
                    else:
                        st.write("（本人）")
            
            st.write("---")
            
            st.write("修改密碼")
            with st.form("change_password_form"):
                target_user = st.selectbox("選擇用戶", [u[1] for u in users])
                new_pwd = st.text_input("新密碼", type="password")
                change_pwd_btn = st.form_submit_button("修改密碼", use_container_width=True)
                if change_pwd_btn:
                    if new_pwd:
                        change_password(target_user, new_pwd)
                        st.success(f"已修改 {target_user} 的密碼")
                    else:
                        st.error("請輸入新密碼")
    
    else:
        # ========== 一般用戶 ==========
        st.subheader("我的日報表")
        
        my_reports = load_all_reports(creator_filter=st.session_state.username)
        if not my_reports.empty:
            st.info(f"共 {len(my_reports)} 筆日報，點選可進行修改")
            for idx, row in my_reports.head(10).iterrows():
                with st.expander(f"{row['日期']} - {row['案場']} {row['棟別']} {row['樓層']}"):
                    col_info, col_edit = st.columns([3, 1])
                    with col_info:
                        st.write(f"人員： {row['人員']} ({row['工數']:.1f}工)")
                        st.write(f"產值： ${row['產值']:,.0f}")
                        st.write(f"成本： ${row['總成本']:,.0f}")
                        st.write(f"利潤： ${row['利潤']:,.0f}")
                        st.write(f"工率： {row['工率']:.2f}")
                        if row['更新時間']:
                            st.caption(f"最後更新：{row['更新時間']}")
                    with col_edit:
                        if st.button("修改", key=f"user_edit_{row['id']}"):
                            st.session_state.editing_report = row['id']
                            st.rerun()
        
        st.write("---")
        
        is_editing = 'editing_report' in st.session_state and st.session_state.editing_report
        if is_editing:
            st.subheader("修改日報表")
            report_detail = get_report_detail(st.session_state.editing_report)
            if not report_detail:
                st.error("找不到此日報")
                del st.session_state.editing_report
                st.rerun()
            edit_data = {
                'report_id': st.session_state.editing_report,
                'report_detail': report_detail,
                'items': load_report_items(st.session_state.editing_report),
                'materials': load_report_materials(st.session_state.editing_report),
                'photos': load_photos(st.session_state.editing_report)
            }
            if st.button("取消修改"):
                del st.session_state.editing_report
                st.rerun()
            render_report_form(True, edit_data)
        else:
            st.subheader("填寫新日報")
            render_report_form(False)
