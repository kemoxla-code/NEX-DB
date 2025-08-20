# loaders.py
import os
import pandas as pd
import sqlite3
import chardet

SUPPORTED_EXTS = {'.csv', '.xlsx', '.db', '.sqlite3'}

def discover_files(folder_path):
    """يبحث في المجلد عن الملفات المدعومة (.csv, .xlsx, .db, .sqlite3)."""
    files = []
    for root, _, filenames in os.walk(folder_path):
        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower()
            if ext in SUPPORTED_EXTS:
                files.append(os.path.join(root, fn))
    return files

def load_csv(path):
    """
    يحاول قراءة ملف CSV بعدة ترميزات شائعة.
    يُرجع: DataFrame و الترميز المستخدم.
    """
    # 1. اقرأ بعض البايتات لتحديد الترميز
    with open(path, 'rb') as f:
        raw = f.read(20_000)   # قراءة أول 10 كيلوبايت فقط للتسريع
    detected = chardet.detect(raw).get('encoding')

    # 2. قائمة الترميزات التي سنجربها
    candidates = [
        detected,
        'utf-8',
        'utf-8-sig',
        'cp1252',
        'cp1256',
        'latin1',
        'iso-8859-1',
        'ascii'
    ]

    # 3. جرب كل ترميز حتى ينجح
    for enc in candidates:
        if not enc:
            continue
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"🔍 Loaded CSV with encoding: {enc}")
            
            return df, enc
        except Exception:
            continue

    # 4. حل أخير: قراءة بـ utf-8 وتجاهل/استبدال الأخطاء
    print("All encodings failed, using utf-8 with replacement of invalid chars")
    df = pd.read_csv(path, encoding='utf-8', errors='replace')
    return df, 'utf-8 (fallback)'

def load_xlsx(path):
    """يقرأ ملفات Excel."""
    return pd.read_excel(path, engine='openpyxl')

def load_sqlite(path):
    """يتصل بقاعدة SQLite ويحمّل كل الجداول في dict."""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    dfs = {}
    for tbl in tables:
        dfs[f"({tbl})"] = pd.read_sql_query(f"SELECT * FROM `{tbl}`", conn)
    conn.close()
    return dfs
