# -*- coding: utf-8 -*-
import json
import pyodbc
import requests
import time
import os
import logging
import sys
import traceback
import concurrent.futures
from dotenv import load_dotenv
from datetime import datetime

# ==================== 日誌與路徑設定 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)

today_str = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"AILearner_{today_str}.log")
log_formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('AutoLearner')
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# ==================== 環境變數 ====================
load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))
SQL_SERVER, SQL_DATABASE = 'host.docker.internal', 'MicrosoftRDB'
RULES_FILE = os.path.join(BASE_DIR, 'dynamic_category_rules.json')

api_keys_raw = os.getenv("GEMINI_API_KEYS", "").strip().replace('"', '').replace("'", "")
VERTEX_AI_KEY = os.getenv("VERTEX_AI_KEY", "").strip().replace('"', '').replace("'", "")
GEMINI_API_KEYS = api_keys_raw.split(',')
CURRENT_API_KEY_INDEX = 0

LEARN_BATCH_SIZE = 100 
AI_BATCH_SIZE = 50     

def connect_db():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={db_user};PWD={db_password};" 
        "Encrypt=no;TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def safe_parse_ai_json(raw_response):
    try:
        raw_text = raw_response.json()['candidates'][0]['content']['parts'][0]['text']
        clean_json = raw_text.strip().replace('```json', '').replace('```', '').strip()
        data = json.loads(clean_json)
        
        rebuilt = {}
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    k = str(item.get('keyword') or item.get('Keyword') or next(iter(item.values()))).strip()
                    v = str(item.get('category') or item.get('Category') or list(item.values())[-1]).strip()
                    rebuilt[k] = v
        elif isinstance(data, dict):
            for k, v in data.items():
                rebuilt[str(k).strip()] = str(v).strip()
        
        return rebuilt
    except Exception as e:
        logger.error(f"解析 JSON 失敗: {e}")
        return {}

def call_ai_for_deep_learning(keywords_list):
    """呼叫 AI 進行分類"""
    global CURRENT_API_KEY_INDEX
    if not keywords_list: return {}
    
    keywords_str = ", ".join([f'"{kw}"' for kw in keywords_list])
    prompt = f"請將以下關鍵字分類（Technology, Entertainment, Sports, Business, Politics, News, Education, Lifestyle）。如果真的無法歸類請填 Other。請只回傳 JSON 物件格式：{{\"關鍵字\": \"類別\"}}。關鍵字：{keywords_str}"

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1}
    }

    headers = {"Content-Type": "application/json"}
    if VERTEX_AI_KEY:
        try:
            url = f"https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash-lite:generateContent?key={VERTEX_AI_KEY}"
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                return safe_parse_ai_json(res)
        except Exception as e:
            logger.warning(f"Vertex AI 異常: {e}")

    for _ in range(len(GEMINI_API_KEYS) * 2):
        key = GEMINI_API_KEYS[CURRENT_API_KEY_INDEX]
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}"
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                return safe_parse_ai_json(res)
        except: pass
        CURRENT_API_KEY_INDEX = (CURRENT_API_KEY_INDEX + 1) % len(GEMINI_API_KEYS)
    
    return {}

def run_auto_learner():
    logger.info("="*60 + "\n啟動自動學習模組\n" + "="*60)

    if not os.path.exists(RULES_FILE):
        rules = { 'Technology': [], 'Entertainment': [], 'Sports': [], 'Business': [], 'Politics': [], 'News': [], 'Education': [], 'Lifestyle': [], 'Other': [] }
    else:
        with open(RULES_FILE, 'r', encoding='utf-8') as f: 
            rules = json.load(f)

    conn = connect_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM KeywordsMaster WHERE Category = 'Other' OR Category IS NULL")
        total_other = cursor.fetchone()[0]
        logger.info(f"資料庫中共有 {total_other} 個 'Other' 或 'NULL' 詞彙需要學習。")

        if total_other == 0: 
            logger.info("目前沒有需要學習的詞彙。")
            return

        cursor.execute(f"SELECT TOP {LEARN_BATCH_SIZE} KeywordID, Keyword FROM KeywordsMaster WHERE Category = 'Other' OR Category IS NULL ORDER BY CreatedAt DESC")
        rows = cursor.fetchall()
        keyword_map = {row[1].strip(): row[0] for row in rows}
        all_keywords = list(keyword_map.keys())
        ai_results = {}
        batches = [all_keywords[i:i + AI_BATCH_SIZE] for i in range(0, len(all_keywords), AI_BATCH_SIZE)]
        
        logger.info(f"將 {len(all_keywords)} 個關鍵字分為 {len(batches)} 個批次進行 AI 學習...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(batches))) as executor:
            future_to_batch = {executor.submit(call_ai_for_deep_learning, batch): i for i, batch in enumerate(batches)}
            for future in concurrent.futures.as_completed(future_to_batch):
                try:
                    res = future.result()
                    if res:
                        ai_results.update(res)
                except Exception as exc:
                    logger.error(f"批次學習發生錯誤: {exc}")
        
        updated_count, db_updates = 0, []
        for kw, cat in ai_results.items():
            clean_kw = kw.strip()
            
            if clean_kw in keyword_map and cat in rules and cat not in ["Unknown", "Other"]:

                if clean_kw not in rules[cat]:
                    rules[cat].append(clean_kw)
                    updated_count += 1

                db_updates.append((cat, keyword_map[clean_kw]))

        if updated_count > 0:
            with open(RULES_FILE, 'w', encoding='utf-8') as f: 
                json.dump(rules, f, ensure_ascii=False, indent=4)
            logger.info(f"規則檔已擴充 {updated_count} 個新詞彙。")

        if db_updates:
            cursor.executemany("UPDATE KeywordsMaster SET Category = ? WHERE KeywordID = ?", db_updates)
            conn.commit()
            logger.info(f"資料庫已成功導正 {len(db_updates)} 筆分類。")
        else:
            logger.info("本次執行未發現可明確歸類的新詞彙。")

    except Exception as e:
        logger.error(f"自動學習執行失敗: {e}")
        traceback.print_exc()
    finally:
        conn.close()
        logger.info("資料庫連線已關閉。\n")

    logger.info("="*60 + "\n自動學習模組執行完畢\n" + "="*60)

if __name__ == "__main__":
    run_auto_learner()