# -*- coding: utf-8 -*-
# !/usr/bin/env python

import time
import random
import pyodbc
import os
import sys
import shutil
import urllib.parse
import threading
import glob
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures

if hasattr(sys.stdout, 'buffer'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
if not os.path.exists(LOG_DIR): 
    os.makedirs(LOG_DIR)

time_str = datetime.now().strftime("%Y-%m-%d_%A_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"Search_Logs_Backfill_{time_str}.log")

class DualLogger:
    def __init__(self, original_stream, log_path):
        self.original_stream = original_stream
        self.log_file = open(log_path, "a", encoding="utf-8")
        self.lock = threading.Lock()  

    def write(self, message):
        with self.lock:
            self.original_stream.write(message)
            self.original_stream.flush()
            self.log_file.write(message)
            self.log_file.flush()

    def flush(self):
        with self.lock:
            self.original_stream.flush()
            self.log_file.flush()

sys.stdout = DualLogger(sys.stdout, LOG_FILE)
sys.stderr = DualLogger(sys.stderr, LOG_FILE)

SQL_SERVER = 'host.docker.internal' 
SQL_DATABASE = 'MicrosoftRDB'
DRIVER_PATH = "/usr/bin/chromedriver"
MAX_WORKERS = 2 

def init_driver_for_thread():
    """為每個執行緒初始化專屬的輕量化 Chrome"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-extensions") 
    options.add_argument("--mute-audio") 
    options.add_argument("--disable-accelerated-video-decode") 
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-3d-apis")
    options.add_argument("--no-zygote") 
    options.page_load_strategy = 'eager'
    
    prefs = {
        "profile.managed_default_content_settings.images": 2, 
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.media_stream": 2
    }
    options.add_experimental_option("prefs", prefs)
    
    # 使用 Thread ID 與時間戳記確保暫存資料夾絕對不重複
    thread_id = threading.get_ident()
    custom_profile = f"/tmp/Chrome_Temp_{thread_id}_{int(time.time())}_{random.randint(10,99)}"
    os.makedirs(custom_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={custom_profile}")
    
    service = Service(executable_path=DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(15) 
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver, custom_profile

def process_single_keyword(kid, kw):
    """Worker 執行的單一任務：開啟瀏覽器 -> 抓取 -> 關閉並清理"""
    driver = None
    profile_path = None
    try:
        driver, profile_path = init_driver_for_thread()
        search_url = f"https://www.bing.com/search?q={urllib.parse.quote(kw)}"
        driver.get(search_url)
        
        time.sleep(random.uniform(1.5, 2.5))
        try: driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
        except: pass
        
        try: WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, "b_results")))
        except: pass
        
        selectors = [
            "//li[@class='b_algo']//div[@class='b_caption']/p",
            "//li[@class='b_algo']//p",
            "//div[@id='b_results']//p"
        ]
        
        summary_text = ""
        for selector in selectors:
            try:
                elem = driver.find_element(By.XPATH, selector)
                if elem and elem.text.strip():
                    summary_text = elem.text.strip()
                    break
            except:
                continue
                
        if summary_text:
            print(f"  [成功] '{kw}': {summary_text[:30]}...")
            return (kid, summary_text[:150] + "...", "Success", None)
        else:
            print(f"  [失敗] '{kw}': 找不到內容")
            return (kid, "", "Fail", "無法擷取有效摘要")
            
    except Exception as e:
        print(f"  [異常] '{kw}': 發生錯誤 ({str(e)[:50]})")
        return (kid, "", "Fail", str(e))
        
    finally:
        if driver:
            try: driver.quit()
            except: pass
        if profile_path and os.path.exists(profile_path):
            try: shutil.rmtree(profile_path, ignore_errors=True)
            except: pass

def main():
    print("=" * 60)
    print(f"啟動資料庫補抓程式 (多執行緒加速版, Worker: {MAX_WORKERS})")
    print("=" * 60)

    try:
        load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
            f"UID={db_user};PWD={db_password};" 
            "Encrypt=no;TrustServerCertificate=yes"
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        print(f"[INFO] 資料庫連線成功")
    except Exception as e:
        print(f"[錯誤] 資料庫連線失敗: {e}")
        return

    query_missing = """
        SELECT m.KeywordID, m.Keyword
        FROM KeywordsMaster m
        WHERE NOT EXISTS (
            SELECT 1 
            FROM KeywordsLog l 
            WHERE l.KeywordID = m.KeywordID 
              AND l.Status = 'Success'
        )
    """
    cursor.execute(query_missing)
    missing_keywords = cursor.fetchall()
    
    if not missing_keywords:
        print("[INFO] 沒有需要補抓的關鍵字。")
        conn.close()
        return
        
    print(f"[INFO] 發現 {len(missing_keywords)} 筆關鍵字，開始多執行緒抓取...")
    scraped_results = []
    success_count = 0
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_keyword = {executor.submit(process_single_keyword, kid, kw): (kid, kw) for kid, kw in missing_keywords}
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_keyword), 1):
                kid, kw = future_to_keyword[future]
                try:
                    result = future.result()
                    scraped_results.append(result)
                    if result[2] == "Success":
                        success_count += 1
                except Exception as exc:
                    print(f"  [系統錯誤] '{kw}' 執行緒崩潰: {exc}")
                    scraped_results.append((kid, "", "Fail", str(exc)))
                
                print(f"[進度] {i}/{len(missing_keywords)} 處理完成。")
                
                # 任務間隔，降低被封鎖的機率
                time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("\n[INFO] 強制中斷，準備寫入已抓取資料...")
    finally:
        # 強制清理所有殘留暫存資料夾
        for temp_dir in glob.glob("/tmp/Chrome_Temp_*"):
            try: shutil.rmtree(temp_dir, ignore_errors=True)
            except: pass

        if scraped_results:
            print(f"\n[INFO] 批次寫入 {len(scraped_results)} 筆資料...")
            try:
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("SELECT ISNULL(MAX(LogID), 0) FROM KeywordsLog WITH (UPDLOCK, SERIALIZABLE)")
                current_max_id = int(cursor.fetchone()[0])
                
                insert_data_list = []
                for res in scraped_results:
                    kid, summary, status, error_msg = res
                    current_max_id += 1 
                    insert_data_list.append((current_max_id, kid, summary, status, error_msg))
                
                sql_insert = """
                    INSERT INTO KeywordsLog 
                    (LogID, KeywordID, LogDate, CrawlTime, SummaryText, Status, ErrorMessage, CreatedAt) 
                    VALUES (?, ?, CAST(GETDATE() AS DATE), GETDATE(), ?, ?, ?, GETDATE())
                """
                cursor.executemany(sql_insert, insert_data_list)
                cursor.execute("COMMIT TRANSACTION")
                conn.commit()
                print("[INFO] 寫入成功！")
            except Exception as db_err:
                print(f"[錯誤] 寫入失敗 Rollback: {db_err}")
                cursor.execute("ROLLBACK TRANSACTION")
                conn.rollback()
        
        print(f"\n{'=' * 60}")
        print(f"作業完成！成功補抓: {success_count} 筆資料")
        print(f"{'=' * 60}")
        conn.close()

if __name__ == "__main__":
    main()