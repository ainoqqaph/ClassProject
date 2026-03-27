# -*- coding: utf-8 -*-
# !/usr/bin/env python

import time
import random
import pyodbc
import os
import sys
import shutil
import urllib.parse
import psutil
import threading
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

# ==================== 日誌 (Logging) 設定 ====================
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

# 攔截 print 輸出到檔案
sys.stdout = DualLogger(sys.stdout, LOG_FILE)
sys.stderr = DualLogger(sys.stderr, LOG_FILE)

# ==================== 資料庫與瀏覽器配置 ====================
SQL_SERVER = 'host.docker.internal' 
SQL_DATABASE = 'MicrosoftRDB'
DRIVER_PATH = "/usr/bin/chromedriver"

def force_cleanup_browser_processes():
    """強制清理系統中殘留的 Chrome 與 WebDriver 處理程序"""
    try:
        killed = 0
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                process_name = proc.info['name'].lower()
                if process_name in ['chrome', 'chromedriver', 'chromium']:
                    proc.kill()
                    killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        if killed > 0:
            print(f"[系統] 成功清理 {killed} 個殘留的瀏覽器殭屍行程。")
    except Exception:
        pass

def init_driver():
    """初始化 Chrome WebDriver"""
    options = webdriver.ChromeOptions()
    
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-extensions") 
    options.add_argument("--disable-software-rasterizer") 
    options.add_argument("--disable-features=NetworkService")
    options.add_argument("--disable-webgl")
    options.add_argument("--disable-3d-apis")
    options.add_argument("--disable-features=WebGL")
    options.page_load_strategy = 'eager'

    prefs = {
        "profile.managed_default_content_settings.images": 2, 
        "profile.default_content_setting_values.notifications": 2 
    }
    options.add_experimental_option("prefs", prefs)
    
    custom_profile = f"/tmp/Chrome_Temp_{int(time.time())}_{random.randint(100,999)}"
    os.makedirs(custom_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={custom_profile}")
    
    service = Service(executable_path=DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)

    driver.set_page_load_timeout(15) 
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver, custom_profile

def search_and_extract_summary(driver, keyword):
    """執行 Bing 搜尋並精準擷取第一筆摘要"""
    search_url = f"https://www.bing.com/search?q={urllib.parse.quote(keyword)}"
    driver.get(search_url)
    
    time.sleep(random.uniform(1.5, 2.5))
    driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "b_results"))
    )
    
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
        return summary_text[:150] + "..." 
    else:
        return ""

def main():
    print("=" * 60)
    print("啟動資料庫補抓程式")
    print("=" * 60)
    
    # 程式啟動前先大掃除一次
    force_cleanup_browser_processes()

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
        print(f"[INFO] 資料庫連線成功 (User: {db_user})")
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
        print("[INFO] 資料庫目前很完整，沒有需要補抓的關鍵字！")
        conn.close()
        return
        
    print(f"[INFO] 總共發現 {len(missing_keywords)} 筆需要補抓的關鍵字。準備開始作業...\n")
    
    print("[INFO] 啟動 Chrome 瀏覽器 (Headless 模式)...")
    driver, profile_path = init_driver()
    scraped_results = []
    success_count = 0
    
    try:
        for idx, (kid, kw) in enumerate(missing_keywords, 1):
            print(f"[{idx}/{len(missing_keywords)}] 正在搜尋: '{kw}'...")
            
            try:
                summary = search_and_extract_summary(driver, kw)
                status = "Success" if summary else "Fail"
                error_msg = None if summary else "無法擷取有效摘要"
                
                if summary:
                    print(f"  成功抓取: {summary[:30]}...")
                    success_count += 1
                else:
                    print(f"  抓取失敗 (未找到內容)")
                    
            except WebDriverException as wd_err:
                print(f"  [嚴重警告] 瀏覽器崩潰，啟動復活機制... ({str(wd_err)[:50]}...)")
                error_msg = "瀏覽器崩潰重啟"
                status = "Fail"
                summary = ""
                
                # 1. 關閉壞掉的 session
                try: driver.quit()
                except: pass
                
                # 2. 殺死記憶體中的殭屍行程
                force_cleanup_browser_processes()
                
                # 3. 刪除舊的 Profile 資料夾 (釋放硬碟空間)
                if profile_path and os.path.exists(profile_path):
                    try: shutil.rmtree(profile_path, ignore_errors=True)
                    except: pass
                
                time.sleep(3)
                # 重新啟動
                driver, profile_path = init_driver()
                
            except Exception as e:
                print(f"  [警告] 發生例外錯誤: {e}")
                status = "Fail"
                summary = ""
                error_msg = str(e)
            
            scraped_results.append((kid, summary, status, error_msg))
            
            # 預防性重啟：每 15 次主動大掃除，預防記憶體緩慢洩漏 (Memory Leak)
            if idx % 15 == 0 and idx < len(missing_keywords):
                print("  [系統] 預防性重啟瀏覽器，釋放記憶體...")
                try: driver.quit()
                except: pass
                force_cleanup_browser_processes()
                if profile_path and os.path.exists(profile_path):
                    try: shutil.rmtree(profile_path, ignore_errors=True)
                    except: pass
                time.sleep(2)
                driver, profile_path = init_driver()
            else:
                time.sleep(random.uniform(4, 7))
            
    except KeyboardInterrupt:
        print("\n[INFO] 使用者強制中斷程式，準備寫入已抓取的資料...")
    finally:
        try: driver.quit()
        except: pass
        
        force_cleanup_browser_processes()
        
        if profile_path and os.path.exists(profile_path):
            try:
                time.sleep(2) 
                shutil.rmtree(profile_path, ignore_errors=True) 
                print("[INFO] 暫存 Profile 清理完成")
            except: pass

        if scraped_results:
            print(f"\n[INFO] 開始批次寫入 {len(scraped_results)} 筆資料到資料庫...")
            try:
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("SELECT ISNULL(MAX(LogID), 0) FROM KeywordsLog WITH (UPDLOCK, SERIALIZABLE)")
                current_max_id = int(cursor.fetchone()[0])
                
                insert_data_list = []
                for result in scraped_results:
                    kid, summary, status, error_msg = result
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
                print("[INFO] 批次寫入成功！")
            except Exception as db_err:
                print(f"[錯誤] 批次寫入資料庫失敗，觸發 Rollback: {db_err}")
                cursor.execute("ROLLBACK TRANSACTION")
                conn.rollback()
        
        print(f"\n{'=' * 60}")
        print(f"作業完成！成功補抓: {success_count}/{len(missing_keywords)} 筆資料")
        print(f"{'=' * 60}")
        conn.close()

if __name__ == "__main__":
    main()