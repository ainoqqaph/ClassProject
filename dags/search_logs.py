# -*- coding: utf-8 -*-
# !/usr/bin/env python

import time
import random
import pyodbc
import os
import sys
import shutil
import urllib.parse
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

# 確保 stdout/stderr 強制使用 UTF-8
if hasattr(sys.stdout, 'buffer'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ==================== 資料庫與瀏覽器配置 ====================
SQL_SERVER = 'host.docker.internal' # Docker 專用
SQL_DATABASE = 'MicrosoftRDB'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DRIVER_PATH = "/usr/bin/chromedriver" # Linux 版 ChromeDriver

def init_driver():
    """初始化 Chrome WebDriver (極輕量 Headless 模式)"""
    options = webdriver.ChromeOptions()
    
    # 基礎防崩潰與偽裝參數
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage")  
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--log-level=3")
    
    # 🌟 瘦身核心 1：關閉所有不必要的功能，減輕 Docker 記憶體負擔
    options.add_argument("--disable-extensions") # 禁用擴充功能
    options.add_argument("--disable-software-rasterizer") 
    options.add_argument("--disable-features=NetworkService")
    
    # 🌟 瘦身核心 2：【禁止載入圖片】與通知，節省 80% 記憶體與網路頻寬
    prefs = {
        "profile.managed_default_content_settings.images": 2, # 2 代表拒絕載入圖片
        "profile.default_content_setting_values.notifications": 2 # 關閉通知
    }
    options.add_experimental_option("prefs", prefs)
    
    # 建立隨機暫存 Profile 資料夾
    custom_profile = f"/tmp/Chrome_Temp_{int(time.time())}_{random.randint(100,999)}"
    os.makedirs(custom_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={custom_profile}")
    
    service = Service(executable_path=DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    
    # 抹除 WebDriver 特徵
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver, custom_profile

def search_and_extract_summary(driver, keyword):
    """執行 Bing 搜尋並精準擷取第一筆摘要"""
    # 🌟 關鍵修復：改用 URL 直接搜尋，完全避開首頁 UI 干擾與輸入框報錯
    search_url = f"https://www.bing.com/search?q={urllib.parse.quote(keyword)}"
    driver.get(search_url)
    
    time.sleep(random.uniform(1.5, 2.5))
    driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
    
    # 等待搜尋結果出來
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
    print("啟動資料庫補抓程式 (終極防崩潰版)")
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
                    print(f"  ✓ 成功抓取: {summary[:30]}...")
                    success_count += 1
                else:
                    print(f"  ✗ 抓取失敗 (未找到內容)")
                    
            except WebDriverException as wd_err:
                # 🌟 關鍵修復：如果瀏覽器崩潰，觸發復活機制
                print(f"  [嚴重警告] 瀏覽器崩潰，啟動復活機制... ({str(wd_err)[:50]}...)")
                error_msg = "瀏覽器崩潰重啟"
                status = "Fail"
                summary = ""
                
                # 關掉壞掉的，重開一個新的
                try: driver.quit()
                except: pass
                time.sleep(2)
                driver, profile_path = init_driver()
                
            except Exception as e:
                print(f"  [警告] 發生例外錯誤: {e}")
                status = "Fail"
                summary = ""
                error_msg = str(e)
            
            scraped_results.append((kid, summary, status, error_msg))
            time.sleep(random.uniform(4, 7))
            
    except KeyboardInterrupt:
        print("\n[INFO] 使用者強制中斷程式，準備寫入已抓取的資料...")
    finally:
        try: driver.quit()
        except: pass
        
        if profile_path and os.path.exists(profile_path):
            try:
                time.sleep(2) 
                shutil.rmtree(profile_path, ignore_errors=True) 
                print("[INFO] ✓ 暫存 Profile 清理完成")
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