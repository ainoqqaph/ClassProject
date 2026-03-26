import time
import random
import pyodbc
import os
import sys
import io
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 確保 stdout/stderr 強制使用 UTF-8
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ==================== 資料庫與瀏覽器配置 ====================
SQL_SERVER = 'localhost'
SQL_DATABASE = 'MicrosoftRDB'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DRIVER_PATH = os.path.join(BASE_DIR, "msedgedriver.exe")

def init_driver():
    """初始化 Edge WebDriver (啟用純淨訪客/無痕模式)"""
    print("[INFO] 啟動 Edge 瀏覽器 (Guest/InPrivate 模式)...")
    options = webdriver.EdgeOptions()
    
    # 基本視窗設定
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("-inprivate") 
    options.add_argument("--guest")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")

    # 關閉自動化測試的提示橫幅
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # 建立暫時的乾淨 Profile 資料夾
    custom_profile = os.path.join(BASE_DIR, f"Edge_Profile_Temp_{int(time.time())}")
    os.makedirs(custom_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={custom_profile}")
    
    service = Service(executable_path=DRIVER_PATH)
    driver = webdriver.Edge(service=service, options=options)
    
    # 抹除 WebDriver 特徵
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver, custom_profile

def search_and_extract_summary(driver, keyword):
    """執行 Bing 搜尋並精準擷取第一筆摘要"""
    try:
        # 1. 進入 Bing 並搜尋
        driver.get("https://www.bing.com")
        time.sleep(random.uniform(1.0, 2.0))
        search_box = driver.find_element(By.NAME, "q")
        
        # 模擬真人打字
        for ch in keyword:
            search_box.send_keys(ch)
            time.sleep(random.uniform(0.02, 0.08))
        search_box.send_keys(Keys.ENTER)
        
        # 模擬真人看網頁
        time.sleep(random.uniform(1.5, 2.5))
        driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
        
        # 2. 等待搜尋結果出來
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "b_results"))
        )
        
        # 3. 精準擷取摘要
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
            
    except Exception as e:
        print(f"  [警告] 搜尋 '{keyword}' 時發生錯誤: {e}")
        return ""

def main():
    print("=" * 60)
    print("啟動資料庫補抓程式 (Backfill Script - 批次效能版)")
    print("=" * 60)
    
    # 1. 連線資料庫
    try:
        conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes"
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        print("[INFO] 資料庫連線成功")
    except Exception as e:
        print(f"[錯誤] 資料庫連線失敗: {e}")
        return

    # 2. 尋找「沒有在 KeywordsLog 裡」的關鍵字
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
    
    # 3. 啟動瀏覽器
    driver, profile_path = init_driver()
    
    scraped_results = []
    success_count = 0
    
    try:
        # 4. 開始補抓迴圈 (僅執行耗時的網路請求，不鎖定資料庫)
        for idx, (kid, kw) in enumerate(missing_keywords, 1):
            print(f"[{idx}/{len(missing_keywords)}] 正在搜尋: '{kw}'...")
            
            summary = search_and_extract_summary(driver, kw)
            status = "Success" if summary else "Fail"
            error_msg = None if summary else "無法擷取有效摘要"
            
            if summary:
                print(f"  ✓ 成功抓取摘要: {summary[:30]}...")
                success_count += 1
            else:
                print(f"  ✗ 抓取失敗")
            
            # 將結果暫存到記憶體中
            scraped_results.append((kid, summary, status, error_msg))
            time.sleep(random.uniform(5, 10))
            
    except KeyboardInterrupt:
        print("\n[INFO] 使用者強制中斷程式，準備寫入已抓取的資料...")
    finally:
        driver.quit()
        
        if profile_path and os.path.exists(profile_path):
            print(f"[INFO] 正在清理暫存 Profile: {profile_path}")
            try:
                # 稍微等待 2 秒，確保 Windows 徹底釋放 Edge 的背景檔案鎖
                time.sleep(2) 
                # ignore_errors=True 可以防止某些殘留檔案鎖導致程式崩潰
                shutil.rmtree(profile_path, ignore_errors=True) 
                print("[INFO] ✓ 暫存 Profile 清理完成")
            except Exception as e:
                print(f"[警告] 清理 Profile 發生例外: {e}")

        # 5. 批次寫入與安全取號 (極短時間鎖定，解決 Deadlock)
        if scraped_results:
            print(f"\n[INFO] 開始批次寫入 {len(scraped_results)} 筆資料到資料庫...")
            try:
                # 明確開啟交易
                cursor.execute("BEGIN TRANSACTION")
                
                # 面試亮點：精準鎖定並查出目前的 MAX(ID)
                # UPDLOCK 防止死結，SERIALIZABLE 防止幻讀 (確保取號的絕對安全)
                cursor.execute("SELECT ISNULL(MAX(LogID), 0) FROM KeywordsLog WITH (UPDLOCK, SERIALIZABLE)")
                current_max_id = int(cursor.fetchone()[0])
                
                insert_data_list = []
                for result in scraped_results:
                    kid, summary, status, error_msg = result
                    current_max_id += 1 # 在記憶體中遞增，不碰資料庫鎖
                    insert_data_list.append((current_max_id, kid, summary, status, error_msg))
                
                # 使用 executemany 一次性打進資料庫 (效能提升百倍)
                sql_insert = """
                    INSERT INTO KeywordsLog 
                    (LogID, KeywordID, LogDate, CrawlTime, SummaryText, Status, ErrorMessage, CreatedAt) 
                    VALUES (?, ?, CAST(GETDATE() AS DATE), GETDATE(), ?, ?, ?, GETDATE())
                """
                cursor.executemany(sql_insert, insert_data_list)
                
                # 提交交易，釋放鎖定
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