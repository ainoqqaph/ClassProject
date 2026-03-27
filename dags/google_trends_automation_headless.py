# -*- coding: utf-8 -*-
# !/usr/bin/env python

import json
import argparse
import time
import random
import pyodbc
import threading
import re
import traceback
import sys
import io
import os
import requests
import logging
import concurrent.futures
import psutil
import shutil
from dotenv import load_dotenv
from datetime import datetime
from collections import Counter, defaultdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
# 確保 stdout/stderr 強制使用 UTF-8
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ==================== 1. 日誌 (Logging) 與 檔案設定 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RULES_FILE = os.path.join(BASE_DIR, "dynamic_category_rules.json")
DOTENV_PATH = os.path.join(BASE_DIR, ".env")
LOG_DIR = os.path.join(BASE_DIR, "logs") 

if not os.path.exists(LOG_DIR): 
    os.makedirs(LOG_DIR)

time_str = datetime.now().strftime("%Y-%m-%d_%A_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"Keyword_parser_{time_str}.log")


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

# 攔截並替換系統的標準輸出 (stdout) 與錯誤輸出 (stderr)
sys.stdout = DualLogger(sys.stdout, LOG_FILE)
sys.stderr = DualLogger(sys.stderr, LOG_FILE)

log_formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('Keyword_parser')  
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

def debug_print(message, level="INFO"):
    if level == "DEBUG": logger.debug(message)
    elif level == "WARNING": logger.warning(message)
    elif level == "ERROR": logger.error(message)
    else: logger.info(message)

# ==================== 2. 環境變數、金鑰與資料庫配置 ====================
load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))
load_dotenv(dotenv_path=DOTENV_PATH)
api_keys_raw = os.getenv("GEMINI_API_KEYS", "").strip().replace('"', '').replace("'", "")
VERTEX_AI_KEY = os.getenv("VERTEX_AI_KEY", "").strip().replace('"', '').replace("'", "")
GEMINI_API_KEYS = api_keys_raw.split(',')
CURRENT_API_KEY_INDEX = 0
GEMINI_AVAILABLE = True

SQL_SERVER, SQL_DATABASE = 'host.docker.internal', 'MicrosoftRDB'
DRIVER_PATH = "/usr/bin/chromedriver"
RULES_FILE = os.path.join(BASE_DIR, "dynamic_category_rules.json")

# ==================== 3. 爬蟲與 AI 分類設定 ====================


# Google Trends 地區設定
REGIONS = [
    {"code": "US", "name": "美國", "url": "https://trends.google.com.tw/trending?geo=US"},
    {"code": "AU", "name": "澳洲", "url": "https://trends.google.com.tw/trending?geo=AU"},
    {"code": "ES", "name": "西班牙", "url": "https://trends.google.com.tw/trending?geo=ES"},
    {"code": "GB", "name": "英國", "url": "https://trends.google.com.tw/trending?geo=GB"},
    {"code": "HK", "name": "香港", "url": "https://trends.google.com.tw/trending?geo=HK"},
    {"code": "TW", "name": "台灣", "url": "https://trends.google.com.tw/trending?geo=TW"}
]

KEYWORDS_PER_REGION = 25
TOP_ENGLISH_KEYWORDS = 0
TOP_CHINESE_KEYWORDS = 0
PER_KEYWORD_MIN = 10
PER_KEYWORD_MAX = 15
AFTER_KEYWORD_MIN = 10
AFTER_KEYWORD_MAX = 15
MAX_RETRIES = 3
INITIAL_BACKOFF = 2

SCRAPE_MAX_RETRIES = 2  # 每個地區最多重試次數
SCRAPE_RETRY_DELAY = 10  # 重試間隔（秒）
PAGE_LOAD_TIMEOUT = 30  # 頁面載入超時（秒）
ELEMENT_WAIT_TIME = 8  # 元素等待時間（秒）
DEBUG_MODE = True  # 啟用 DEBUG 模式

# AI 處理設定
USE_AI_CLASSIFICATION = True  # 是否啟用 AI 分類
AI_BATCH_SIZE = 50            # 批次交給 AI 處理的數量上限

# 規則式分類設定
# ==================== 動態規則載入模組 ====================


def load_category_rules():
    """從 JSON 檔案動態載入分類規則，若無則自動建立初始基礎版"""
    if not os.path.exists(RULES_FILE):
        # 精簡版的基礎規則，後續將由 Aikeyword.py 自動學習並擴充至 JSON 內
        initial_rules = {
            'Technology': ['AI', 'iPhone', 'Android', '科技', '電腦', '手機', '軟體', '硬體'],
            'Entertainment': ['電影', '音樂', '遊戲', '動漫', 'movie', 'music', 'game'],
            'Politics': ['政治', '選舉', '總統', 'election', 'president'],
            'News': ['新聞', '快訊', '地震', '颱風', 'news', 'breaking', 'weather'],
            'Sports': ['足球', '籃球', '棒球', 'NBA', 'MLB', '奧運', 'sports', 'vs', '對'],
            'Business': ['股票', '投資', '經濟', '股價', 'stock', 'business', 'ETF'],
            'Health': ['健康', '醫療', '疫情', 'health', '醫院'],
            'Education': ['教育', '學校', '考試', '大學'],
            'Lifestyle': ['美食', '旅遊', '機票', '時尚', 'food', 'travel'],
            'Other': []
        }
        with open(RULES_FILE, 'w', encoding='utf-8') as f:
            json.dump(initial_rules, f, ensure_ascii=False, indent=4)
        return initial_rules
    
    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

# 載入動態規則
CATEGORY_RULES = load_category_rules()

SEARCH_INTENT_RULES = {
    'Transactional': [
        '購買', '買', '價格', '優惠', '折扣', '訂購', '下單',
        'buy', 'purchase', 'price', 'deal', 'discount', 'order',
        '哪裡買', '多少錢', '便宜', '比價', 'sale',
        '特價', '促銷', '團購', '代購', 'shop', 'shopping',
        '門票', 'ticket', '預購', 'preorder', '訂房', '機票', 
        'flight', 'booking', '訂位', '索票', '售票'
    ],
    'Commercial': [
        '推薦', '評價', 'review', 'best', 'top', '比較', 
        '心得', '評比', '排行', '開箱', 'unboxing', '值得買'
    ],
    'Navigational': [
        '官網', '網站', '登入', 'login', 'sign in', 'download',
        '下載', 'app', '安裝', 'install', '註冊', 'register',
        'website', 'site', 'homepage', '首頁', 'portal',
        '客服', '門市', 'near me', '附近', '地址', 'location', '營業時間'
    ]
}


# ==================== 工具函式 ====================

def retry_with_backoff(func, max_retries=MAX_RETRIES, initial_backoff=INITIAL_BACKOFF):
    """簡單的重試 + 指數退避（含 jitter）"""
    t = initial_backoff
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                raise
            sleep_time = t + random.uniform(0, 1)
            debug_print(f"重試 {attempt}/{max_retries}，等待 {sleep_time:.1f} 秒... 錯誤: {e}", "WARNING")
            time.sleep(sleep_time)
            t *= 2
def log_error(filename, message):
    """統一的錯誤日誌寫入函式 (支援動態日期與集中管理)"""
    try:
        # 1. 確保 logs 資料夾存在
        log_dir = os.path.join(BASE_DIR, "logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # 2. 取得當天日期
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # 3. 分離原始檔名與副檔名 (例如把 "fatal_errors.log" 變成 "fatal_errors_2026-03-21.log")
        name, ext = os.path.splitext(filename)
        daily_filename = f"{name}_{today_str}{ext}"
        
        # 4. 組合成完整路徑
        full_path = os.path.join(log_dir, daily_filename)
        
        with open(full_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {message}\n")
    except Exception as e:
        debug_print(f"寫入日誌失敗 {filename}: {e}", "ERROR")



def convert_search_volume_to_number(volume_text):
    """將搜尋量文字轉換為數值"""
    if not volume_text:
        return None

    try:
        text = volume_text.strip().replace('+', '').replace(',', '')

        if '萬' in text:
            number_str = text.replace('萬', '').strip()
            number = float(number_str)
            return int(number * 10000)
        elif '千' in text:
            number_str = text.replace('千', '').strip()
            number = float(number_str)
            return int(number * 1000)
        elif '百' in text:
            number_str = text.replace('百', '').strip()
            number = float(number_str)
            return int(number * 100)
        elif 'M' in text.upper():
            number_str = re.sub(r'[Mm]', '', text).strip()
            number = float(number_str)
            return int(number * 1000000)
        elif 'K' in text.upper():
            number_str = re.sub(r'[Kk]', '', text).strip()
            number = float(number_str)
            return int(number * 1000)
        else:
            return int(float(text))

    except Exception as e:
        debug_print(f"轉換搜尋量失敗 '{volume_text}': {e}", "WARNING")
        return None

def force_cleanup_browser_processes():
    """【Airflow 專用】強制清理系統中殘留的 Edge 與 WebDriver 處理程序"""
    print(f"\n{'=' * 60}")
    print("[INFO] 執行系統級清理：尋找並終止殘留的 Edge 處理程序...")
    killed_count = 0
    try:
        # 掃描所有執行中的程序
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                process_name = proc.info['name'].lower()
                # 鎖定 Edge 瀏覽器與驅動程式
                if process_name in ['chrome', 'chromedriver', 'chromium']:
                    proc.kill()  # 無情擊殺
                    killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
                
        if killed_count > 0:
            print(f"[INFO] 成功清理 {killed_count} 個殘留的殭屍程序。")
        else:
            print("[INFO] 系統環境乾淨，無殘留程序。")
    except Exception as e:
        print(f"[WARNING] 清理程序時發生異常 (可忽略): {e}")
    print(f"{'=' * 60}\n")

# ==================== 分類函式 ====================

def classify_keyword_by_rules(keyword):
    """規則式分類 (AI 失敗時的 Fallback 安全氣囊)"""
    # 將關鍵字轉小寫，方便英文比對
    kw_lower = keyword.lower()
 
    # 1. 判斷搜尋意圖 (Search Intent)
   
    intent = "Informational" 
    
    # 交易型
    if any(word in kw_lower for word in ['buy', 'price', 'cheap', 'discount', 'order', '買', '價格', '便宜', '折扣', '預購', '特價', '哪裡買']):
        intent = "Transactional"
    # 商業調查型
    elif any(word in kw_lower for word in ['best', 'review', 'vs', 'compare', 'top', '推薦', '評價', '比較', '排行', '開箱']):
        intent = "Commercial"
    # 導航型
    elif any(word in kw_lower for word in ['login', 'official', 'www', '官網', '登入', '客服', '下載']):
        intent = "Navigational"

    # 2. 判斷產業分類 (Category)
    
    category = "Other"  
    
    # 商業與金融 (含金量最高)
    if any(word in kw_lower for word in ['stock', 'market', 'bank', 'finance', '股票', '股市', '銀行', '投資', '匯率', '台積電', 'ETF']):
        category = "Business"
    # 科技與3C
    elif any(word in kw_lower for word in ['iphone', 'apple', 'app', 'software', 'tech', '手機', '蘋果', '軟體', '科技', 'ai', '微軟']):
        category = "Technology"
    # 體育賽事
    elif any(word in kw_lower for word in ['nba', 'mlb', 'score', 'match', '籃球', '棒球', '賽程', '比分', '奧運', '羽球', '直播']):
        category = "Sports"
    # 娛樂與影視
    elif any(word in kw_lower for word in ['movie', 'film', 'actor', 'concert', 'netflix', '電影', '演唱會', '韓劇', '明星', '影評', '動漫']):
        category = "Entertainment"
    # 新聞與民生
    elif any(word in kw_lower for word in ['weather', 'news', 'typhoon', '天氣', '新聞', '颱風', '地震', '停班', '停課']):
        category = "News"
    # 生活風格
    elif any(word in kw_lower for word in ['recipe', 'food', 'travel', 'hotel', '食譜', '旅遊', '飯店', '美食', '餐廳']):
        category = "Lifestyle"
  
    # 3. 回傳格式 
    
    return {
        'category': category,
        'search_intent': intent,
        'english_translation': keyword  
    }
def classify_keywords_batch(keywords_list):
    """分類關鍵字 (保留詳細指令與解決格式報錯)"""
    global CURRENT_API_KEY_INDEX
    if not keywords_list: return {}

    keywords_str = ", ".join([f'"{kw}"' for kw in keywords_list])
    
    prompt = f"""請將以下關鍵字分類到適當的 Category 和 SearchIntent。
【核心指令：絕對禁止翻譯中文】:
1. 絕對禁止翻譯「中文」關鍵字。中文關鍵字請保持原樣填入 'english_translation' 欄位。
2. 只有非英文的外語需要翻譯成英文並填入 'english_translation'。
3. 如果是英文，'english_translation' 直接填入原本的關鍵字。
4. 如果你不確定 SearchIntent，請預設填入 "Informational"。絕對不可以留空或回傳 null。

【分類嚴格定義與常見防呆範例】：
- Business (商業): 總體經濟、股市大盤與企業動態。包含公司名稱加股價/財報、股票代號、財經術語、股東會紀念品、ETF、金價、匯率。
- Sports (運動): 包含體育賽事、各國運動員、非美系球隊與中文譯名。注意：只要有 "vs" 、 "對" 、連字號對戰、大學名稱對抗，絕對屬於 Sports。
- News (新聞/時事): 突發事件、天災、天氣警報與社會案件。包含地震、颱風、暴風雪、氣象雷達、天氣查詢。
- Politics (政治): 各國大選、法案與政治人物。包含台灣政界人士。
- Entertainment (娛樂): 影視音樂、遊戲、八卦、演唱會、國內外知名藝人網紅，以及所有博弈/樂透/猜字遊戲。
- Education (教育): 包含升學考試與分析、天文奇觀與太空科學。
- Lifestyle (生活): 包含宗教節慶、民俗信仰、地標景點、促銷購物。
- Technology (科技): 包含 3C 產品、AI工具、軟硬體發布。警告：絕對不要把「大學名稱」或「服飾品牌」誤分到這裡。
- 如果真的完全無法歸類再選 Other (其他)。

SearchIntent 可選：
- Informational, Transactional, Navigational, Commercial

關鍵字：{keywords_str}
請回傳如下格式的 JSON 物件 (以關鍵字為鍵)：
{{
  "關鍵字1": {{"category": "...", "search_intent": "...", "english_translation": "..."}},
  "關鍵字2": {{"category": "...", "search_intent": "...", "english_translation": "..."}}
}}
只需回傳 JSON。"""

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}], 
        "generationConfig": {"temperature": 0.1}
    }
    headers = {"Content-Type": "application/json"}

    def safe_process_response(res):
        try:
            raw_text = res.json()['candidates'][0]['content']['parts'][0]['text']
            clean_json = raw_text.strip().replace('```json', '').replace('```', '').strip()
            ai_data = json.loads(clean_json)
            rebuilt_dict = {}
            if isinstance(ai_data, list):
                for item in ai_data:
                    # 強制轉字串並清理
                    k = str(item.get('keyword') or next(iter(item.values()))).strip()
                    rebuilt_dict[k] = item
            elif isinstance(ai_data, dict):
                for k, v in ai_data.items():
                    rebuilt_dict[str(k).strip()] = v
            return rebuilt_dict
        except Exception as e:
            debug_print(f"解析 AI JSON 失敗: {e}", "ERROR")
            return {}

    # --- 引擎 1：Vertex AI (2.5) ---
    if VERTEX_AI_KEY:
        try:
            url = f"https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash-lite:generateContent?key={VERTEX_AI_KEY}"
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                result = safe_process_response(res)
                if result: return result
        except: pass

    # --- 引擎 2：金鑰輪替 (2.0) ---
    for _ in range(len(GEMINI_API_KEYS) * 2):
        key = GEMINI_API_KEYS[CURRENT_API_KEY_INDEX]
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={key}"
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                result = safe_process_response(res)
                if result: return result
        except: pass
        CURRENT_API_KEY_INDEX = (CURRENT_API_KEY_INDEX + 1) % len(GEMINI_API_KEYS)
    
    return {kw: classify_keyword_by_rules(kw) for kw in keywords_list}
    

# ==================== 時間序列追蹤模組 ====================

class DailySnapshotManager:
    """管理每日趨勢快照"""

    def __init__(self, db_connection):
        self.conn = db_connection
        self.db_lock = threading.Lock()

    def create_daily_snapshots(self, snapshot_date=None):
        """建立每日快照（呼叫 SQL Server SP）"""
        try:
            if snapshot_date is None:
                snapshot_date = datetime.now().date()

            print(f"\n{'=' * 60}")
            print(f"建立每日快照: {snapshot_date}")
            print(f"{'=' * 60}")

            with self.db_lock:
                cursor = self.conn.cursor()

                cursor.execute(
                    "EXEC dbo.usp_InsertDailySnapshots @SnapshotDate = ?",
                    snapshot_date
                )

                self.conn.commit()

                print(f" 每日快照建立成功")
                return True

        except Exception as e:
            print(f" 建立快照失敗: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False

    def print_daily_report(self, date=None):
        """列印每日趨勢報告"""
        if date is None:
            date = datetime.now().date()

        print(f"\n{'=' * 60}")
        print(f"每日趨勢報告 - {date}")
        print(f"{'=' * 60}\n")


# ==================== 數據質量監控模組====================

class DataQualityMonitor:
    """數據質量監控器：處理執行紀錄、狀態更新與品質檢查"""

    def __init__(self, db_connection):
        self.conn = db_connection
        self.current_execution_id = None
    
    def start_execution(self, crawler_version="v1.1.0", python_version=None):
        """
        開始爬蟲執行
        解決原因：SP 內部的 INSERT 訊息會干擾 pyodbc 讀取 SELECT 結果集 
        """
        try:
            if python_version is None:
                python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

            print("\n" + "=" * 60)
            print("開始爬蟲執行監控")
            print("=" * 60)

            cursor = self.conn.cursor()

            # 加入 SET NOCOUNT ON 防止「受影響資料列」訊息干擾結果集讀取
            sql_command = """
                SET NOCOUNT ON;
                DECLARE @NewID INT;
                EXEC dbo.usp_StartCrawlerExecution 
                    @CrawlerVersion = ?, 
                    @PythonVersion = ?, 
                    @ExecutionID = @NewID OUTPUT;
                SELECT @NewID;
            """
            
            cursor.execute(sql_command, crawler_version, python_version)
            
            # 迴圈切換結果集，直到找到包含 @NewID 的查詢結果
            self.current_execution_id = None
            while True:
                try:
                    row = cursor.fetchone()
                    if row:
                        self.current_execution_id = row[0]
                        break
                except pyodbc.Error:
                    # 如果當前結果集不是查詢（例如 PRINT 訊息），跳過
                    pass
                
                if not cursor.nextset():
                    break
            
            self.conn.commit()

            if self.current_execution_id:
                print(f" 執行 ID: {self.current_execution_id}")
            else:
                print(" 警告: 成功執行但未能抓取到 ExecutionID")
                
            return self.current_execution_id

        except Exception as e:
            print(f" 開始執行記錄失敗: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return None

    def update_execution(self, status=None, regions_scraped=None,
                         total_keywords=None, new_keywords=None,
                         keywords_searched=None, error_message=None):
        """更新爬蟲執行狀態"""
        if self.current_execution_id is None:
            return

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                EXEC dbo.usp_UpdateCrawlerExecution 
                    @ExecutionID = ?,
                    @Status = ?,
                    @RegionsScraped = ?,
                    @TotalKeywordsFound = ?,
                    @NewKeywordsAdded = ?,
                    @KeywordsSearched = ?,
                    @ErrorMessage = ?
                """,
                self.current_execution_id,
                status,
                regions_scraped,
                total_keywords,
                new_keywords,
                keywords_searched,
                error_message
            )
            self.conn.commit()

            if status:
                print(f" 執行狀態已更新: {status}")

        except Exception as e:
            print(f" 更新執行狀態失敗: {e}")
            try:
                self.conn.rollback()
            except:
                pass

    def check_quality(self, check_date=None, region_id=None, enable_alert=True):
        """呼叫 SQL Server 品質檢查與擷取告警訊息"""
        try:
            cursor = self.conn.cursor()

            print("\n" + "=" * 60)
            print("數據質量檢查")
            if check_date:
                print(f"日期: {check_date}")
            print("=" * 60)

            # 增強版品質檢查
            try:
                cursor.execute("""
                    SET NOCOUNT ON;
                    EXEC dbo.usp_CheckDataQuality_Enhanced
                        @CheckDate = ?,
                        @RegionID = ?,
                        @EnableAlert = ?
                """, check_date, region_id, 1 if enable_alert else 0)
            except Exception as e:
                # Fallback 到基礎版
                print(f" [DEBUG] 增強版 SP 失敗，嘗試基礎版... 錯誤: {e}")
                cursor.execute("""
                    SET NOCOUNT ON;
                    EXEC dbo.usp_CheckDataQuality
                        @CheckDate = ?,
                        @RegionID = ?
                """, check_date, region_id)

            # 取得品質結果
            row = cursor.fetchone()
            result = None

            if row:
                result = {
                    'quality_id': row[0] if len(row) > 0 else None,
                    'check_date': row[1] if len(row) > 1 else None,
                    'status': row[2] if len(row) > 2 else None,
                    'total_keywords': row[3] if len(row) > 3 else 0,
                    'keywords_with_volume': row[4] if len(row) > 4 else 0,
                    'keywords_with_rank': row[5] if len(row) > 5 else 0,
                    'new_keywords': row[6] if len(row) > 6 else 0,
                    'volume_completeness': float(row[7]) if len(row) > 7 and row[7] else 0,
                    'rank_completeness': float(row[8]) if len(row) > 8 and row[8] else 0,
                    'category_completeness': float(row[9]) if len(row) > 9 and row[9] else 0,
                    'error_count': row[10] if len(row) > 10 else 0,
                    'warning_count': row[11] if len(row) > 11 else 0,
                    'comments': row[12] if len(row) > 12 else ''
                }

                print(f"質量檢查完成")
                print(f"狀態: {result['status']}")
                print(f"總關鍵字: {result['total_keywords']}")
                print(f"完整性: 搜尋量 {result['volume_completeness']:.1f}%, 排名 {result['rank_completeness']:.1f}%")

                # 擷取下一個結果集（告警訊息）
                try:
                    if cursor.nextset():
                        alerts = cursor.fetchall()
                        if alerts:
                            print(f"\n告警訊息:")
                            for alert_row in alerts:
                                print(f"  [{alert_row[1]}] {alert_row[2]}")
                            print("-" * 60)
                except:
                    pass

            while cursor.nextset():
                pass

            return result

        except Exception as e:
            print(f"品質檢查失敗: {e}")
            traceback.print_exc()
            return None


# ==================== 關聯詞分析模組 ====================

class KeywordRelationAnalyzer:
    """關鍵字關聯分析器"""

    def __init__(self, db_connection):
        self.conn = db_connection

    def calculate_co_occurrence(self, calculate_date=None):
        """計算關鍵字共現關係"""
        try:
            if calculate_date is None:
                calculate_date = datetime.now().date()

            print("\n" + "=" * 60)
            print("計算關鍵字共現關係")
            print(f"日期: {calculate_date}")
            print("=" * 60)

            cursor = self.conn.cursor()

            # 直接呼叫關聯詞分析
            cursor.execute("""
                EXEC dbo.usp_CalculateCoOccurrence
                    @CalculateDate = ?
            """, calculate_date)

            debug_print("使用共現計算 SP", "DEBUG")

            # 消耗所有結果集
            while cursor.nextset():
                pass

            self.conn.commit()

            print(f"共現關係計算完成")
            return True

        except Exception as e:
            print(f"計算共現關係失敗: {e}")
            traceback.print_exc()
            try:
                self.conn.rollback()
            except:
                pass
            return False

    def update_cooccurrence_scores(self):
        """更新關鍵字共現分數"""
        try:
            print("\n更新關鍵字共現分數...")

            cursor = self.conn.cursor()
            cursor.execute("EXEC dbo.usp_UpdateCoOccurrenceScores")

            while cursor.nextset():
                pass

            self.conn.commit()

            print(f"共現分數已更新")
            return True

        except Exception as e:
            print(f"✗ 更新共現分數失敗: {e}")
            traceback.print_exc()
            try:
                self.conn.rollback()
            except:
                pass
            return False
        
# ==================== Crawler 類別 ====================

class Crawler:
    def __init__(self):
        self.driver = None
        self.conn = None
        self.db_lock = threading.Lock()
        self.region_id_map = {}
        self.initial_window_handle = None
        self.profile_path = None
        self.snapshot_manager = None
        self.quality_monitor = None
        self.relation_analyzer = None

    def connect_db(self):
        """連接 SQL Server"""
        try:
            # 1. 從環境變數抓取帳號密碼，並賦值給變數
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")

            
            if not db_user or not db_password:
                raise ValueError("環境變數 DB_USER 或 DB_PASSWORD 未設定，請檢查 .env 檔案！")
            
            
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={db_user};PWD={db_password};" 
                "Encrypt=no;TrustServerCertificate=yes"
            )
            
            self.conn = pyodbc.connect(conn_str, autocommit=False)
            print(f"[INFO] Connected to SQL Server via SQL Auth (User: {db_user})")
            cursor = self.conn.cursor()
            cursor.execute("SELECT GETDATE()")
            current_time = cursor.fetchone()[0]
            print(f"[INFO] Database current time: {current_time}")

            self.load_region_ids()
            self.snapshot_manager = DailySnapshotManager(self.conn)
            print(f"[INFO] 時間序列模組已載入")
            self.quality_monitor = DataQualityMonitor(self.conn)
            print(f"[INFO] 數據質量監控模組已載入")
            self.relation_analyzer = KeywordRelationAnalyzer(self.conn)
            print(f"[INFO] 關聯詞分析模組已載入")

        except Exception as e:
            print(f"[FATAL ERROR] Failed to connect to SQL Server: {e}")
            raise

    def load_region_ids(self):
        """載入地區 ID 對應表"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT RegionID, RegionCode FROM RegionsMaster")
            for row in cursor.fetchall():
                self.region_id_map[row[1]] = row[0]
            print(f"[INFO] Loaded {len(self.region_id_map)} regions")
        except Exception as e:
            print(f"[ERROR] Failed to load region IDs: {e}")
            raise

    def init_driver(self):
        """初始化 Chromium WebDriver (Docker Linux 專用版)"""
        try:
            print(f"[INFO] Initializing Chromium WebDriver (Headless Mode)...")
            
            # 1. 指向 Dockerfile 安裝的 Linux 驅動
            driver_path = "/usr/bin/chromedriver"
            
            options = webdriver.ChromeOptions()  
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--no-sandbox") 
            options.add_argument("--disable-dev-shm-usage")  
            options.add_argument("--disable-gpu")
            options.add_argument("--remote-allow-origins=*")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            self.profile_path = f"/tmp/Chrome_Temp_{int(time.time())}"
            options.add_argument(f"--user-data-dir={self.profile_path}")

            service = Service(driver_path)
            
            # 4. 啟動
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            
            print(f"[INFO] Linux Chromium WebDriver initialized successfully")
            
        except Exception as e:
            print(f"[FATAL ERROR] Failed to initialize WebDriver: {e}")
            traceback.print_exc()
            sys.exit(1)

    def close(self):
        """關閉瀏覽器，清理處理程序"""
        driver_pid = None
        
        if self.driver:
            try:
                driver_pid = self.driver.service.process.pid
                print(f"[INFO] Closing browser (Driver PID: {driver_pid})...")
                
                self.driver.quit()
                print(f"[INFO] WebDriver session terminated elegantly.")
            except Exception as e:
                print(f"[ERROR] Error during normal quit(): {e}")
            finally:
                
                if driver_pid:
                    try:
                        parent = psutil.Process(driver_pid)
                        children = parent.children(recursive=True)
                        
                        for child in children:
                            try:
                                child.kill() 
                            except psutil.NoSuchProcess:
                                pass
                                
                        try:
                            parent.kill() 
                        except psutil.NoSuchProcess:
                            pass
                        print(f"[INFO] Zombie processes strictly cleaned up for PID {driver_pid}.")
                        
                    except psutil.NoSuchProcess:
                        pass
                    except Exception as clean_err:
                        print(f"[ERROR] Process cleanup error: {clean_err}")

        if self.profile_path and os.path.exists(self.profile_path):
            try:
                time.sleep(2)
                shutil.rmtree(self.profile_path, ignore_errors=True)
                print(f"[INFO] 暫存 Profile 清理完成 ({self.profile_path})")
            except Exception as clean_err:
                print(f"[ERROR] 清理 Profile 發生例外: {clean_err}")

        if self.conn:
            try:
                self.conn.close()
                print(f"[INFO] Database connection closed")
            except:
                pass

    def simulate_human(self):
        """模擬簡單人為行為"""
        time.sleep(random.uniform(1.0, 2.5))
        for _ in range(random.randint(1, 2)):
            try:
                self.driver.execute_script("window.scrollBy(0, {})".format(random.randint(100, 300)))
            except:
                pass
            time.sleep(random.uniform(0.3, 1.0))

    def extract_summary(self):
        """擷取 Bing 搜尋結果的第一筆摘要"""
        try:
            # 1. 顯式等待
            WebDriverWait(self.driver, 10).until(
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
                    elem = self.driver.find_element(By.XPATH, selector)
                    if elem and elem.text.strip():
                        summary_text = elem.text.strip()
                        break 
                except:
                    continue
                    
            if summary_text:
                
                return summary_text[:150] + "..." 
            else:
                return "無法擷取有效摘要"

        except Exception as e:
            debug_print(f"提取摘要失敗: {e}", "WARNING")
            return "無法擷取有效摘要"

    def get_next_id(self, table_name, id_column_name):
        """獲取下一個 ID"""
        cursor = self.conn.cursor()
        try:
            query = f"SELECT ISNULL(MAX({id_column_name}), 0) + 1 FROM {table_name} WITH (UPDLOCK, SERIALIZABLE)"
            cursor.execute(query)
            row = cursor.fetchone()
            next_id = int(row[0]) if row and row[0] else 1
            return next_id
        except Exception as e:
            debug_print(f"取得 next ID 失敗 {table_name}: {e}", "ERROR")
            log_error("database_errors.log", f"get_next_id error for {table_name}: {e}")
            raise
    
    def get_or_create_keywords_batch(self, keywords_data):
        if not keywords_data:
            return {}

        cursor = self.conn.cursor()
        keyword_id_map = {}
        
        try:
            # 1. 取得所有要處理的唯一關鍵字字串
            unique_keywords = list(set([k['keyword'] for k in keywords_data]))
            existing_keywords = {}
            
            if unique_keywords:
                chunk_size = 1000
                for i in range(0, len(unique_keywords), chunk_size):
                    chunk = unique_keywords[i:i + chunk_size]
                    placeholders = ','.join(['?'] * len(chunk))
                    query = f"SELECT Keyword, KeywordID FROM KeywordsMaster WHERE Keyword IN ({placeholders})"
                    cursor.execute(query, chunk)
                    for row in cursor.fetchall():
                        existing_keywords[row[0]] = int(row[1])
            cursor.execute("SELECT ISNULL(MAX(KeywordID), 0) FROM KeywordsMaster WITH (UPDLOCK, SERIALIZABLE)")
            current_max_id = int(cursor.fetchone()[0])
            
            insert_params = []
            update_params = []
            updated_kids = set()
            
            # 3. 記憶體中分配 ID
            for data in keywords_data:
                kw = data['keyword']
                cat = data.get('category', 'Other') or 'Other'
                intent = data.get('search_intent', 'Informational') or 'Informational'

                if kw in existing_keywords:
                    # 已存在：準備更新
                    kid = existing_keywords[kw]
                    keyword_id_map[kw] = kid
                    if kid not in updated_kids:
                        update_params.append((cat, intent, kid))
                        updated_kids.add(kid)
                else:
                    # 不存在：準備新增
                    if kw not in keyword_id_map:
                        current_max_id += 1
                        kid = current_max_id
                        keyword_id_map[kw] = kid
                        insert_params.append((kid, kw, cat, intent))
            
            # 4. 批次寫入與更新
            if insert_params:
                insert_query = "INSERT INTO KeywordsMaster (KeywordID, Keyword, Category, SearchIntent, CreatedAt) VALUES (?, ?, ?, ?, GETDATE())"
                cursor.executemany(insert_query, insert_params)
                
            if update_params:
                update_query = "UPDATE KeywordsMaster SET Category = ?, SearchIntent = ? WHERE KeywordID = ?"
                cursor.executemany(update_query, update_params)
                
            self.conn.commit()
            return keyword_id_map
            
        except Exception as e:
            debug_print(f"批次處理 Keywords 失敗: {e}", "ERROR")
            traceback.print_exc()
            try:
                self.conn.rollback()
            except:
                pass
            return {}

    def insert_region_stats_batch(self, stats_data):
        if not stats_data:
            return
            
        cursor = self.conn.cursor()
        try:
            # 取得目前的 MAX(StatsID)
            cursor.execute("SELECT ISNULL(MAX(StatsID), 0) FROM KeywordRegionStats WITH (UPDLOCK, SERIALIZABLE)")
            current_max_id = int(cursor.fetchone()[0])
            
            insert_query = """
                INSERT INTO KeywordRegionStats 
                (StatsID, KeywordID, RegionID, LogDate, SearchVolume, AppearanceCount, TrendRank, CreatedAt)
                VALUES (?, ?, ?, CAST(GETDATE() AS DATE), ?, ?, ?, GETDATE())
            """
            
            params = []
            for stat in stats_data:
                region_id = self.region_id_map.get(stat['region_code'])
                if region_id:
                    current_max_id += 1
                    params.append((
                        current_max_id,
                        stat['keyword_id'], 
                        region_id, 
                        stat['search_volume'], 
                        stat['appearance_count'], 
                        stat['trend_rank']
                    ))
            
            if params:
                cursor.executemany(insert_query, params)
                self.conn.commit()
                print(f"[INFO] 成功批次寫入 {len(params)} 筆地區統計資料")
                
        except Exception as e:
            debug_print(f"批次插入 KeywordRegionStats 失敗: {e}", "ERROR")
            traceback.print_exc() 
            try:
                self.conn.rollback()
            except:
                pass
    
    def insert_keywords_log(self, keyword_id, keyword_text, status, summary=None, error_msg=None):
        """插入 KeywordsLog（搜尋結果日誌）"""
        cursor = self.conn.cursor()
        try:
            next_log_id = self.get_next_id("KeywordsLog", "LogID")

            insert_query = """
                INSERT INTO KeywordsLog
                (LogID, KeywordID, LogDate, CrawlTime, SummaryText, Status, ErrorMessage, CreatedAt)
                VALUES (?, ?, CAST(GETDATE() AS DATE), GETDATE(), ?, ?, ?, GETDATE())
            """

            insert_params = (
                next_log_id,
                keyword_id,
                summary,
                status,
                error_msg
            )

            cursor.execute(insert_query, insert_params)
            self.conn.commit()

            print(f"[INFO] KeywordsLog inserted - LogID: {next_log_id}")
            return next_log_id

        except Exception as e:
            debug_print(f"插入 KeywordsLog 失敗: {e}", "ERROR")
            try:
                self.conn.rollback()
            except:
                pass
            log_error("keywords_log_errors.log", f"insert_keywords_log error: {e}")
            return None
    def insert_keywords_log_batch(self, scraped_results):
        """批次寫入搜尋日誌，避免迴圈內頻繁鎖死資料庫"""
        if not scraped_results: return
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("SELECT ISNULL(MAX(LogID), 0) FROM KeywordsLog WITH (UPDLOCK, SERIALIZABLE)")
            current_max_id = int(cursor.fetchone()[0])
            
            insert_data = []
            for kid, summary, status, error_msg in scraped_results:
                current_max_id += 1
                insert_data.append((current_max_id, kid, summary, status, error_msg))
            
            sql_insert = """
                INSERT INTO KeywordsLog 
                (LogID, KeywordID, LogDate, CrawlTime, SummaryText, Status, ErrorMessage, CreatedAt) 
                VALUES (?, ?, CAST(GETDATE() AS DATE), GETDATE(), ?, ?, ?, GETDATE())
            """
            cursor.executemany(sql_insert, insert_data)
            cursor.execute("COMMIT TRANSACTION")
            self.conn.commit()
            print(f"[INFO] ✓ 成功批次寫入 {len(insert_data)} 筆搜尋日誌資料")
        except Exception as e:
            cursor.execute("ROLLBACK TRANSACTION")
            self.conn.rollback()
            debug_print(f"批次寫入搜尋日誌失敗: {e}", "ERROR")

    def scrape_single_region(self, region_info):
        """從單一地區抓取關鍵字 (已修正為 debug_print 以確保紀錄入 log 檔)"""
        region_code = region_info["code"]
        region_name = region_info["name"]
        trends_url = region_info["url"]

        debug_print(f"\n{'=' * 60}")
        debug_print(f"[INFO] Scraping {region_code} ({region_name})")
        debug_print(f"[INFO] URL: {trends_url}")
        debug_print(f"{'=' * 60}")

        try:
            # 載入網頁
            debug_print(f"[DEBUG] Loading page...")
            retry_with_backoff(lambda: self.driver.get(trends_url))

            wait = WebDriverWait(self.driver, 20)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                debug_print(f"[DEBUG] Page loaded")
            except:
                debug_print(f"[DEBUG] Timeout waiting for page", "WARNING")

            time.sleep(5)

            # 滾動頁面
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
            except:
                pass

            keywords_with_info = []
            debug_print(f"\n--- 策略: 尋找表格行 (tr) ---")

            row_selectors = [
                "//tr[@jsname='oKdM2c']",
                "//tr[contains(@class, 'enOdEe-wZVHId-xMbwt')]",
            ]

            rows = []
            for selector in row_selectors:
                debug_print(f"嘗試 Selector: {selector}")
                try:
                    temp_rows = self.driver.find_elements(By.XPATH, selector)
                    if len(temp_rows) > 0:
                        rows = temp_rows
                        debug_print(f"  找到 {len(temp_rows)} 個表格行，使用此 selector")
                        break
                except Exception as e:
                    debug_print(f"  失敗: {e}", "WARNING")
                    continue

            if len(rows) == 0:
                debug_print(f"[WARNING] 沒有找到表格行", "WARNING")
                return []

            debug_print(f"成功找到 {len(rows)} 個表格行，開始提取資料...")

            for idx, row in enumerate(rows[:KEYWORDS_PER_REGION], 1):
                try:
                    keyword = None
                    search_volume_text = None

                    # 提取關鍵字
                    try:
                        keyword_elem = row.find_element(By.XPATH, ".//div[@class='mZ3RIc']")
                        keyword = keyword_elem.text.strip()
                    except:
                        try:
                            first_td = row.find_element(By.XPATH, "./td[1]")
                            keyword = first_td.text.strip().split('\n')[0]
                        except:
                            pass

                    if not keyword:
                        continue

                    # 提取搜尋量
                    try:
                        vol_elem = row.find_element(By.XPATH, ".//div[@class='p6GQOc']//div[@class='lqv0Cb']")
                        search_volume_text = vol_elem.text.strip()
                    except:
                        try:
                            vol_elem = row.find_element(By.XPATH, ".//div[@class='lqv0Cb']")
                            search_volume_text = vol_elem.text.strip()
                        except:
                            try:
                                row_text = row.text
                                volume_match = re.search(r'(\d+(?:\.\d+)?(?:萬|千|百)?)\s*\+', row_text)
                                if volume_match:
                                    search_volume_text = volume_match.group(0).strip()
                            except:
                                pass

                    if search_volume_text and '+' not in search_volume_text:
                        search_volume_text = search_volume_text + '+'

                    search_volume_number = convert_search_volume_to_number(search_volume_text) if search_volume_text else None

                    # 排除與去重邏輯
                    exclude_keywords = ['google', 'trends', '搜尋', 'search', '登入', 'login', '探索', '最新熱搜榜']
                    if any(exclude in keyword.lower() for exclude in exclude_keywords):
                        continue

                    if any(k['keyword'] == keyword for k in keywords_with_info):
                        continue

                    keywords_with_info.append({
                        "keyword": keyword,
                        "rank": idx,
                        "region_code": region_code,
                        "search_volume_text": search_volume_text,
                        "search_volume_number": search_volume_number
                    })

                    vol_display = f"{search_volume_text} ({search_volume_number:,})" if search_volume_text and search_volume_number else "NULL"
                    debug_print(f"  [{idx}] '{keyword}' | Vol: {vol_display}")

                except Exception as e:
                    debug_print(f"  [{idx}] 處理行時出錯: {e}", "ERROR")
                    continue

            debug_print(f"\n[RESULT] {region_code}: Found {len(keywords_with_info)} keywords")
            return keywords_with_info

        except Exception as e:
            debug_print(f"{region_code} 抓取發生嚴重錯誤: {e}", "ERROR")
            traceback.print_exc()
            return []

    def run_trends_scrape_multi_region(self):
        """STEP A: 從 6 個地區抓取 Google Trends（含 AI 分類與自動翻譯）"""
        try:
            print(f"\n{'=' * 80}")
            print(f"STEP A: 從 {len(REGIONS)} 個地區抓取 Google Trends")
            print(f"{'=' * 80}")

            all_keywords_with_info = []
            successful_regions = 0
            failed_regions = []

            for idx, region_info in enumerate(REGIONS, 1):
                print(f"\n[{idx}/{len(REGIONS)}] {region_info['code']} ({region_info['name']})")

                regional_keywords = self.scrape_single_region(region_info)

                if len(regional_keywords) > 0:
                    all_keywords_with_info.extend(regional_keywords)
                    successful_regions += 1
                else:
                    failed_regions.append(region_info['code'])
                    debug_print(f"{region_info['code']} 抓取失敗", "WARNING")

                if idx < len(REGIONS):
                    wait_time = random.uniform(3, 6)
                    debug_print(f"等待 {wait_time:.1f}s 後繼續下一個地區...", "INFO")
                    time.sleep(wait_time)

            #  檢查整體結果
            print(f"\n{'=' * 80}")
            print(f"抓取結果統計")
            print(f"{'=' * 80}")
            print(f"成功地區: {successful_regions}/{len(REGIONS)}")
            if failed_regions:
                print(f"失敗地區: {', '.join(failed_regions)}")
            print(f"總關鍵字數: {len(all_keywords_with_info)}")

            if len(all_keywords_with_info) == 0:
                print("\n" + "=" * 80)
                print(" 警告：未抓取到任何關鍵字！")
                print("可能原因：")
                print("  1. Google Trends 頁面結構已變更")
                print("  2. 網路連線問題")
                print("  3. 網頁載入時間不足")
                print("  4. IP 被暫時封鎖")
                print("=" * 80)

                if self.quality_monitor:
                    self.quality_monitor.update_execution(
                        status='失敗',
                        error_message='未抓取到任何關鍵字'
                    )

                return [], [], {}

            expected_count = KEYWORDS_PER_REGION * len(REGIONS)
            if len(all_keywords_with_info) < expected_count * 0.5:
                print(f"\n 警告：只抓取到 {len(all_keywords_with_info)} 個關鍵字（預期 {expected_count}）")

            print(f"\n{'=' * 80}")
            print(f"統計關鍵字地區分布")
            print(f"{'=' * 80}")

            keyword_region_map = defaultdict(lambda: defaultdict(list))

            for item in all_keywords_with_info:
                keyword = item["keyword"]
                region_code = item["region_code"]
                rank = item["rank"]
                search_volume_number = item["search_volume_number"]
                keyword_region_map[keyword][region_code].append((rank, search_volume_number))

            keyword_counter = Counter([item["keyword"] for item in all_keywords_with_info])

            print(f"[INFO] Total appearances: {len(all_keywords_with_info)}")
            print(f"[INFO] Unique keywords: {len(keyword_region_map)}")

            # AI 分類（含翻譯）
            print(f"\n{'=' * 80}")
            print(f"Google Gemini AI 自動分類與翻譯關鍵字 (多執行緒加速)")
            print(f"{'=' * 80}")

            all_unique_keywords = list(keyword_region_map.keys())
            classification_results = {}

            if USE_AI_CLASSIFICATION and GEMINI_AVAILABLE and len(all_unique_keywords) > 0:
                # 切割批次
                batches = [all_unique_keywords[i:i + AI_BATCH_SIZE] for i in range(0, len(all_unique_keywords), AI_BATCH_SIZE)]
            
                max_workers = min(5, len(batches))
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_batch = {executor.submit(classify_keywords_batch, batch): i+1 for i, batch in enumerate(batches)}
                    
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch_num = future_to_batch[future]
                        try:
                            batch_results = future.result()
                            classification_results.update(batch_results)
                            print(f"[INFO] 批次 {batch_num}/{len(batches)} 分類完成！")
                        except Exception as exc:
                            print(f"[ERROR] 批次 {batch_num} 分類產生例外: {exc}")
            else:
                classification_results = classify_keywords_batch(all_unique_keywords)

            # 寫入資料庫
            print(f"\n{'=' * 80}")
            print(f"將關鍵字批次寫入資料庫")
            print(f"{'=' * 80}")

            successfully_inserted = []
            failed_keywords = []
            
            # 1. 準備批次處理 Keywords 的資料結構
            keywords_batch_data = []
            for original_keyword in keyword_region_map.keys():
                classification = classification_results.get(original_keyword, 
                                                            {'category': 'Other', 'search_intent': 'Informational'})
                
                has_chinese = any('\u4e00' <= char <= '\u9fff' for char in original_keyword)
                
                if has_chinese:
                    translated_keyword = original_keyword
                else:
                    translated_keyword = classification.get('english_translation', original_keyword).lower()

                ai_category = classification.get('category', 'Other')
                ai_intent = classification.get('search_intent', 'Informational')

                keywords_batch_data.append({
                    'original_keyword': original_keyword,
                    'keyword': translated_keyword, 
                    'category': ai_category,
                    'search_intent': ai_intent,
                    'english_translation': translated_keyword
                })

            # 2. 呼叫批次處理函數取得 KeywordID
            keyword_id_map = self.get_or_create_keywords_batch(keywords_batch_data)
            
            # 3. 準備批次處理 地區統計 的資料結構
            stats_batch_data = []
            for data in keywords_batch_data:
                original_keyword = data['original_keyword']
                translated_keyword = data['keyword']
                kid = keyword_id_map.get(translated_keyword)
                
                if kid:
                    regions_dict = keyword_region_map[original_keyword]
                    for region_code, rank_volume_list in regions_dict.items():
                        appearance_count = len(rank_volume_list)
                        best_rank = min([rv[0] for rv in rank_volume_list])

                        search_volume_number = None
                        for rv in rank_volume_list:
                            if rv[1] is not None:
                                search_volume_number = rv[1]
                                break
                                
                        stats_batch_data.append({
                            'keyword_id': kid,
                            'region_code': region_code,
                            'appearance_count': appearance_count,
                            'search_volume': search_volume_number,
                            'trend_rank': best_rank
                        })
                        
                    successfully_inserted.append((kid, translated_keyword))
                else:
                    failed_keywords.append(translated_keyword)
                    
            # 印出處理狀態
            for idx, data in enumerate(keywords_batch_data, 1):
                trans_text = f" (原詞: {data['original_keyword']})" if data['keyword'] != data['original_keyword'] else ""
                print(f"[{idx}/{len(keywords_batch_data)}] '{data['keyword']}'{trans_text} → {data['category']}/{data['search_intent']}")

            # 4. 呼叫批次寫入 地區統計
            self.insert_region_stats_batch(stats_batch_data)

            # 選出英文和中文前 N 名
            print(f"\n{'=' * 80}")
            print(f"選擇搜尋關鍵字（英文前 {TOP_ENGLISH_KEYWORDS} 名 + 中文前 {TOP_CHINESE_KEYWORDS} 名）")
            print(f"{'=' * 80}")

            # 聚合與語言分流邏輯合併
            merged_keywords = {}
            for kw, count in keyword_counter.items():
                cls_data = classification_results.get(kw, {})
                is_chinese = any('\u4e00' <= char <= '\u9fff' for char in kw)
                
                # (中文保持原樣，外文使用翻譯)
                final_kw = kw if is_chinese else cls_data.get('english_translation', kw).lower()
                
                if final_kw not in merged_keywords:
                    merged_keywords[final_kw] = {'count': 0, 'is_chinese': is_chinese}
                merged_keywords[final_kw]['count'] += count

            # 依出現次數排序
            sorted_keywords = sorted(merged_keywords.items(), key=lambda x: x[1]['count'], reverse=True)
            
            # 直接分流，不需再次檢查字元
            english_keywords = []
            chinese_keywords = []
            for kw, data in sorted_keywords:
                if data['is_chinese']:
                    chinese_keywords.append((kw, data['count']))
                else:
                    english_keywords.append((kw, data['count']))

            top_english = english_keywords[:TOP_ENGLISH_KEYWORDS]
            print(f"\n英文關鍵字（前 {TOP_ENGLISH_KEYWORDS} 名）：")
            for idx, (kw, count) in enumerate(top_english, 1):
                cat = classification_results.get(kw, {}).get('category', '?')
                print(f"  [{idx}] '{kw}' ({cat}): {count} 次出現")

            top_chinese = chinese_keywords[:TOP_CHINESE_KEYWORDS]
            print(f"\n中文關鍵字（前 {TOP_CHINESE_KEYWORDS} 名）：")
            for idx, (kw, count) in enumerate(top_chinese, 1):
                cat = classification_results.get(kw, {}).get('category', '?')
                print(f"  [{idx}] '{kw}' ({cat}): {count} 次出現")

            top_keywords_to_search = top_english + top_chinese

            print(f"\n總共將搜尋 {len(top_keywords_to_search)} 個關鍵字")
            print(f"{'=' * 80}")

            print(f"\n{'=' * 80}")
            print(f"[INFO] Successfully processed: {len(successfully_inserted)} keywords")
            print(f"[INFO] Failed: {len(failed_keywords)} keywords")

            if failed_regions:
                print(f"[WARNING] Failed regions: {', '.join(failed_regions)}")

            print(f"{'=' * 80}\n")

            return successfully_inserted, top_keywords_to_search, keyword_id_map

        except Exception as e:
            log_error("trends_errors.log", f"run_trends_scrape_multi_region error: {e}")
            debug_print(f"多地區抓取失敗: {e}", "ERROR")
            print(traceback.format_exc())
            return [], [], {}

    def run_keyword_search(self, keyword):
        """在 Bing 搜尋關鍵字"""
        try:
            retry_with_backoff(lambda: self.driver.get("https://www.bing.com"))
            time.sleep(random.uniform(0.8, 1.5))

            search_box = self.driver.find_element(By.NAME, "q")
            try:
                search_box.clear()
            except:
                pass

            for ch in keyword:
                search_box.send_keys(ch)
                time.sleep(random.uniform(0.02, 0.06))

            search_box.send_keys(Keys.ENTER)
            self.simulate_human()

            summary = self.extract_summary()
            return summary

        except Exception as e:
            log_error("keyword_search_errors.log", f"run_keyword_search error for '{keyword}': {e}")
            return ""


# ==================== 主程式 ====================

def main():
    parser = argparse.ArgumentParser(description="Google Trends Crawler v1.1.0 - Stability Enhanced Edition")
    parser.add_argument("--no-search", action="store_true", help="Skip keyword search")
    parser.add_argument("--no-ai", action="store_true", help="Disable AI classification")
    parser.add_argument("--no-snapshot", action="store_true", help="Skip daily snapshot creation")
    parser.add_argument("--no-debug", action="store_true", help="Disable DEBUG mode")
    args = parser.parse_args()

    global USE_AI_CLASSIFICATION, DEBUG_MODE
    if args.no_ai:
        USE_AI_CLASSIFICATION = False
    if args.no_debug:
        DEBUG_MODE = False

    c = Crawler()
    
    try:
        print(f"\n{'=' * 80}")
        print(f"Google Trends Crawler v1.1.0")
        print(f"[系統診斷] 當前 Python 行程 PID: {os.getpid()}")
        print(f"[系統診斷] 啟動時間: {datetime.now().strftime('%H:%M:%S.%f')}")
        print(f"{'=' * 80}")

        force_cleanup_browser_processes()

        ai_status = "Google Gemini 2.0 Flash" if USE_AI_CLASSIFICATION and GEMINI_AVAILABLE else "Rules-Based"
        if USE_AI_CLASSIFICATION and GEMINI_AVAILABLE:
            ai_status += f" ({len(GEMINI_API_KEYS)} API Keys)"
        debug_print(f"AI: {ai_status}")

        print(f"Search: 英文前 {TOP_ENGLISH_KEYWORDS} 名 + 中文前 {TOP_CHINESE_KEYWORDS} 名")
        print(f"Snapshot: {'Enabled' if not args.no_snapshot else 'Disabled'}")
        print(f"Quality Monitor: SQL Server Version")
        print(f"Relation Analyzer: SQL Server Version")
        print(f"DEBUG Mode: {'Enabled' if DEBUG_MODE else 'Disabled'}")
        print(f"Retry Mechanism: Max {SCRAPE_MAX_RETRIES} retries per region")
        print(f"{'=' * 80}")

        c.connect_db()
        c.init_driver()

        # 啟動執行監控
        if c.quality_monitor:
            execution_id = c.quality_monitor.start_execution(crawler_version="v1.1.0")

        keywords_with_ids, top_keywords_to_search, keyword_id_map = c.run_trends_scrape_multi_region()

        if len(keywords_with_ids) == 0:
            debug_print("未抓取到任何關鍵字，提早結束", "ERROR")

            if c.quality_monitor:
                c.quality_monitor.update_execution(
                    regions_scraped=len(REGIONS),
                    total_keywords=0,
                    new_keywords=0,
                    status='失敗',
                    error_message='未抓取到任何關鍵字'
                )

            return

        # 更新執行統計
        if c.quality_monitor:
            c.quality_monitor.update_execution(
                regions_scraped=len(REGIONS),
                total_keywords=len(keywords_with_ids),
                new_keywords=len(keywords_with_ids)
            )

        if not args.no_snapshot and c.snapshot_manager:
            print(f"\n{'=' * 80}")
            print(f"STEP A_1: 建立每日趨勢快照")
            print(f"{'=' * 80}")

            snapshot_success = c.snapshot_manager.create_daily_snapshots()

            if snapshot_success:
                c.snapshot_manager.print_daily_report()

        if c.quality_monitor:
            print(f"\n{'=' * 80}")
            print(f"STEP A_2: 數據質量檢查")
            print(f"{'=' * 80}")

            quality_result = c.quality_monitor.check_quality()

        if c.relation_analyzer:
            print(f"\n{'=' * 80}")
            print(f"STEP A_3: 關鍵字關聯分析")
            print(f"{'=' * 80}")

            co_success = c.relation_analyzer.calculate_co_occurrence()

            if co_success:
                c.relation_analyzer.update_cooccurrence_scores()

        if not args.no_search and top_keywords_to_search:
            print(f"\n{'=' * 80}")
            print(f"STEP B: 搜尋英文前 {TOP_ENGLISH_KEYWORDS} 名 + 中文前 {TOP_CHINESE_KEYWORDS} 名關鍵字")
            print(f"{'=' * 80}")

            searched_count = 0

            scraped_results = []  # 暫存列表
            for idx, (keyword, count) in enumerate(top_keywords_to_search, 1):
                is_chinese = any('\u4e00' <= char <= '\u9fff' for char in keyword)
                lang = "中文" if is_chinese else "英文"

                print(f"\n[{idx}/{len(top_keywords_to_search)}] Searching ({lang}): '{keyword}' (出現 {count} 次)")

                kid = keyword_id_map.get(keyword)

                if kid:
                    try:
                        summary = c.run_keyword_search(keyword)
                        # 存入暫存列表，不直接寫資料庫
                        scraped_results.append((kid, summary, "Success", None))
                        searched_count += 1
                    except Exception as e:
                        scraped_results.append((kid, None, "Fail", str(e)))

                    if idx < len(top_keywords_to_search):
                        time.sleep(random.uniform(PER_KEYWORD_MIN, PER_KEYWORD_MAX))
            
            if scraped_results:
                c.insert_keywords_log_batch(scraped_results)

            if c.quality_monitor:
                c.quality_monitor.update_execution(
                    keywords_searched=searched_count
                )

        print(f"\n{'=' * 80}")
        print(f"執行完成！")
        print(f"{'=' * 80}\n")
        print(f"[INFO] 資料庫寫入統計：")
        print(f"  - 地區數: {len(REGIONS)}")
        print(f"  - KeywordsMaster: {len(keywords_with_ids)} 筆")
        print(f"  - KeywordRegionStats: 已記錄各地區統計")
        print(f"  - DailyTrendSnapshots: {'已建立' if not args.no_snapshot else '跳過'}")
        print(f"  - KeywordsLog: {len(top_keywords_to_search) if not args.no_search else 0} 筆")
        print(f"  - 品質檢查: 已完成")
        print(f"  - 關聯分析: 已完成")
        print(f"\n")

        # 標記執行成功
        if c.quality_monitor:
            c.quality_monitor.update_execution(status='成功')

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        print(traceback.format_exc())
        log_error("fatal_errors.log", f"main() error: {e}\n{traceback.format_exc()}")
        if c and c.quality_monitor:
            c.quality_monitor.update_execution(status='失敗', error_message=str(e))
            
    finally:
        c.close()

        force_cleanup_browser_processes()

        print(f"\n{'=' * 50}")
        print(f"Program Exit Complete")
        print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()