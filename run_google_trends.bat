@echo off
:: 設定主控台為 UTF-8 編碼，避免中文亂碼
chcp 65001 >nul
set PYTHONIOENCODING=utf-8

:: 切換到專案目錄
cd /d C:\ClassProject

echo ============================================================
echo 啟動 Google Trends 自動化爬蟲...
echo 開始時間: %date% %time%
echo (日誌將由 Python 自動生成並儲存於 logs 資料夾中)
echo ============================================================
echo.

:: 🔥 關鍵修正：先啟動 (Activate) Anaconda 虛擬環境
:: 這樣才能正確載入 Selenium 和 PyODBC 所需的系統變數與底層檔案
call "C:\Users\Noqqa\anaconda3\Scripts\activate.bat" Keyword_Parser

:: 啟動環境後，直接呼叫 python 即可
python google_trends_automation.py

echo.
echo ============================================================
echo 結束時間: %date% %time%
echo ============================================================

:: 檢查執行結果
if %errorlevel% equ 0 (
    echo [SUCCESS] 程式順利執行完畢！
) else (
    echo [ERROR] 程式發生異常，錯誤代碼: %errorlevel%
    echo 請前往 C:\ClassProject\logs 查看最新的日誌檔了解錯誤原因。
)

:: 讓視窗停留，方便你確認是否有錯誤訊息印在畫面上
pause