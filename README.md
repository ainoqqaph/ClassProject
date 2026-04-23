# 跨國市場關鍵字抓取
> Enterprise End-to-End Data Pipeline & AI-Driven Market Intelligence Dashboard

專案為一套完整的」數據工程與商業分析解決方案。透過自動化爬蟲獲取跨國 Google Trends 熱搜數據，導入 Google Gemini AI 進行語意分析與商業標籤賦能，並經由 Apache Airflow 自動化排程，最終存入 SQL Server 資料倉儲，透過 Power BI 呈現。

### 1. 自動化爬蟲
- 於 Linux Docker 環境建置爬蟲。
- 透過關閉圖片加載與阻擋不必要之 JS 腳本，大幅降低 Docker 記憶體消耗。

### 2. AI輔助分類
- 整合 **Google Gemini API**，將生硬且非結構化的熱搜關鍵字，自動判定並賦予「商業意圖 (Search Intent)」與「產業分類 (Category)」。
- 具備自我學習機制的 `dynamic_category_rules.json`，能隨著爬取次數增加，自動擴充並完善分類規則字典。

### 3. 資料倉儲與 ETL 架構
- 遵循 **星狀模型 (Star Schema)** 設計關聯式資料庫 (SQL Server)。
- 實作 **Append-Only 日誌模式** 進行資料補抓，保留完整的爬蟲歷史軌跡 (Audit Trail)。

### 4. 自動化排程管線
- 使用 **Apache Airflow** 建立有向無環圖 (DAG)。
- 將流程拆解為 `抓取最新熱搜 ➔ AI 語意分類學習 ➔ 失敗摘要補抓` 的標準化 ETL 流水線，實現每日自動化排程與監控。

### 5. 商業洞察儀表板
- 運用 Power BI 與進階 DAX 語法 
- 打造四象限決策散佈圖、神經網絡關聯圖與 AI 戰情快報，協助管理層快速辨識跨國市場中的「高價值潛力商機」。

---

##  技術棧 (Tech Stack)

- **資料獲取 (Data Ingestion):** Python, Selenium, Requests, BeautifulSoup
- **人工智慧 (AI/LLM):** Google Gemini Pro API
- **排程與容器化 (Orchestration & DevOps):** Apache Airflow, Docker, Docker Compose
- **資料庫與資料倉儲 (Database & DWH):** Microsoft SQL Server, T-SQL, SSAS
- **資料視覺化 (Data Visualization):** Power BI, DAX

---

##  專案架構 (Project Structure)

```text
ClassProject/
├── dags/                                 # Airflow DAGs 與核心執行腳本
│   ├── google_trends_dag.py              # Airflow 排程設定檔
│   ├── google_trends_automation_headless.py # 核心爬蟲與基礎 ETL
│   ├── Aikeyword.py                      # Gemini AI 語意分類模組
│   ├── search_logs.py                    # 資料智慧補抓腳本
│   └── dynamic_category_rules.example.json # AI 分類規則字典 (範例檔)
├── SSAS/                                 # 多維度模型專案設定
├── .env.example                          # 環境變數設定範本
├── docker-compose.yaml                   # Airflow 與資料庫容器化部署配置
├── Dockerfile                            # 自定義含有 Chrome 的 Airflow 映像檔
├── requirements.txt                      # Python 第三方套件依賴清單
└── README.md                             # 專案說明文件
