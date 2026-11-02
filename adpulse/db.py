import sqlite3, os
from pathlib import Path
from datetime import datetime, timezone
DB_PATH = Path(os.getenv("ADPULSE_DB", Path(__file__).resolve().parent.parent / "adpulse.sqlite3"))

def now(): return datetime.now(timezone.utc).isoformat(timespec="seconds")
def connect():
    c=sqlite3.connect(DB_PATH, timeout=20); c.row_factory=sqlite3.Row; c.execute('PRAGMA foreign_keys=ON'); return c
def init_db():
    DB_PATH.parent.mkdir(parents=True,exist_ok=True)
    with connect() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS ad_performance(id INTEGER PRIMARY KEY, platform TEXT NOT NULL,campaign_id TEXT NOT NULL,campaign_name TEXT NOT NULL,report_date TEXT NOT NULL,spend REAL NOT NULL,impressions INTEGER NOT NULL,clicks INTEGER NOT NULL,conversions INTEGER NOT NULL,source_payload TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(platform,campaign_id,report_date));
        CREATE TABLE IF NOT EXISTS pipeline_runs(run_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,completed_at TEXT,status TEXT NOT NULL,records_fetched INTEGER DEFAULT 0,records_inserted INTEGER DEFAULT 0,records_updated INTEGER DEFAULT 0,records_rejected INTEGER DEFAULT 0,successful_requests INTEGER DEFAULT 0,failed_requests INTEGER DEFAULT 0,duration_seconds REAL DEFAULT 0,summary TEXT);
        CREATE TABLE IF NOT EXISTS api_request_logs(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL,platform TEXT NOT NULL,endpoint TEXT NOT NULL,page INTEGER,attempt INTEGER,status_code INTEGER,event TEXT NOT NULL,delay_seconds REAL DEFAULT 0,started_at TEXT NOT NULL,duration_seconds REAL DEFAULT 0,error TEXT,FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id));
        CREATE TABLE IF NOT EXISTS rejected_records(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL,platform TEXT,report_date TEXT,source_payload TEXT NOT NULL,validation_error TEXT NOT NULL,rejected_at TEXT NOT NULL,FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id));
        CREATE TABLE IF NOT EXISTS platform_run_stats(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL,platform TEXT NOT NULL,status TEXT NOT NULL,records_fetched INTEGER DEFAULT 0,attempts INTEGER DEFAULT 0,failed_attempts INTEGER DEFAULT 0,recoveries INTEGER DEFAULT 0,last_http_status INTEGER,duration_seconds REAL DEFAULT 0,FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id));
        ''')
        columns={row['name'] for row in c.execute('PRAGMA table_info(ad_performance)')}
        if 'source_payload' not in columns: c.execute('ALTER TABLE ad_performance ADD COLUMN source_payload TEXT')
def insert_run(run_id, started):
    with connect() as c: c.execute("INSERT INTO pipeline_runs(run_id,started_at,status) VALUES(?,?,'Running')",(run_id,started))
def finish_run(run_id, **v):
    with connect() as c: c.execute('''UPDATE pipeline_runs SET completed_at=?,status=?,records_fetched=?,records_inserted=?,records_updated=?,records_rejected=?,successful_requests=?,failed_requests=?,duration_seconds=?,summary=? WHERE run_id=?''',(now(),v['status'],v['fetched'],v['inserted'],v['updated'],v['rejected'],v['successes'],v['failures'],v['duration'],v.get('summary',''),run_id))
def log_request(run_id, platform, endpoint, page, attempt, status, event, started, duration=0, delay=0, error=None):
    with connect() as c: c.execute('INSERT INTO api_request_logs(run_id,platform,endpoint,page,attempt,status_code,event,delay_seconds,started_at,duration_seconds,error) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(run_id,platform,endpoint,page,attempt,status,event,delay,started,duration,error))
def load(records):
    inserted=updated=0; ts=now()
    with connect() as c:
        c.execute('BEGIN IMMEDIATE')
        for r in records:
            prior=c.execute('SELECT id FROM ad_performance WHERE platform=? AND campaign_id=? AND report_date=?',(r['platform'],r['campaign_id'],r['report_date'])).fetchone()
            c.execute('''INSERT INTO ad_performance(platform,campaign_id,campaign_name,report_date,spend,impressions,clicks,conversions,source_payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(platform,campaign_id,report_date) DO UPDATE SET campaign_name=excluded.campaign_name,spend=excluded.spend,impressions=excluded.impressions,clicks=excluded.clicks,conversions=excluded.conversions,source_payload=excluded.source_payload,updated_at=excluded.updated_at''',(r['platform'],r['campaign_id'],r['campaign_name'],r['report_date'],r['spend'],r['impressions'],r['clicks'],r['conversions'],r.get('source_payload'),ts,ts))
            if prior: updated+=1
            else: inserted+=1
    return inserted,updated
