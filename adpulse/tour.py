"""Interactive Streamlit product tour backed by real AdPulse API and SQLite operations."""
from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from . import db
from .etl import run_pipeline, transform_validate

DEMO_ORDER = ["daily", "reliability", "storage", "duplicates"]
DEMO_NAMES = {
    "daily": "Daily API Collection",
    "reliability": "API Reliability",
    "storage": "Database Storage",
    "duplicates": "Duplicate Prevention",
}
DEMO_DESCRIPTIONS = {
    "daily": "Watch data arrive from three local advertising provider simulators.",
    "reliability": "Trigger real simulated rate limits, failures and timeouts, then inspect retries.",
    "storage": "Follow provider payloads through normalization, validation and SQLite transactions.",
    "duplicates": "Run the same reporting date twice and verify the composite key and UPSERT.",
}
STEPS = {
    "daily": ["Scheduler", "API requests", "Provider responses", "Pagination", "Extraction complete", "Implementation"],
    "reliability": ["Normal request", "Inject HTTP 429", "Retry-After", "Backoff timeline", "Retry exhaustion", "Partial success", "Implementation"],
    "storage": ["Raw API data", "Common schema", "Validation", "Parameterized SQL", "Transaction and rollback", "Database Explorer", "Implementation"],
    "duplicates": ["Initial row count", "First extraction", "Repeat identical data", "Composite unique key", "UPSERT operation", "Prove no duplicate rows", "Update a metric", "Implementation"],
}


def _open_demo(demo_id: str) -> None:
    st.session_state["welcome_seen"] = True
    st.session_state["active_demo"] = demo_id
    st.session_state["tour_step"] = 0
    st.session_state["tour_done"] = False
    st.session_state["tour_paused"] = False
    st.session_state["tour_results"] = {}
    st.session_state["page_nav"] = "Product Walkthrough"


def _next() -> None:
    st.session_state["tour_step"] = st.session_state.get("tour_step", 0) + 1


def _previous() -> None:
    st.session_state["tour_step"] = max(0, st.session_state.get("tour_step", 0) - 1)


def _toggle_pause() -> None:
    st.session_state["tour_paused"] = not st.session_state.get("tour_paused", False)


def _replay() -> None:
    demo_id = st.session_state.get("active_demo")
    st.session_state["tour_step"] = 0
    st.session_state["tour_done"] = False
    st.session_state["tour_paused"] = False
    results = st.session_state.get("tour_results", {})
    prefix = "" if demo_id == "complete" else f"{demo_id}:"
    st.session_state["tour_results"] = {k: v for k, v in results.items() if prefix and not k.startswith(prefix)}


def _exit() -> None:
    st.session_state["active_demo"] = None
    st.session_state["tour_done"] = False
    st.session_state["tour_paused"] = False
    st.session_state["welcome_seen"] = True
    st.session_state["page_nav"] = "Overview"


def _explore() -> None:
    st.session_state["welcome_seen"] = True
    st.session_state["active_demo"] = None
    st.session_state["page_nav"] = "Overview"


def _skip() -> None:
    _explore()


def render_welcome() -> None:
    st.markdown('<div class="tour-eyebrow">ADPULSE · PRODUCT WALKTHROUGH</div>', unsafe_allow_html=True)
    st.title("Welcome to AdPulse")
    st.markdown("*See how reliable API data collection works, from extraction to reporting.*")
    st.markdown('<div class="tour-simulated"><b>Simulated Demonstration Data</b> · The walkthrough makes real requests to local simulated providers; it never connects to live ad accounts.</div>', unsafe_allow_html=True)
    for start in range(0, len(DEMO_ORDER), 2):
        cards = st.columns(2)
        for col, demo_id in zip(cards, DEMO_ORDER[start:start+2]):
            idx = DEMO_ORDER.index(demo_id)
            with col:
                st.markdown(f'''<div class="tour-card"><div class="tour-icon">0{idx+1}</div><div class="tour-eyebrow">{DEMO_NAMES[demo_id].upper()}</div><p>{DEMO_DESCRIPTIONS[demo_id]}</p></div>''', unsafe_allow_html=True)
                st.button("Play Demo", key=f"welcome_play_{demo_id}", type="primary", use_container_width=True, on_click=_open_demo, args=(demo_id,))
    st.button("Start Complete Walkthrough", key="welcome_complete", use_container_width=True, on_click=_open_demo, args=("complete",))
    exit_options = st.columns(2)
    exit_options[0].button("Skip Walkthrough", key="welcome_skip", use_container_width=True, on_click=_skip)
    exit_options[1].button("Explore Application", key="welcome_explore", use_container_width=True, on_click=_explore)


def _key(demo_id: str, action: str) -> str:
    return f"{demo_id}:{action}"


def _result(demo_id: str, action: str):
    return st.session_state.get("tour_results", {}).get(_key(demo_id, action))


def _show_result_summary(result: dict) -> None:
    st.success(f"{result['status']} · {result['fetched']} fetched · {result['inserted']} inserted · {result['updated']} updated · {result['rejected']} rejected · {result['duration']:.2f}s · run {result['run_id'][:8]}")
    if result.get("platforms"):
        st.dataframe(pd.DataFrame(result["platforms"]), use_container_width=True, hide_index=True)


def _run_action(demo_id: str, action: str, label: str, report_date: str, *, revision: int = 0, injection=None, max_retries: int = 3, base_delay: float = 0.08, max_delay: float = 0.25, timeout: float = 0.65) -> None:
    result = _result(demo_id, action)
    if result is None:
        if st.button(label, key=f"tour_action_{demo_id}_{action}", type="primary"):
            with st.spinner("Making real requests to the local provider simulator…"):
                before = _database_count() if demo_id == "duplicates" else None
                run = run_pipeline(report_date, revision, injection, max_retries, base_delay, max_delay, timeout)
                if demo_id == "duplicates":
                    run["db_count_before"] = before
                    run["row_count_after"] = _database_count()
            st.session_state.setdefault("tour_results", {})[_key(demo_id, action)] = run
            st.rerun()
    else:
        _show_result_summary(result)
        if st.button("Run this step again", key=f"tour_again_{demo_id}_{action}"):
            st.session_state["tour_results"].pop(_key(demo_id, action), None)
            st.rerun()


def _run_logs(result: dict) -> pd.DataFrame:
    if not result:
        return pd.DataFrame()
    with db.connect() as conn:
        return pd.read_sql_query("SELECT started_at,platform,endpoint,page,attempt,status_code,event,delay_seconds,duration_seconds,error FROM api_request_logs WHERE run_id=? ORDER BY id", conn, params=(result["run_id"],))


def _events_frame(result: dict) -> pd.DataFrame:
    if not result:
        return pd.DataFrame()
    rows=[]
    for event in result.get("events", []):
        rows.append({"platform":event.get("platform"),"page":event.get("page"),"attempt":event.get("attempt"),"HTTP":event.get("status"),"event":event.get("event", "success" if event.get("status")==200 else "failed"),"Retry-After":event.get("retry_after"),"delay_s":round(event.get("delay",0),3),"duration_s":round(event.get("duration",0),3),"error":event.get("error")})
    return pd.DataFrame(rows)


def _records_for_date(report_date: str, limit: int = 12) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query("SELECT platform,campaign_id,campaign_name,report_date,spend,impressions,clicks,conversions,source_payload FROM ad_performance WHERE report_date=? ORDER BY platform,campaign_id LIMIT ?", conn, params=(report_date, limit))


def _database_count() -> int:
    with db.connect() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM ad_performance").fetchone()[0])


def _duplicate_key_count() -> int:
    with db.connect() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM (SELECT 1 FROM ad_performance GROUP BY platform,campaign_id,report_date HAVING COUNT(*)>1)").fetchone()[0])


def _display_payload_rows(report_date: str, count: int = 5) -> None:
    rows = _records_for_date(report_date, count)
    if rows.empty:
        st.info("No stored records for this reporting date yet. Run the action above to fetch and load actual simulator output.")
        return
    for row in rows.to_dict("records"):
        with st.expander(f"{row['platform']} · {row['campaign_id']} · {row['campaign_name']}"):
            st.caption("Persisted source payload returned by the local provider")
            st.json(json.loads(row["source_payload"]) if row.get("source_payload") else {})
            st.caption("Database row")
            st.json({k: row[k] for k in ("platform","campaign_id","campaign_name","report_date","spend","impressions","clicks","conversions")})


def _rollback_demo() -> dict:
    """Exercise the real db.load transaction against an isolated temporary SQLite file."""
    original_path = db.DB_PATH
    with tempfile.TemporaryDirectory(prefix="adpulse-tour-") as tmp:
        db.DB_PATH = Path(tmp) / "rollback-demo.sqlite3"
        try:
            db.init_db()
            valid={"platform":"Demo Provider","campaign_id":"TX-001","campaign_name":"Valid row","report_date":"2026-11-03","spend":10.0,"impressions":100,"clicks":5,"conversions":1,"source_payload":"{}"}
            invalid={**valid,"campaign_id":"TX-002","campaign_name":"Constraint failure","spend":None}
            try:
                db.load([valid, invalid])
            except sqlite3.IntegrityError as exc:
                error=str(exc)
            else:
                error="Expected NOT NULL constraint error did not occur"
            with db.connect() as conn:
                final_count=int(conn.execute("SELECT COUNT(*) FROM ad_performance").fetchone()[0])
            return {"error":error,"rows_after_rollback":final_count,"rolled_back":final_count==0}
        finally:
            db.DB_PATH = original_path


def _render_step(demo_id: str, step: int, report_date: str) -> None:
    if demo_id == "daily":
        _daily_step(step, report_date)
    elif demo_id == "reliability":
        _reliability_step(step, report_date)
    elif demo_id == "storage":
        _storage_step(step, report_date)
    else:
        _duplicate_step(step, report_date)


def _daily_step(step: int, report_date: str) -> None:
    if step == 0:
        st.markdown("### Scheduler status")
        st.markdown('<div class="tour-flow"><span>Daily at 02:00 IST</span><b>→</b><strong>Scheduler: DISABLED</strong></div>', unsafe_allow_html=True)
        st.warning("02:00 IST is the proposed demo schedule only. The repository has no scheduler; a person starts each pipeline run from the Streamlit UI.")
        st.caption("Concept: a scheduler starts unattended work at a planned time. This application currently supports manual runs.")
        st.code("# No APScheduler job is registered.\n# Manual trigger: app.py → pipeline_controls() → run_pipeline(...)\n# Simulator startup: app.py → start_api()", language="python")
    elif step == 1:
        st.markdown("### Make real local GET requests")
        st.markdown('<div class="tour-flow"><strong>AdPulse</strong><b>→</b><span>Google simulator</span><b>·</b><span>Meta simulator</span><b>·</b><span>LinkedIn simulator</span></div>', unsafe_allow_html=True)
        _run_action("daily", "collection", "Run actual collection now", report_date)
        result=_result("daily","collection")
        if result:
            st.dataframe(_run_logs(result), use_container_width=True, hide_index=True)
    elif step == 2:
        st.markdown("### Provider responses from SQLite source payloads")
        _display_payload_rows(report_date)
        st.caption("These JSON objects were returned by the local FastAPI routes and persisted with the loaded records. They are simulated demonstration data.")
    elif step == 3:
        st.markdown("### Page 1 followed by page 2")
        result=_result("daily","collection")
        if result:
            logs=_run_logs(result)
            summary=logs.groupby("platform").agg(pages=("page","nunique"),requests=("attempt","count")).reset_index()
            st.dataframe(summary, use_container_width=True, hide_index=True)
            st.dataframe(logs[["platform","page","endpoint","status_code","event"]], use_container_width=True, hide_index=True)
            st.caption("Each simulator has seven campaigns. With page_size=4, the actual requests fetch two pages per provider.")
        else:
            st.info("Run the collection in the previous step to inspect its actual page requests.")
    elif step == 4:
        st.markdown("### Extraction result")
        result=_result("daily","collection")
        if result:
            metrics=st.columns(4)
            metrics[0].metric("Providers",3)
            metrics[1].metric("Pages requested",int(_run_logs(result).groupby(["platform","page"]).ngroups))
            metrics[2].metric("Records fetched",result["fetched"])
            metrics[3].metric("Duration",f"{result['duration']:.2f}s")
            st.metric("Reporting date",report_date)
            _show_result_summary(result)
        else:
            st.info("Run actual collection first; this summary is populated from its persisted run and request logs.")
    else:
        st.markdown("### Implementation")
        st.code("# adpulse/etl.py · extract()\nresponse = requests.get(endpoint, params=params, timeout=timeout)\npayload = response.json()\nrows, next_page = parse_response(slug, payload)\ndata.extend({'source_platform': platform, 'payload': row} for row in rows)", language="python")
        st.caption("GET reads a resource. Query parameters select reporting date and page. JSON carries records. Pagination follows provider-specific continuation fields. Daily scheduling is disabled in this build.")
        st.markdown("**Code:** `adpulse/etl.py` · `extract`, `parse_response`; **routes:** `adpulse/providers.py` · `get_campaigns`.")


def _reliability_step(step: int, report_date: str) -> None:
    if step == 0:
        st.markdown("### Normal request · HTTP 200")
        _run_action("reliability","normal","Run a normal successful pipeline",report_date)
        result=_result("reliability","normal")
        if result: st.dataframe(_events_frame(result),use_container_width=True,hide_index=True)
    elif step == 1:
        st.markdown("### Inject HTTP 429 for two actual requests")
        st.caption("The provider returns 429 twice, then returns 200. This configuration is sent to the local simulator; the retry handler makes the requests.")
        inject={"platform":"google","scenario":"429","failures_before_recovery":2,"retry_after":"0.2"}
        _run_action("reliability","recovery","Run real 429 recovery",report_date,injection=inject,max_retries=3)
        result=_result("reliability","recovery")
        if result: st.dataframe(_events_frame(result),use_container_width=True,hide_index=True)
    elif step == 2:
        st.markdown("### Respect the simulator's Retry-After header")
        st.code("HTTP/1.1 429 Too Many Requests\nRetry-After: 0.2",language="http")
        result=_result("reliability","recovery")
        if result:
            events=_events_frame(result)
            st.dataframe(events[events["HTTP"]==429][["platform","attempt","HTTP","Retry-After","delay_s","event"]],use_container_width=True,hide_index=True)
            st.caption("Retry-After is configured on the provider response and read from response.headers by extract(). The actual response header and applied delay are shown.")
        else: st.info("Run the 429 recovery step to populate actual response attempts and delays.")
    elif step == 3:
        st.markdown("### Actual retry timeline")
        result=_result("reliability","recovery")
        if result:
            events=_events_frame(result)
            target=events[events.platform=="Google Ads"]
            for row in target.to_dict("records"):
                state="HTTP 200 · recovered" if row["HTTP"]==200 else f"HTTP {row['HTTP']} · wait {row['delay_s']:.2f}s then retry"
                st.markdown(f"`Page {row['page']} · Attempt {row['attempt']}` → **{state}**")
            st.dataframe(target,use_container_width=True,hide_index=True)
        else: st.info("Run the 429 recovery step first.")
        st.caption("The delay for 429 comes from Retry-After. Other retryable failures use exponential backoff plus jitter.")
    elif step == 4:
        st.markdown("### Retry exhaustion · HTTP 500")
        injection={"platform":"google","scenario":"500","failures_before_recovery":10}
        _run_action("reliability","exhaustion","Run actual retry-exhaustion scenario",report_date,injection=injection,max_retries=2,base_delay=.08,max_delay=.2)
        result=_result("reliability","exhaustion")
        if result:
            st.dataframe(_events_frame(result),use_container_width=True,hide_index=True)
            st.warning("The terminal failed attempt appears after the initial request plus the configured retries. Other providers can still succeed, so the overall run is Partial Success.")
    elif step == 5:
        st.markdown("### One run: success, recovery and failure")
        injections=[
            {"platform":"meta","scenario":"429","failures_before_recovery":1,"retry_after":"0.2"},
            {"platform":"linkedin","scenario":"503","failures_before_recovery":10,"retry_after":"0.2"},
        ]
        _run_action("reliability","partial","Run mixed-provider partial-success scenario",report_date,injection=injections,max_retries=1,base_delay=.08,max_delay=.2)
        result=_result("reliability","partial")
        if result:
            st.dataframe(pd.DataFrame(result["platforms"]),use_container_width=True,hide_index=True)
            st.dataframe(_events_frame(result),use_container_width=True,hide_index=True)
            st.info(f"Actual aggregate run status: {result['status']}.")
    else:
        st.markdown("### Implementation")
        st.code("# adpulse/etl.py · extract()\ncan_retry = response.status_code in RETRYABLE and attempt <= max_retries\ndelay = retry_after(response.headers.get('Retry-After'))\nif can_retry:\n    delay = delay if delay is not None else backoff(attempt, base_delay, max_delay)\n    time.sleep(delay)\n    continue",language="python")
        st.caption("429 is rate limiting. Retry-After is a provider hint. Backoff grows with each attempt; jitter spreads callers. Timeouts are retried. 400 and 403 are terminal. A 401 would also be terminal; no token refresh exists.")
        st.markdown("**Code:** `adpulse/etl.py` · `retry_after`, `backoff`, `extract`, `run_pipeline`; **simulator:** `adpulse/providers.py` · `configure_injection`, `get_campaigns`.")


def _storage_step(step: int, report_date: str) -> None:
    if step == 0:
        st.markdown("### Raw response from a local provider")
        _run_action("storage","source","Fetch and persist sample provider data",report_date)
        _display_payload_rows(report_date)
    elif step == 1:
        st.markdown("### Provider fields → common record")
        rows=_records_for_date(report_date,1)
        if rows.empty:
            st.info("Fetch sample data in the previous step first.")
        else:
            row=rows.iloc[0]
            raw=json.loads(row["source_payload"])
            st.json(raw)
            mapping=pd.DataFrame([
                {"Provider source":key,"Normalized field":target,"Actual behavior":"mapped by transform_validate"}
                for key,target in [("source_platform","platform"),("campaign_id","campaign_id"),("campaign_name","campaign_name"),("report_date","report_date"),("spend","spend"),("impressions","impressions"),("clicks","clicks"),("conversions","conversions")]
            ])
            st.dataframe(mapping,use_container_width=True,hide_index=True)
            st.caption("The simulator already sends `spend`; it does not send `cost_micros`. No currency conversion is implemented.")
    elif step == 2:
        st.markdown("### Run the actual validator")
        rows=_records_for_date(report_date,1)
        if rows.empty: st.info("Fetch sample data first.")
        else:
            raw=json.loads(rows.iloc[0]["source_payload"])
            invalid=copy.deepcopy(raw); invalid.pop("campaign_name",None); invalid["impressions"]=-1
            valid,rejected=transform_validate([
                {"source_platform":rows.iloc[0]["platform"],"payload":raw},
                {"source_platform":rows.iloc[0]["platform"],"payload":invalid},
            ],"tour-validation-example")
            left,right=st.columns(2)
            with left:
                st.markdown("**Valid · real simulator row**")
                st.json(valid[0] if valid else {})
            with right:
                st.markdown("**Rejected · intentionally malformed validation example**")
                st.json(rejected[0] if rejected else {})
            st.caption("The malformed example is derived from a real payload and modified for the validator demo. It is not represented as a provider response and is not inserted into rejected_records.")
    elif step == 3:
        st.markdown("### Parameterized insertion and UPSERT")
        st.code("INSERT INTO ad_performance(platform,campaign_id,campaign_name,report_date,spend,impressions,clicks,conversions,source_payload,created_at,updated_at)\nVALUES(?,?,?,?,?,?,?,?,?,?,?)\nON CONFLICT(platform,campaign_id,report_date) DO UPDATE SET\n  campaign_name=excluded.campaign_name,\n  spend=excluded.spend, impressions=excluded.impressions,\n  clicks=excluded.clicks, conversions=excluded.conversions,\n  source_payload=excluded.source_payload, updated_at=excluded.updated_at",language="sql")
        st.caption("This is the SQL executed by adpulse/db.py:load. Values are bound separately as parameters. Successful pipeline actions above executed this code against the application DB.")
    elif step == 4:
        st.markdown("### Commit a successful load; roll back a failing load")
        st.markdown('<div class="tour-flow"><span>BEGIN IMMEDIATE</span><b>→</b><span>UPSERT rows</span><b>→</b><strong>COMMIT on success</strong></div>',unsafe_allow_html=True)
        st.caption("The successful transaction is the real pipeline load. The next button calls the same db.load function against a temporary SQLite file to prove rollback without altering AdPulse data.")
        if st.button("Run isolated rollback proof",key="tour_storage_rollback",type="primary"):
            st.session_state["tour_rollback_result"]=_rollback_demo()
        outcome=st.session_state.get("tour_rollback_result")
        if outcome:
            st.code(f"BEGIN IMMEDIATE\nINSERT first row\nDATABASE ERROR: {outcome['error']}\nROLLBACK\nRows remaining in isolated DB: {outcome['rows_after_rollback']}",language="text")
            st.success("Rollback verified in an isolated SQLite database." if outcome["rolled_back"] else "Rollback proof did not match the expected result.")
    elif step == 5:
        st.markdown("### Actual rows in Database Explorer")
        rows=_records_for_date(report_date,10)
        if rows.empty: st.info("Fetch sample data in the first step.")
        else:
            st.dataframe(rows.drop(columns=["source_payload"]),use_container_width=True,hide_index=True)
            st.metric("Rows shown from application database",len(rows))
    else:
        st.markdown("### Implementation")
        st.code("# adpulse/etl.py · transform_validate()\nout = {'platform': platform, 'campaign_id': ..., 'campaign_name': ...,\n       'report_date': ..., 'spend': raw.get('spend'), ...}\n# validate required values and metric types\n# adpulse/db.py · load()\nc.execute('BEGIN IMMEDIATE')\n# parameterized INSERT ... ON CONFLICT ... DO UPDATE",language="python")
        st.caption("Relational tables store structured rows. Validation rejects malformed source records. Parameterized SQL binds values separately. Connection context commits when the transaction block succeeds and rolls back an exception.")
        st.markdown("**Code:** `adpulse/etl.py` · `transform_validate`; `adpulse/db.py` · `connect`, `load`; rejected source rows are written by `run_pipeline`.")


def _duplicate_step(step: int, report_date: str) -> None:
    if step == 0:
        count=_database_count(); dupes=_duplicate_key_count()
        st.markdown("### Initial application database state")
        c1,c2=st.columns(2); c1.metric("Actual ad_performance rows",count); c2.metric("Duplicate business-key groups",dupes)
        st.caption("Both values come from SELECT queries against the current SQLite file.")
    elif step == 1:
        st.markdown("### First extraction for this reporting date")
        before=_database_count(); st.metric("Rows before request",before)
        _run_action("duplicates","first","Run first extraction with original dataset",report_date)
        result=_result("duplicates","first")
        if result:
            st.metric("Rows after first run",_database_count())
            st.caption(f"Inserted {result['inserted']} · updated {result['updated']} for this run.")
    elif step == 2:
        st.markdown("### Fetch the exact same date and source again")
        if _result("duplicates","first") is None:
            st.info("Complete the first extraction step before repeating it.")
        else:
            before=_database_count(); st.metric("Before second run",before)
            _run_action("duplicates","repeat","Run identical extraction again",report_date)
            result=_result("duplicates","repeat")
            if result:
                c1,c2=st.columns(2); c1.metric("Before",result.get("db_count_before",before)); c2.metric("After",result.get("row_count_after",_database_count()))
                st.caption(f"Fetched {result['fetched']} · inserted {result['inserted']} · updated {result['updated']}. The same business keys were received again.")
    elif step == 3:
        st.markdown("### Composite unique key")
        st.code("UNIQUE(platform, campaign_id, report_date)",language="sql")
        st.caption("This means one platform’s campaign can have one stored metrics row for each reporting date.")
        with db.connect() as conn:
            columns=conn.execute("PRAGMA index_list(ad_performance)").fetchall()
        st.dataframe(pd.DataFrame([dict(r) for r in columns]),use_container_width=True,hide_index=True)
    elif step == 4:
        st.markdown("### Actual SQLite UPSERT statement")
        st.code("INSERT INTO ad_performance(..., spend, clicks, conversions, ...)\nVALUES(...)\nON CONFLICT(platform,campaign_id,report_date) DO UPDATE SET\n  campaign_name=excluded.campaign_name,\n  spend=excluded.spend,\n  impressions=excluded.impressions,\n  clicks=excluded.clicks,\n  conversions=excluded.conversions,\n  source_payload=excluded.source_payload,\n  updated_at=excluded.updated_at",language="sql")
        st.caption("The application’s actual SQL includes all current metric/source columns and uses bound `?` parameters. `excluded` means the incoming INSERT values.")
    elif step == 5:
        st.markdown("### Prove the row count stays stable")
        first=_result("duplicates","first"); repeat=_result("duplicates","repeat")
        if not first or not repeat:
            st.info("Run the first extraction and then the identical repeat in the previous steps.")
        else:
            before=first.get("row_count_after",first["db_count_before"]+first["inserted"])
            after=repeat.get("row_count_after",_database_count())
            c1,c2,c3,c4=st.columns(4)
            c1.metric("Before repeat",before); c2.metric("After repeat",after)
            c3.metric("New rows on repeat",repeat["inserted"]); c4.metric("Updated rows on repeat",repeat["updated"])
            st.metric("Duplicate business-key groups",_duplicate_key_count())
            st.success("No duplicate records created." if before==after and _duplicate_key_count()==0 else "Review the actual DB counts above.")
    elif step == 6:
        st.markdown("### Change metrics while preserving the unique key")
        with db.connect() as conn:
            old=conn.execute("SELECT platform,campaign_id,campaign_name,report_date,spend,clicks,conversions FROM ad_performance WHERE report_date=? AND campaign_id LIKE 'GO-%' ORDER BY campaign_id LIMIT 1",(report_date,)).fetchone()
        if old:
            st.write("Before update")
            st.json(dict(old))
        _run_action("duplicates","modified","Run revision 1 for the same reporting date",report_date,revision=1)
        changed=_result("duplicates","modified")
        if changed and old:
            with db.connect() as conn:
                new=conn.execute("SELECT platform,campaign_id,campaign_name,report_date,spend,clicks,conversions FROM ad_performance WHERE platform=? AND campaign_id=? AND report_date=?",(old["platform"],old["campaign_id"],old["report_date"])).fetchone()
            st.write("After update")
            st.json(dict(new))
            st.caption(f"Unique key unchanged. Inserted {changed['inserted']}; updated {changed['updated']}; current total rows {_database_count()}.")
        elif changed:
            st.info("Run the first extraction for this date to create a row for comparison.")
    else:
        st.markdown("### Implementation")
        st.code("# adpulse/db.py · init_db()\nUNIQUE(platform,campaign_id,report_date)\n\n# adpulse/db.py · load()\nprior = SELECT id WHERE platform=? AND campaign_id=? AND report_date=?\nINSERT ... ON CONFLICT(platform,campaign_id,report_date) DO UPDATE SET ...",language="sql")
        st.caption("Idempotency means re-fetching the same date does not grow the business-row set. Existing rows are updated, including `updated_at`; this code does not skip unchanged values. INSERT OR IGNORE would avoid duplicate inserts but would also discard corrected metrics.")
        st.markdown("**Code:** `adpulse/db.py` · `init_db`, `load`; business key is platform + campaign ID + report date.")


def render_tour() -> None:
    active=st.session_state.get("active_demo")
    if active not in (*DEMO_ORDER,"complete"):
        render_welcome()
        return
    if "tour_report_date" not in st.session_state:
        st.session_state["tour_report_date"]=date.today()-timedelta(days=1)
    report_date=st.date_input("Walkthrough reporting date",key="tour_report_date").isoformat()
    if active == "complete":
        sequence=[(demo_id,idx) for demo_id in DEMO_ORDER for idx in range(len(STEPS[demo_id]))]
        global_step=st.session_state.get("tour_step",0)
        if global_step >= len(sequence):
            st.markdown('<div class="tour-complete">',unsafe_allow_html=True)
            st.title("You have completed the AdPulse data pipeline walkthrough.")
            st.markdown("1. Collect data from APIs on a daily schedule.  \n2. Handle rate limits and failures through retries and error handling.  \n3. Transform, validate and persist records in a relational database.  \n4. Prevent duplicates with unique constraints and UPSERT.")
            st.button("Open Full Application",key="tour_open_application",type="primary",on_click=_exit)
            return
        demo_id,local_step=sequence[global_step]
        total=len(sequence)
        label=f"Complete Walkthrough · {DEMO_NAMES[demo_id]}"
    else:
        demo_id=active; local_step=st.session_state.get("tour_step",0)
        total=len(STEPS[demo_id]); global_step=local_step
        if st.session_state.get("tour_done"):
            st.title(f"{DEMO_NAMES[demo_id]} complete")
            st.success("You reached the end of this product demonstration.")
            c1,c2=st.columns(2)
            c1.button("Replay Demo",key="tour_finish_replay",on_click=_replay)
            c2.button("Exit to Application",key="tour_finish_exit",on_click=_exit,type="primary")
            return
        label=DEMO_NAMES[demo_id]
    st.markdown(f'<div class="tour-eyebrow">INTERACTIVE PRODUCT DEMO</div>',unsafe_allow_html=True)
    st.title(label)
    st.progress(min((global_step+1)/total,1.0),text=f"Step {global_step+1} of {total} · {STEPS[demo_id][local_step]}")
    paused=st.session_state.get("tour_paused",False)
    if paused: st.info("Walkthrough paused. Continue to enable step progression.")
    else: st.caption("Step-driven playback · pause at any time, inspect the live data, then continue.")
    with st.container(border=True):
        st.markdown(f"### {local_step+1:02d} · {STEPS[demo_id][local_step]}")
        _render_step(demo_id,local_step,report_date)
    st.divider()
    prev,next_col,pause_col,replay_col,exit_col=st.columns([1,1.2,1,1,1])
    prev.button("← Previous",key="tour_previous",disabled=(global_step==0 or paused),on_click=_previous)
    if active=="complete":
        if global_step+1>=total:
            next_col.button("Finish →",key="tour_next",disabled=paused,on_click=_next,type="primary")
        else:
            next_col.button("Next →",key="tour_next",disabled=paused,on_click=_next,type="primary")
    elif local_step+1>=total:
        def finish_demo(): st.session_state["tour_done"]=True
        next_col.button("Finish demo →",key="tour_finish",disabled=paused,on_click=finish_demo,type="primary")
    else:
        next_col.button("Next →",key="tour_next",disabled=paused,on_click=_next,type="primary")
    pause_col.button("Continue" if paused else "Pause",key="tour_pause",on_click=_toggle_pause)
    replay_col.button("Replay",key="tour_replay",on_click=_replay)
    exit_col.button("Exit",key="tour_exit",on_click=_exit)
