"""Local deterministic provider simulators used by the AdPulse demonstration."""
from datetime import date
import asyncio
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="AdPulse Simulated Ad APIs", version="1.0")
PLATFORMS = {"google": "Google Ads", "meta": "Meta Ads", "linkedin": "LinkedIn Ads"}
NAMES = {
    "google": ["Search - Brand", "Search - Nonbrand", "Performance Max", "YouTube Awareness", "Shopping - Core", "Display Retargeting", "App Installs"],
    "meta": ["Prospecting - Broad", "Catalog Sales", "Reels Awareness", "Retargeting - Site", "Lead Generation", "Lookalike - Buyers", "Video Views"],
    "linkedin": ["Enterprise Demand Gen", "Sponsored Content", "Lead Gen Forms", "ABM - Tier 1", "Hiring Campaign", "Thought Leadership", "Website Conversions"],
}
# In-memory injection state is deliberately local to this demo process.
INJECTIONS: dict[str, dict] = {}

@app.get('/health')
def health(): return {"status": "ok", "data_label": "Simulated Demonstration Data"}

@app.post('/admin/injection')
async def configure_injection(config: dict):
    platform = config.get("platform")
    if platform not in PLATFORMS: raise HTTPException(400, "Unknown platform")
    INJECTIONS[platform] = {**config, "remaining": int(config.get("failures_before_recovery", 0)), "calls": 0}
    return {"configured": True, "platform": platform}

@app.delete('/admin/injection/{platform}')
def clear_injection(platform: str):
    INJECTIONS.pop(platform, None)
    return {"cleared": True}

def campaigns(platform: str, report_date: date, revision: int = 0):
    rows=[]
    for i, name in enumerate(NAMES[platform], 1):
        seed = sum(report_date.isoformat().encode()) % 97
        spend = round((145 + i*73 + seed*1.7 + (revision*23 if i == 1 else 0)), 2)
        rows.append({"campaign_id": f"{platform[:2].upper()}-{i:03d}", "campaign_name": name,
          "report_date": report_date.isoformat(), "spend": spend,
          "impressions": 12000 + i*4300 + seed*19 + (revision*900 if i == 1 else 0),
          "clicks": 180 + i*37 + seed%23 + (revision*11 if i == 1 else 0),
          "conversions": 9 + i*3 + seed%7 + (revision if i == 1 else 0)})
    return rows

@app.get('/{platform}/campaigns')
async def get_campaigns(platform: str, request: Request, report_date: date = Query(...), page: int = Query(1, ge=1), page_size: int = Query(4, ge=1, le=50), revision: int = Query(0, ge=0)):
    if platform not in PLATFORMS: raise HTTPException(404, "Unknown provider")
    # Failure controls are consumed by actual HTTP requests, allowing retry behaviour to be observed.
    cfg = INJECTIONS.get(platform)
    if cfg:
        cfg["calls"] += 1
        scenario = cfg.get("scenario", "200")
        if scenario == "timeout" and cfg["remaining"] > 0:
            cfg["remaining"] -= 1
            await asyncio.sleep(float(cfg.get("timeout_seconds", 2)))
        elif scenario != "200" and cfg["remaining"] > 0:
            cfg["remaining"] -= 1
            code = int(scenario) if scenario in {"429", "500", "503", "400", "403"} else 503
            headers = {"Retry-After": str(cfg.get("retry_after", "0.2"))} if code in (429,503) else {}
            return JSONResponse(status_code=code, content={"detail": f"Injected HTTP {code}", "simulated": True}, headers=headers)
        if scenario == "timeout" and cfg.get("failures_before_recovery",0) < cfg.get("calls",0):
            # once configured timeout failures were consumed, normal response resumes
            pass
    await asyncio.sleep(0.025)
    rows = campaigns(platform, report_date, revision)
    start=(page-1)*page_size; chunk=rows[start:start+page_size]; next_page=page+1 if start+page_size < len(rows) else None
    if platform == 'google': return {"results": chunk, "next_page": next_page, "data_label":"Simulated Demonstration Data"}
    if platform == 'meta': return {"data": chunk, "paging":{"next_page":next_page}, "data_label":"Simulated Demonstration Data"}
    return {"elements":chunk, "pagination":{"page":page,"total_pages":(len(rows)+page_size-1)//page_size}, "data_label":"Simulated Demonstration Data"}
