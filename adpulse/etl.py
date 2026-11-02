"""Reusable extraction, transformation, validation and loading workflow."""
import email.utils, random, time, json, uuid
from datetime import datetime, timezone, date
import requests
from . import db
from .providers import PLATFORMS
BASE_URL='http://127.0.0.1:8765'
SLUGS={'Google Ads':'google','Meta Ads':'meta','LinkedIn Ads':'linkedin'}
RETRYABLE={429,500,502,503,504}
def utcnow(): return datetime.now(timezone.utc).isoformat(timespec='seconds')
def retry_after(value):
    if not value: return None
    try: return max(0,float(value))
    except ValueError:
        try: return max(0,email.utils.parsedate_to_datetime(value).timestamp()-time.time())
        except Exception: return None
def backoff(attempt,base,maximum): return min(base*(2**max(0,attempt-1)),maximum)*random.uniform(.75,1.25)
def extract(platform, report_date, run_id, config=None, revision=0, max_retries=3, base_delay=.35,max_delay=2,timeout=.65,page_size=4):
    slug=SLUGS[platform]; page=1; data=[]; all_events=[]; last_status=None; success=False
    while page:
        params={'report_date':report_date,'page':page,'page_size':page_size,'revision':revision}
        attempt=0
        while True:
            attempt+=1; started=utcnow(); t=time.monotonic(); endpoint=f'{BASE_URL}/{slug}/campaigns'
            try:
                response=requests.get(endpoint,params=params,timeout=timeout); duration=time.monotonic()-t; last_status=response.status_code
                event={'platform':platform,'page':page,'attempt':attempt,'status':response.status_code,'time':started,'duration':duration,'endpoint':response.url,'error':None,'delay':0}
                if response.ok:
                    payload=response.json(); rows,next_page=parse_response(slug,payload)
                    data.extend({'source_platform':platform,'payload':r} for r in rows)
                    db.log_request(run_id,platform,endpoint,page,attempt,response.status_code,'success',started,duration)
                    all_events.append(event); success=True; page=next_page; break
                error=f'HTTP {response.status_code}: {response.text[:160]}'; event['error']=error
                can_retry=response.status_code in RETRYABLE and attempt <= max_retries
                delay=retry_after(response.headers.get('Retry-After')) if response.status_code in (429,503) else None
                if can_retry:
                    delay=delay if delay is not None else backoff(attempt,base_delay,max_delay); event['delay']=delay; event['event']='retry'
                else: event['event']='failed'
                db.log_request(run_id,platform,endpoint,page,attempt,response.status_code,event['event'],started,duration,delay or 0,error)
                all_events.append(event)
                if can_retry: time.sleep(delay); continue
                return data,all_events,False,last_status
            except requests.Timeout as e:
                duration=time.monotonic()-t; error=f'Network timeout after {timeout:.2f}s'; last_status=None
                can_retry=attempt<=max_retries; delay=backoff(attempt,base_delay,max_delay) if can_retry else 0
                event={'platform':platform,'page':page,'attempt':attempt,'status':None,'time':started,'duration':duration,'endpoint':endpoint,'error':error,'delay':delay,'event':'retry' if can_retry else 'failed'}
                db.log_request(run_id,platform,endpoint,page,attempt,None,event['event'],started,duration,delay,error); all_events.append(event)
                if can_retry: time.sleep(delay); continue
                return data,all_events,False,last_status
            except requests.RequestException as e:
                duration=time.monotonic()-t; error=str(e); last_status=None
                db.log_request(run_id,platform,endpoint,page,attempt,None,'failed',started,duration,0,error)
                all_events.append({'platform':platform,'page':page,'attempt':attempt,'status':None,'time':started,'duration':duration,'endpoint':endpoint,'error':error,'delay':0,'event':'failed'})
                return data,all_events,False,last_status
    return data,all_events,success,last_status

def parse_response(slug,p):
    if slug=='google': return p.get('results',[]),p.get('next_page')
    if slug=='meta': return p.get('data',[]),p.get('paging',{}).get('next_page')
    pg=p.get('pagination',{}); return p.get('elements',[]),(pg.get('page',1)+1 if pg.get('page',1)<pg.get('total_pages',1) else None)
def transform_validate(items,run_id):
    valid=[]; rejected=[]
    for item in items:
        raw=item['payload']; platform=item['source_platform']; errors=[]
        out={'platform':platform,'campaign_id':str(raw.get('campaign_id','')).strip(),'campaign_name':str(raw.get('campaign_name','')).strip(),'report_date':str(raw.get('report_date','')),'spend':raw.get('spend'),'impressions':raw.get('impressions'),'clicks':raw.get('clicks'),'conversions':raw.get('conversions'),'source_payload':json.dumps(raw)}
        if not out['campaign_id']: errors.append('campaign_id is required')
        if not out['campaign_name']: errors.append('campaign_name is required')
        try: out['report_date']=date.fromisoformat(out['report_date']).isoformat()
        except Exception: errors.append('report_date must be an ISO date')
        try:
            out['spend']=float(out['spend'])
            if out['spend']<0: errors.append('spend must be non-negative')
        except (ValueError,TypeError): errors.append('spend must be numeric')
        for f in ('impressions','clicks','conversions'):
            v=out[f]
            if isinstance(v,bool) or not isinstance(v,int): errors.append(f'{f} must be an integer')
            elif v<0: errors.append(f'{f} must be non-negative')
        if errors: rejected.append({'run_id':run_id,'platform':platform,'report_date':raw.get('report_date'),'source_payload':json.dumps(raw),'validation_error':'; '.join(errors),'rejected_at':utcnow()})
        else: valid.append(out)
    return valid,rejected

def run_pipeline(report_date, revision=0, injection=None, max_retries=3, base_delay=.35,max_delay=2,timeout=.65, progress=None):
    run_id=str(uuid.uuid4()); started=utcnow(); started_m=time.monotonic(); db.insert_run(run_id,started)
    fetched=[]; events=[]; platform_results=[]; failures=[]
    try:
        for platform in PLATFORMS.values():
            platform_started=time.monotonic()
            if progress: progress(platform)
            if injection and injection.get('platform')==SLUGS[platform]:
                requests.post(f'{BASE_URL}/admin/injection',json=injection,timeout=2)
            else: requests.delete(f'{BASE_URL}/admin/injection/{SLUGS[platform]}',timeout=2)
            rows,ev,ok,status=extract(platform,report_date,run_id,injection,revision,max_retries,base_delay,max_delay,timeout)
            fetched.extend(rows); events.extend(ev)
            failed_attempts=sum(x['status']!=200 for x in ev)
            platform_results.append({'platform':platform,'ok':ok,'status':status,'records':len(rows),'attempts':len(ev),'failed':failed_attempts,'recovered':ok and failed_attempts>0,'duration':time.monotonic()-platform_started})
            with db.connect() as c:
                c.execute('INSERT INTO platform_run_stats(run_id,platform,status,records_fetched,attempts,failed_attempts,recoveries,last_http_status,duration_seconds) VALUES(?,?,?,?,?,?,?,?,?)',(run_id,platform,'Successful' if ok else ('Partial Success' if rows else 'Failed'),len(rows),len(ev),failed_attempts,int(ok and failed_attempts>0),status,time.monotonic()-platform_started))
            if not ok: failures.append(platform)
        valid,rejected=transform_validate(fetched,run_id)
        ins,upd=db.load(valid)
        with db.connect() as c:
            for r in rejected: c.execute('INSERT INTO rejected_records(run_id,platform,report_date,source_payload,validation_error,rejected_at) VALUES(?,?,?,?,?,?)',(r['run_id'],r['platform'],r['report_date'],r['source_payload'],r['validation_error'],r['rejected_at']))
        status='Failed' if len(failures)==3 else ('Partial Success' if failures else 'Successful')
        successes=sum(x['status']==200 for x in events); failed=sum(x['status']!=200 for x in events)
        duration=time.monotonic()-started_m
        db.finish_run(run_id,status=status,fetched=len(fetched),inserted=ins,updated=upd,rejected=len(rejected),successes=successes,failures=failed,duration=duration,summary='; '.join(failures))
        return {'run_id':run_id,'status':status,'fetched':len(fetched),'inserted':ins,'updated':upd,'rejected':len(rejected),'successes':successes,'failures':failed,'duration':duration,'platforms':platform_results,'events':events}
    except Exception as e:
        duration=time.monotonic()-started_m
        db.finish_run(run_id,status='Failed',fetched=len(fetched),inserted=0,updated=0,rejected=0,successes=0,failures=len(events),duration=duration,summary=str(e))
        raise
