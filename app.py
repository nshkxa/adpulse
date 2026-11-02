import threading, time, os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import uvicorn
from datetime import date, timedelta
from adpulse import db
from adpulse.providers import app as api_app, PLATFORMS
from adpulse.etl import run_pipeline, extract, SLUGS, BASE_URL
FRESHNESS_DAYS=int(os.getenv('ADPULSE_FRESHNESS_DAYS','3'))

st.set_page_config(page_title='AdPulse | Reliable data. Clear decisions.',page_icon='A',layout='wide',initial_sidebar_state='expanded')

def start_api():
    if st.session_state.get('_api_started'): return
    def serve(): uvicorn.run(api_app,host='127.0.0.1',port=8765,log_level='error')
    t=threading.Thread(target=serve,daemon=True); t.start(); st.session_state['_api_started']=True
    for _ in range(30):
        try:
            if requests.get(BASE_URL+'/health',timeout=.2).ok: break
        except Exception: time.sleep(.1)
start_api(); db.init_db()
st.markdown('''<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Manrope:wght@500;600;700;800&display=swap');
:root{--ink:#142c45;--muted:#65778a;--teal:#16b8b0;--blue:#3478ed;--line:#e5ebef;--paper:#f5f7f6}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--ink)}.stApp{background:var(--paper)}
section[data-testid="stSidebar"]{background:#fff;border-right:1px solid var(--line)} section[data-testid="stSidebar"] h1{font-family:Manrope;color:var(--ink)!important;font-size:25px}
h1,h2,h3,div[data-testid="stMarkdownContainer"] h1,div[data-testid="stMarkdownContainer"] h2,div[data-testid="stMarkdownContainer"] h3{font-family:Manrope;color:var(--ink)!important}h1{font-size:30px!important;letter-spacing:-.6px}h2{font-size:21px!important}
section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,.stApp [data-testid="stWidgetLabel"] p{color:var(--ink)!important}
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p{color:var(--muted)!important}
[data-testid="stMetric"]{background:#fff;border:1px solid var(--line);border-radius:12px;padding:16px 18px;box-shadow:0 2px 8px #17324d08}
[data-testid="stMetricLabel"]{color:var(--muted)}[data-testid="stMetricValue"]{color:var(--ink);font-family:Manrope}
div.stButton>button{border-radius:9px;font-weight:600;border:1px solid #dbe5e9;min-height:42px}div.stButton>button[kind="primary"]{background:var(--teal);border-color:var(--teal);color:#062c35}
.card{background:white;border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin-bottom:12px}.eyebrow{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:700}.small{font-size:13px;color:var(--muted)}.pill{display:inline-block;padding:4px 9px;border-radius:20px;background:#e3f7f4;color:#087970;font-size:12px;font-weight:700}.banner{background:#e8f3ff;border:1px solid #d4e6fc;color:#244c78;border-radius:10px;padding:12px 16px;font-size:13px;margin:12px 0 22px}
</style>''',unsafe_allow_html=True)

st.sidebar.markdown('# AdPulse')
st.sidebar.caption('Reliable data. Clear decisions.')
st.sidebar.markdown('<div class="banner">SIMULATED DEMONSTRATION DATA<br>Local provider responses only. No live ad accounts.</div>',unsafe_allow_html=True)
page=st.sidebar.radio('Workspace',['Overview','Pipeline Operations','Failure Injection Lab','Database Explorer','Execution History','Marketing Analytics'],label_visibility='collapsed')
st.sidebar.markdown('---')
st.sidebar.caption('Provider service · local FastAPI\nSQLite · transactional UPSERT')

with db.connect() as c:
    perf=pd.read_sql_query('SELECT * FROM ad_performance',c)
    runs=pd.read_sql_query('SELECT * FROM pipeline_runs ORDER BY started_at DESC',c)
    logs=pd.read_sql_query('SELECT * FROM api_request_logs ORDER BY id DESC',c)
    rejects=pd.read_sql_query('SELECT * FROM rejected_records ORDER BY id DESC',c)
    platform_runs=pd.read_sql_query('SELECT * FROM platform_run_stats ORDER BY id DESC',c)

def money(v): return f'${v:,.2f}'
def header(title,sub):
    st.title(title); st.markdown(f'<div class="small">{sub}</div>',unsafe_allow_html=True)
def latest_status(platform):
    stats=platform_runs[platform_runs.platform==platform] if not platform_runs.empty else platform_runs
    if stats.empty:return 'Not Yet Synced',None
    latest=stats.iloc[0]
    return latest.status,latest

def kpis():
    spend=perf.spend.sum() if not perf.empty else 0
    vals=[('Total Ad Spend',money(spend)),('Total Clicks',f"{int(perf.clicks.sum()):,}" if not perf.empty else '0'),('Total Conversions',f"{int(perf.conversions.sum()):,}" if not perf.empty else '0'),('Total Impressions',f"{int(perf.impressions.sum()):,}" if not perf.empty else '0'),('Records Stored',f'{len(perf):,}'),('API Requests Succeeded',str(int(logs.event.eq('success').sum()))),('API Requests Failed',str(int(logs.event.ne('success').sum())))]
    for chunk in (vals[:3],vals[3:]):
        cols=st.columns(len(chunk))
        for col,(label,value) in zip(cols,chunk): col.metric(label,value)

def pipeline_controls(lab=False):
    c1,c2=st.columns([1,1]); chosen=c1.date_input('Reporting date',value=date.today()-timedelta(days=1),key='report_date'); revision=c2.selectbox('Source dataset',['Original dataset','Modified dataset · revision 1'],index=0,key='revision'); rev=1 if revision.startswith('Modified') else 0
    inj=None
    if lab:
        st.markdown('#### Failure scenario')
        a,b,c=st.columns(3); platform=a.selectbox('Platform to affect',list(SLUGS.values()),format_func=lambda x:SLUGS and {'google':'Google Ads','meta':'Meta Ads','linkedin':'LinkedIn Ads'}[x]); scenario=b.selectbox('Provider response',['HTTP 429 · Rate limit','HTTP 500 · Internal error','HTTP 503 · Unavailable','Network timeout','HTTP 200 · Success','HTTP 400 · Bad request','HTTP 403 · Forbidden']); failures=c.number_input('Failures before recovery',0,10,2)
        d,e,f=st.columns(3); retries=d.number_input('Maximum retries',0,8,3); base=e.number_input('Base backoff (seconds)',0.0,5.0,.25,.05); maximum=f.number_input('Maximum backoff (seconds)',.1,10.0,1.0,.1)
        status={'HTTP 429 · Rate limit':'429','HTTP 500 · Internal error':'500','HTTP 503 · Unavailable':'503','Network timeout':'timeout','HTTP 200 · Success':'200','HTTP 400 · Bad request':'400','HTTP 403 · Forbidden':'403'}[scenario]
        inj={'platform':platform,'scenario':status,'failures_before_recovery':int(failures),'retry_after':'0.2','timeout_seconds':2.0}
    if st.button('Run Pipeline' if not lab else 'Run Failure Scenario',type='primary',use_container_width=True,key='run_'+str(lab)):
        bar=st.progress(0,text='Preparing pipeline run…'); state=st.empty(); stages=['Google Ads','Meta Ads','LinkedIn Ads']
        def progress(name): bar.progress((stages.index(name)+1)/4,text=f'Extracting {name} · pagination and retries enabled')
        try:
            result=run_pipeline(chosen.isoformat(),rev,inj,int(retries) if lab else 3,float(base) if lab else .35,float(maximum) if lab else 2, .65, progress)
            bar.progress(1.0,text='Pipeline completed'); state.success(f"{result['status']} · {result['fetched']} fetched · {result['inserted']} inserted · {result['updated']} updated · {result['rejected']} rejected · {result['duration']:.2f}s")
            st.session_state['last_result']=result
        except Exception as e: state.error(f'Pipeline failed: {e}')
        st.rerun()
    result=st.session_state.get('last_result')
    if result:
        st.markdown('#### Latest execution summary')
        st.success(f"{result['status']} · {result['fetched']} fetched · {result['inserted']} inserted · {result['updated']} updated · {result['rejected']} rejected · {result['duration']:.2f}s · Run {result['run_id'][:8]}")
        st.dataframe(pd.DataFrame(result['platforms']),use_container_width=True,hide_index=True)
    return

if page=='Overview':
    header('Marketing data, under control','A live view of extraction health, data quality and consolidated campaign performance.')
    kpis(); st.markdown('<div class="banner"><b>Simulated Demonstration Data</b> · All platform responses are generated locally. Production integrations would replace these endpoints with authenticated provider connectors.</div>',unsafe_allow_html=True)
    st.subheader('Platform health')
    cols=st.columns(3)
    names=list(PLATFORMS.values())
    for col,p in zip(cols,names):
        status,log=latest_status(p); pperf=perf[perf.platform==p] if not perf.empty else perf; latestdate=pperf.report_date.max() if len(pperf) else '—'
        lastok=logs[(logs.platform==p)&(logs.event=='success')]; lastsync=lastok.iloc[0].started_at if len(lastok) else '—'
        with col: st.markdown(f'''<div class="card"><div class="eyebrow">{p}</div><h3 style="margin:8px 0">{status}</h3><div class="small">Last successful sync<br><b>{lastsync}</b></div><br><div class="small">Latest report date · <b>{latestdate}</b><br>Latest run records · <b>{int(log.records_fetched) if log is not None else 0}</b><br>Last HTTP status · <b>{log.last_http_status if log is not None and pd.notna(log.last_http_status) else '—'}</b><br>Sync duration · <b>{log.duration_seconds if log is not None else 0:.2f}s</b><br>Failed attempts · <b>{int(log.failed_attempts) if log is not None else 0}</b><br>Successful recoveries · <b>{int(log.recoveries) if log is not None else 0}</b><br>Freshness · <b>{'Fresh' if latestdate!='—' and latestdate>= (date.today()-timedelta(days=FRESHNESS_DAYS)).isoformat() else ('Stale' if latestdate!='—' else 'Never Synced')}</b></div></div>''',unsafe_allow_html=True)
    st.subheader('Pipeline control')
    pipeline_controls()
    st.subheader('Spend by reporting date')
    if not perf.empty:
        d=perf.groupby('report_date',as_index=False).spend.sum(); st.plotly_chart(px.area(d,x='report_date',y='spend',markers=True,color_discrete_sequence=['#16b8b0']),use_container_width=True)
    else: st.info('No campaign records yet. Run the pipeline to populate the dashboard.')
elif page=='Pipeline Operations':
    header('Pipeline Operations','Run the complete extraction → retry and recovery → transformation → validation → database load workflow.')
    pipeline_controls()
    if not runs.empty: st.dataframe(runs.head(15),use_container_width=True,hide_index=True)
elif page=='Failure Injection Lab':
    header('Failure Injection Lab','Inject provider-side failures and inspect the real HTTP attempts and recovery timeline.')
    st.markdown('<div class="banner">The simulator changes actual local GET responses. Temporary failures recover after the configured count. HTTP 400 and 403 are not retried.</div>',unsafe_allow_html=True)
    pipeline_controls(True)
    if not logs.empty:
        run=st.session_state.get('last_result',{}).get('run_id') or (runs.iloc[0]['run_id'] if not runs.empty else None); recent=logs[logs.run_id==run] if run else logs
        if not recent.empty:
            st.subheader('Observed request timeline')
            st.dataframe(recent.sort_values('id')[['started_at','platform','page','attempt','status_code','event','delay_seconds','duration_seconds','error']],use_container_width=True,hide_index=True)
            col=st.columns(4); col[0].metric('Attempts',len(recent)); col[1].metric('Failed attempts',int(recent.event.ne('success').sum())); col[2].metric('Successful attempts',int(recent.event.eq('success').sum())); col[3].metric('Final HTTP status',str(recent.iloc[0].status_code if len(recent) else '—'))
elif page=='Database Explorer':
    header('Database Explorer','Inspect persisted, deduplicated campaign records and rejected source payloads.')
    tab1,tab2=st.tabs(['Ad performance','Rejected records'])
    with tab1:
        if not perf.empty:
            platforms=st.multiselect('Filter platform',perf.platform.unique(),default=list(perf.platform.unique())); dates=st.date_input('Reporting date range',value=(date.fromisoformat(perf.report_date.min()),date.fromisoformat(perf.report_date.max())))
            view=perf[perf.platform.isin(platforms)]
            if isinstance(dates,tuple) and len(dates)==2:view=view[view.report_date.between(dates[0].isoformat(),dates[1].isoformat())]
            st.dataframe(view.drop(columns=['source_payload'],errors='ignore').sort_values(['report_date','platform','campaign_id'],ascending=[False,True,True]),use_container_width=True,hide_index=True)
        else: st.info('Database is empty. Run the pipeline to load records.')
    with tab2:
        if rejects.empty:st.info('No rejected records. Rejections are retained here with their original payload and reason.')
        else:st.dataframe(rejects,use_container_width=True,hide_index=True)
elif page=='Execution History':
    header('Execution History','Every run and HTTP attempt is persisted for traceability.')
    if not runs.empty:
        st.dataframe(runs,use_container_width=True,hide_index=True)
        selected=st.selectbox('Inspect run',runs.run_id.tolist())
        st.dataframe(logs[logs.run_id==selected],use_container_width=True,hide_index=True)
    else:st.info('No runs recorded yet.')
elif page=='Marketing Analytics':
    header('Marketing Analytics','Explore consolidated outcomes by platform, campaign and reporting date.')
    if not perf.empty:
        dates=sorted(perf.report_date.unique()); plats=st.multiselect('Platforms',sorted(perf.platform.unique()),default=sorted(perf.platform.unique())); view=perf[perf.platform.isin(plats)]
        a,b,c=st.columns(3); a.metric('Spend',money(view.spend.sum())); b.metric('Avg. CTR',f"{(view.clicks.sum()/view.impressions.sum()*100 if view.impressions.sum() else 0):.2f}%"); c.metric('Avg. CPA',money(view.spend.sum()/view.conversions.sum() if view.conversions.sum() else 0))
        x,y=st.columns(2); spend=view.groupby('platform',as_index=False).spend.sum(); x.plotly_chart(px.bar(spend,x='platform',y='spend',color='platform',color_discrete_sequence=['#16b8b0','#3478ed','#8bb9f7']),use_container_width=True)
        daily=view.groupby('report_date',as_index=False)[['spend','clicks','conversions']].sum(); y.plotly_chart(px.line(daily,x='report_date',y=['clicks','conversions'],markers=True),use_container_width=True)
        campaigns=view.groupby(['platform','campaign_name'],as_index=False)[['spend','impressions','clicks','conversions']].sum(); campaigns['CTR %']=campaigns.clicks/campaigns.impressions*100; campaigns['CPA']=campaigns.spend/campaigns.conversions.replace(0,pd.NA); st.dataframe(campaigns.sort_values('spend',ascending=False),use_container_width=True,hide_index=True)
    else:st.info('Analytics will appear after the first successful extraction.')
