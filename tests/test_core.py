from datetime import date, datetime, timezone
from email.utils import format_datetime
import pytest
from fastapi.testclient import TestClient
from adpulse import db
from adpulse.etl import parse_response, retry_after, transform_validate
from adpulse.providers import app, campaigns

client = TestClient(app)

@pytest.mark.parametrize(('slug','rows_key'), [('google','results'),('meta','data'),('linkedin','elements')])
def test_provider_pagination_and_determinism(slug, rows_key):
    first=client.get(f'/{slug}/campaigns',params={'report_date':'2026-11-01','page':1,'page_size':4})
    again=client.get(f'/{slug}/campaigns',params={'report_date':'2026-11-01','page':1,'page_size':4})
    assert first.status_code==200 and first.json()==again.json()
    assert len(first.json()[rows_key])==4
    assert 'Simulated Demonstration Data'==first.json()['data_label']
    rows,next_page=parse_response(slug,first.json())
    assert len(rows)==4 and next_page==2

def test_source_revision_changes_metrics_only_when_selected():
    baseline=campaigns('google',date(2026,11,1),0)
    same=campaigns('google',date(2026,11,1),0)
    revised=campaigns('google',date(2026,11,1),1)
    assert baseline==same
    assert baseline[0]['spend']!=revised[0]['spend']
    assert baseline[1:]==revised[1:]

def test_validation_preserves_rejected_payload():
    valid,rejected=transform_validate([
        {'source_platform':'Meta Ads','payload':{'campaign_id':'M-1','campaign_name':'Valid','report_date':'2026-11-01','spend':12.5,'impressions':20,'clicks':2,'conversions':1}},
        {'source_platform':'Meta Ads','payload':{'campaign_id':'','campaign_name':'Bad','report_date':'yesterday','spend':'bad','impressions':-2,'clicks':1.5,'conversions':0}},
    ],'run-1')
    assert len(valid)==1 and valid[0]['platform']=='Meta Ads'
    assert len(rejected)==1 and rejected[0]['run_id']=='run-1'
    assert 'campaign_id is required' in rejected[0]['validation_error']
    assert 'spend must be numeric' in rejected[0]['validation_error']
    assert 'impressions must be non-negative' in rejected[0]['validation_error']
    assert '"campaign_name": "Bad"' in rejected[0]['source_payload']

def test_retry_after_seconds_and_http_date():
    assert retry_after('2.5')==2.5
    future=datetime.now(timezone.utc).replace(microsecond=0)
    value=format_datetime(future,usegmt=True)
    assert retry_after(value) is not None
    assert retry_after('not a date') is None

def test_upsert_is_idempotent_and_updates(tmp_path,monkeypatch):
    monkeypatch.setattr(db,'DB_PATH',tmp_path/'test.sqlite3')
    db.init_db()
    rec={'platform':'Google Ads','campaign_id':'G-1','campaign_name':'Brand','report_date':'2026-11-01','spend':10.0,'impressions':100,'clicks':5,'conversions':1,'source_payload':'{}'}
    assert db.load([rec])==(1,0)
    assert db.load([rec])==(0,1)
    rec['spend']=12.0
    assert db.load([rec])==(0,1)
    with db.connect() as conn:
        row=conn.execute('select count(*),spend from ad_performance').fetchone()
        assert row[0]==1 and row[1]==12.0
