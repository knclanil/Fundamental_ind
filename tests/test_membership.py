import pytest
from database import DB
from membership import init,publish,parse

def csv(ids):
 return 'Symbol,ISIN Code,Company Name,Industry\n'+''.join(f'S{i},INE{i:09d},Company {i},Industry\n' for i in ids)
def test_history(tmp_path):
 d=DB(str(tmp_path/'test.db'));init(d);init(d)
 assert d.run('SELECT count(*) FROM dm_data_source')==[(32,)]
 iid,sid=d.run("SELECT index_id,id FROM dm_data_source WHERE expected=50 LIMIT 1")[0]
 assert publish(d,iid,sid,csv(range(5)),None)=='PUBLISHED_UNVERIFIED'
 assert publish(d,iid,sid,csv(range(5)),None)=='UNCHANGED'
 with pytest.raises(ValueError):publish(d,iid,sid,csv(range(4)),None)
 publish(d,iid,sid,csv(range(1,6)),None);publish(d,iid,sid,csv(range(5)),None)
 assert d.run('SELECT count(*) FROM vw_index_membership_removals')==[(2,)]
 assert d.run('SELECT count(*) FROM vw_index_membership_history')==[(7,)]
 assert d.run('SELECT count(*) FROM vw_index_constituents')==[(5,)]
 assert d.run('SELECT count(*) FROM misc_queue')==[(0,)]
 assert not d.run('PRAGMA foreign_key_check')
 assert d.run('SELECT count(*) FROM vw_company_factor_current')==[(5,)]

def test_preservation_and_staging(tmp_path):
 from fleet import seed
 d=DB(str(tmp_path/'existing.db'));d.init();seed(d,{'S0':('INE000000000','Industry','Company 0')});before=d.run('SELECT * FROM misc_queue');init(d)
 assert d.run('SELECT * FROM misc_queue')==before
 iid,sid=d.run('SELECT index_id,id FROM dm_data_source LIMIT 1')[0];publish(d,iid,sid,csv(range(5)))
 cid=d.run("SELECT id FROM dm_company WHERE symbol='S0'")[0][0]
 snap=d.run("INSERT INTO ft_index_snapshot(index_id,source_id,observed,sha,member_count,status) VALUES(?,?,'2099','x',4,'STAGING') RETURNING id",(iid,sid))[0][0]
 d.run('INSERT INTO ft_index_event VALUES(?,?,0)',(snap,cid));assert d.run('SELECT count(*) FROM vw_index_constituents')==[(5,)]
 iid2,sid2=d.run("SELECT index_id,id FROM dm_data_source WHERE index_id<>? AND expected=30 LIMIT 1",(iid,))[0];publish(d,iid2,sid2,csv(range(5)))
 assert d.run('SELECT factor_count FROM vw_company_factor_overlap WHERE company_id=?',(cid,))==[(2,)]
