import json
import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock,patch

import httpx
import pytest
import pytest_asyncio
from prisma import Prisma
from src.priority_queue import claim,finish_resolution,resolve_batch,local_cards,enqueue_names
from src.services.scryfall import ScryfallClient

URL=os.getenv('MTG_TEST_DATABASE_URL')
pytestmark=pytest.mark.skipif(not URL,reason='Set MTG_TEST_DATABASE_URL for isolated PostgreSQL tests')


@pytest_asyncio.fixture
async def database():
    schema='test_queue_'+uuid.uuid4().hex
    admin=Prisma(datasource={'url':URL+'?schema=public'})
    await admin.connect()
    await admin.execute_raw(f'CREATE SCHEMA "{schema}"')
    db=Prisma(datasource={'url':URL+'?schema='+schema})
    try:
        for table in ('user_collections','deck_cards'):
            await admin.execute_raw(f'CREATE TABLE "{schema}".{table} (LIKE public.{table} INCLUDING ALL)')
        await db.connect()
        assert (await db.query_raw('SELECT current_schema() AS name'))[0]['name']==schema
        # LIKE INCLUDING ALL renames unique indexes to *_idx; reproduce the
        # original production index name before exercising its migration.
        indexes=await db.query_raw("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=current_schema() AND tablename='user_collections'")
        for index in indexes:
            if index['indexdef'].endswith('(user_id, card_scryfall_id)') and index['indexname'] != 'user_collections_user_id_card_scryfall_id_key':
                await db.execute_raw('ALTER INDEX "'+index['indexname']+'" RENAME TO user_collections_user_id_card_scryfall_id_key')
        sql=(Path(__file__).parents[2]/'backend/prisma/bulk_import.sql').read_text()
        for statement in sql.split(';'):
            statement='\n'.join(l for l in statement.splitlines() if not l.strip().startswith('--')).strip()
            if statement and statement not in ('BEGIN','COMMIT'):
                await db.execute_raw(statement)
        yield db
    finally:
        if db.is_connected(): await db.disconnect()
        await admin.execute_raw(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.disconnect()


@pytest.mark.asyncio
async def test_restart_reclaims_expired_job_and_rejects_stale_writer(database):
    await enqueue_names(database,['Sol Ring','sol ring'])
    old=(await claim(database,'old'))[0]
    assert not await claim(database,'other')
    await database.execute_raw("UPDATE enrichment_jobs SET lease_until=NOW()-INTERVAL '1 second'")
    new=(await claim(database,'new'))[0]
    card={'id':'sol','name':'Sol Ring','set':'c21','collector_number':'263'}
    await finish_resolution(database,old,card)
    assert (await database.query_raw('SELECT card FROM enrichment_jobs'))[0]['card'] is None
    await finish_resolution(database,new,card)
    row=(await database.query_raw('SELECT status,phase,card FROM enrichment_jobs'))[0]
    assert row['status']=='queued' and row['phase']=='enrich' and row['card']['id']=='sol'


@pytest.mark.asyncio
async def test_resolution_merges_quantities_preserving_foil_and_other_editions(database):
    await enqueue_names(database,['Sol Ring'])
    job=(await claim(database,'token'))[0]
    for sid,foil,qty,key in [('pending:x',False,2,job['key']),('sol',False,3,None),('pending:x',True,4,job['key']),('different-edition',False,9,None)]:
        await database.execute_raw('''INSERT INTO user_collections(id,user_id,card_scryfall_id,card_name,quantity,is_foil,enrichment_key,updated_at)
            VALUES($1,'user',$2,'Sol Ring',$3,$4,$5,NOW())''',str(uuid.uuid4()),sid,qty,foil,key)
    await finish_resolution(database,job,{'id':'sol','name':'Sol Ring','set':'c21','collector_number':'263','type_line':'Artifact'})
    rows=await database.query_raw('SELECT card_scryfall_id,is_foil,quantity FROM user_collections ORDER BY card_scryfall_id,is_foil')
    assert rows==[{'card_scryfall_id':'different-edition','is_foil':False,'quantity':9},{'card_scryfall_id':'sol','is_foil':False,'quantity':5},{'card_scryfall_id':'sol','is_foil':True,'quantity':4}]
    await finish_resolution(database,job,{'id':'sol','name':'Sol Ring'})
    assert (await database.query_raw('SELECT sum(quantity)::int AS n FROM user_collections'))[0]['n']==18


@pytest.mark.asyncio
async def test_not_found_is_terminal_but_429_is_retryable(database):
    await enqueue_names(database,['Sol Ring'])
    jobs=await claim(database,'first')
    response=httpx.Response(429,request=httpx.Request('POST','https://api.scryfall.com/cards/collection'))
    with patch('src.priority_queue.request',AsyncMock(return_value=response)):
        await resolve_batch(database,jobs,ScryfallClient())
    assert (await database.query_raw('SELECT status FROM enrichment_jobs'))[0]['status']=='retry'
    await database.execute_raw('UPDATE enrichment_jobs SET next_attempt_at=NOW()')
    jobs=await claim(database,'next')
    response=httpx.Response(200,json={'data':[],'not_found':[{'name':'sol ring'}]},request=httpx.Request('POST','https://api.scryfall.com/cards/collection'))
    with patch('src.priority_queue.request',AsyncMock(return_value=response)):
        await resolve_batch(database,jobs,ScryfallClient())
    assert (await database.query_raw('SELECT status FROM enrichment_jobs'))[0]['status']=='not_found'


@pytest.mark.asyncio
async def test_bulk_name_collision_requires_identity_and_ignores_staging(database):
    from src.services.bulk_catalog import store_batch
    await database.execute_raw("INSERT INTO scryfall_bulk_state(kind,generation,source_type) VALUES('cards','active','all_cards')")
    for generation,sid,oracle,number in [('active','one','oracle1','1'),('active','two','oracle2','2'),('staging','three','oracle3','3')]:
        card={'id':sid,'name':'Goblin','set':'tst','collector_number':number,'lang':'en','oracle_id':oracle}
        await store_batch(database,generation,[{'id':sid,'name_key':'goblin','set_code':'tst','collector_number':number,'lang':'en','oracle_id':oracle,'payload':card}])
    cards,ambiguous=await local_cards(database,{'name':'goblin'})
    assert not cards and ambiguous
    cards,ambiguous=await local_cards(database,{'set':'tst','collector_number':'1'})
    assert cards[0]['id']=='one' and not ambiguous
    cards,_=await local_cards(database,{'id':'three'})
    assert not cards

@pytest.mark.asyncio
async def test_bulk_failure_keeps_old_generation_and_cleans_staging(database):
    import gzip
    from src.services.bulk_catalog import sync_snapshot
    await database.execute_raw("INSERT INTO scryfall_bulk_state(kind,generation,source_type) VALUES('cards','old','default_cards')")
    descriptor=httpx.Response(200,json={'data':[{'type':'default_cards','updated_at':'today','jsonl_download_uri':'https://data.scryfall.io/cards/test.jsonl.gz'}]},request=httpx.Request('GET','https://api.scryfall.com/bulk-data'))
    async def broken(url,path,headers):
        with gzip.open(path,'wt') as out:
            out.write('{broken json')
    with patch('src.services.bulk_catalog.request',AsyncMock(return_value=descriptor)),patch('src.services.bulk_catalog.download_archive',broken):
        with pytest.raises(json.JSONDecodeError):
            await sync_snapshot(database,'default_cards')
    state=(await database.query_raw("SELECT generation,lease_token,last_error FROM scryfall_bulk_state WHERE kind='cards'"))[0]
    assert state['generation']=='old' and state['lease_token'] is None and state['last_error']
    assert not await database.query_raw('SELECT id FROM scryfall_bulk_cards')


@pytest.mark.asyncio
async def test_bulk_publishes_only_complete_generation(database):
    import gzip
    from src.services.bulk_catalog import sync_snapshot
    descriptor=httpx.Response(200,json={'data':[{'type':'default_cards','updated_at':'today','jsonl_download_uri':'https://data.scryfall.io/cards/test.jsonl.gz'}]},request=httpx.Request('GET','https://api.scryfall.com/bulk-data'))
    async def archive(url,path,headers):
        with gzip.open(path,'wt') as out:
            for i in range(1000):
                out.write(json.dumps({'id':str(i),'name':'Sol Ring','set':'tst','collector_number':str(i),'lang':'en','oracle_id':'sol'})+'\n')
    with patch('src.services.bulk_catalog.request',AsyncMock(return_value=descriptor)),patch('src.services.bulk_catalog.download_archive',archive):
        await sync_snapshot(database,'default_cards')
    state=(await database.query_raw("SELECT generation,last_error FROM scryfall_bulk_state WHERE kind='cards'"))[0]
    assert state['generation'] and state['last_error'] is None
    cards,ambiguous=await local_cards(database,{'set':'tst','collector_number':'12'})
    assert not ambiguous and cards[0]['id']=='12'

@pytest.mark.asyncio
async def test_adopts_existing_pending_collection_rows(database):
    from src.priority_queue import recover_pending
    await database.execute_raw("""INSERT INTO user_collections(id,user_id,card_scryfall_id,card_name,quantity,updated_at)
        VALUES('legacy','user','pending:Sol Ring','Sol Ring',7,NOW())""")
    await recover_pending(database)
    job=(await claim(database,'lease'))[0]
    await finish_resolution(database,job,{'id':'sol','name':'Sol Ring','set':'c21','collector_number':'263'})
    rows=await database.query_raw('SELECT card_scryfall_id,quantity FROM user_collections')
    assert rows==[{'card_scryfall_id':'sol','quantity':7}]


@pytest.mark.asyncio
async def test_bulk_handles_duplicates_in_snapshot_gracefully(database):
    import gzip
    from src.services.bulk_catalog import sync_snapshot
    descriptor=httpx.Response(200,json={'data':[{'type':'rulings','updated_at':'today','jsonl_download_uri':'https://data.scryfall.io/rulings/test.jsonl.gz'}]},request=httpx.Request('GET','https://api.scryfall.com/bulk-data'))
    async def archive(url,path,headers):
        with gzip.open(path,'wt') as out:
            for i in range(1000):
                # Introduce duplicate rulings within the same batch
                out.write(json.dumps({'oracle_id':f'oracle_{i}','comment':f'Ruling {i}'})+'\n')
                if i % 10 == 0:
                    out.write(json.dumps({'oracle_id':f'oracle_{i}','comment':f'Ruling {i}'})+'\n')
    with patch('src.services.bulk_catalog.request',AsyncMock(return_value=descriptor)),patch('src.services.bulk_catalog.download_archive',archive):
        await sync_snapshot(database,'rulings')
    state=(await database.query_raw("SELECT generation,last_error FROM scryfall_bulk_state WHERE kind='rulings'"))[0]
    assert state['generation'] and state['last_error'] is None
    count=(await database.query_raw("SELECT count(*)::int AS n FROM scryfall_bulk_cards WHERE generation=$1",state['generation']))[0]['n']
    assert count == 1000

