"""Atomic, streamed Scryfall bulk snapshots. No application collection writes."""
import asyncio
import gzip
import hashlib
import json
import logging
import os
import tempfile
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from src.services.scryfall import ScryfallClient
from src.services.scryfall_transport import request

log = logging.getLogger('mtg_worker.bulk')


def name_key(name):
    return ' '.join(name.strip().lower().split())


def file_url(value):
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or parsed.hostname != 'data.scryfall.io' or parsed.username or parsed.password:
        raise ValueError('Bulk download must use https://data.scryfall.io')
    return value


async def download_archive(url,path,headers):
    async with httpx.AsyncClient(timeout=60,headers={**headers,'Accept-Encoding':'identity'},follow_redirects=False) as http:
        async with http.stream('GET',url) as response:
            response.raise_for_status()
            size=0
            with path.open('wb') as stream:
                async for chunk in response.aiter_bytes():
                    size+=len(chunk)
                    if size>1_000_000_000:
                        raise ValueError('Bulk archive exceeds 1 GB limit')
                    stream.write(chunk)


async def sync_snapshot(db, source_type):
    kind = 'rulings' if source_type=='rulings' else 'cards'
    token = str(uuid.uuid4())
    generation = token
    lease = await db.query_raw('''INSERT INTO scryfall_bulk_state(kind,lease_token,lease_until) VALUES($1,$2,NOW()+INTERVAL '120 seconds')
        ON CONFLICT(kind) DO UPDATE SET lease_token=$2,lease_until=NOW()+INTERVAL '120 seconds'
        WHERE scryfall_bulk_state.lease_until IS NULL OR scryfall_bulk_state.lease_until<NOW() RETURNING *''',kind,token)
    if not lease:
        return
    state=lease[0]
    async def heartbeat():
        while True:
            await asyncio.sleep(20)
            ok=await db.execute_raw("UPDATE scryfall_bulk_state SET lease_until=NOW()+INTERVAL '120 seconds' WHERE kind=$1 AND lease_token=$2",kind,token)
            if not ok:
                raise RuntimeError('Lost bulk snapshot lease')
    async def build():
        client=ScryfallClient()
        response=await request('GET',f'{client.base_url}/bulk-data',headers=client.headers,max_429_retries=0)
        response.raise_for_status()
        descriptor=next((r for r in response.json()['data'] if r['type']==source_type),None)
        if not descriptor:
            raise ValueError(f'Bulk descriptor missing: {source_type}')
        if state.get('source_updated_at')==descriptor['updated_at'] and state.get('source_type')==source_type and state.get('generation'):
            return
        url=file_url(descriptor.get('jsonl_download_uri') or descriptor.get('download_uri',''))
        if '.jsonl.gz' not in urlsplit(url).path:
            raise ValueError('Expected a gzip JSONL snapshot; active generation retained')
        # Only the current lease owner may clean abandoned staging generations.
        await db.execute_raw('''DELETE FROM scryfall_bulk_cards WHERE generation NOT IN
            (SELECT generation FROM scryfall_bulk_state WHERE generation IS NOT NULL)
            AND generation NOT IN (SELECT lease_token FROM scryfall_bulk_state WHERE lease_until>NOW() AND lease_token IS NOT NULL)''')
        seen=set()
        count=0
        with tempfile.TemporaryDirectory(prefix='mtg-bulk-') as directory:
            path=Path(directory)/'snapshot.jsonl.gz'
            await download_archive(url,path,client.headers)
            batch=[]
            with gzip.open(path,'rt') as stream:
                for line in stream:
                    card=json.loads(line)
                    if source_type=='rulings':
                        if not card.get('oracle_id') or not card.get('comment'):
                            raise ValueError('Invalid ruling in snapshot')
                        sid=hashlib.sha256(json.dumps(card,sort_keys=True).encode()).hexdigest()
                        if sid in seen:
                            continue
                        seen.add(sid)
                        entry={'id':sid,'name_key':'','set_code':'','collector_number':'','lang':'','oracle_id':card['oracle_id'],'payload':card}
                    else:
                        if not all(card.get(k) for k in ('id','name','set','collector_number','lang')):
                            raise ValueError('Invalid card in snapshot')
                        cid=card['id']
                        if cid in seen:
                            continue
                        seen.add(cid)
                        entry={'id':cid,'name_key':name_key(card['name']),'set_code':card['set'],
                               'collector_number':card['collector_number'],'lang':card['lang'],'oracle_id':card.get('oracle_id'),'payload':card}
                    batch.append(entry)
                    count+=1
                    if len(batch)>=500:
                        await store_batch(db,generation,batch)
                        batch=[]
                if batch:
                    await store_batch(db,generation,batch)
        actual=(await db.query_raw('SELECT count(*)::int AS n FROM scryfall_bulk_cards WHERE generation=$1',generation))[0]['n']
        if actual<1000 or actual != count:
            raise ValueError(f'Incomplete or duplicate bulk snapshot: {actual}/{count} records')
        active=await db.execute_raw('''UPDATE scryfall_bulk_state SET generation=$3,source_updated_at=$4,source_type=$5,
            updated_at=NOW(),last_error=NULL WHERE kind=$1 AND lease_token=$2''',kind,token,generation,descriptor['updated_at'],source_type)
        if not active:
            raise RuntimeError('Bulk lease lost before activation')
        # Retain the old generation until after the atomic pointer swap.
        if state.get('generation'):
            await db.execute_raw('DELETE FROM scryfall_bulk_cards WHERE generation=$1',state['generation'])
        log.info('Activated %s snapshot with %s records',source_type,count)
    beat,work=asyncio.create_task(heartbeat()),asyncio.create_task(build())
    try:
        done,_=await asyncio.wait([beat,work],return_when=asyncio.FIRST_COMPLETED)
        if beat in done:
            await beat
        await work
    except BaseException as error:
        work.cancel()
        await asyncio.gather(work,return_exceptions=True)
        await db.execute_raw('''DELETE FROM scryfall_bulk_cards WHERE generation=$1 AND NOT EXISTS
            (SELECT 1 FROM scryfall_bulk_state WHERE generation=$1)''',generation)
        await db.execute_raw('UPDATE scryfall_bulk_state SET last_error=$3 WHERE kind=$1 AND lease_token=$2',kind,token,str(error)[:1000])
        raise
    finally:
        beat.cancel()
        work.cancel()
        await asyncio.gather(beat,work,return_exceptions=True)
        await db.execute_raw('UPDATE scryfall_bulk_state SET lease_token=NULL,lease_until=NULL WHERE kind=$1 AND lease_token=$2',kind,token)


async def store_batch(db,generation,rows):
    await db.execute_raw('''INSERT INTO scryfall_bulk_cards(generation,id,name_key,set_code,collector_number,lang,oracle_id,payload)
        SELECT DISTINCT ON (id) $1,id,name_key,set_code,collector_number,lang,oracle_id,payload FROM jsonb_to_recordset($2::jsonb)
        AS x(id text,name_key text,set_code text,collector_number text,lang text,oracle_id text,payload jsonb)
        ON CONFLICT(generation,id) DO UPDATE SET payload=EXCLUDED.payload''',generation,json.dumps(rows))


async def cached_rulings(db,oracle_id):
    rows=await db.query_raw("""SELECT jsonb_agg(b.payload) FILTER (WHERE b.id IS NOT NULL) AS cards
        FROM scryfall_bulk_state s LEFT JOIN scryfall_bulk_cards b ON b.generation=s.generation AND b.oracle_id=$1
        WHERE s.kind='rulings' AND s.generation IS NOT NULL GROUP BY s.kind""",oracle_id)
    if not rows:
        return None
    value=rows[0]['cards']
    return (json.loads(value) if isinstance(value,str) else value) or []


async def cached_spanish(db,set_code,number):
    rows=await db.query_raw("""SELECT s.source_type,jsonb_agg(b.payload) FILTER (WHERE b.id IS NOT NULL) AS cards
        FROM scryfall_bulk_state s LEFT JOIN scryfall_bulk_cards b ON b.generation=s.generation
          AND b.set_code=$1 AND b.collector_number=$2 AND b.lang='es'
        WHERE s.kind='cards' AND s.generation IS NOT NULL GROUP BY s.source_type""",set_code,number)
    if not rows:
        return False,None
    cards=rows[0]['cards']
    cards=(json.loads(cards) if isinstance(cards,str) else cards) or []
    if len(cards)==1:
        return True,cards[0]
    return rows[0]['source_type']=='all_cards',None


async def bulk_scheduler(db):
    if os.getenv('SCRYFALL_BULK_ENABLED','true').lower() not in ('true','1','yes'):
        return
    source=os.getenv('SCRYFALL_BULK_TYPE','default_cards')
    if source not in ('default_cards','all_cards'):
        raise ValueError('SCRYFALL_BULK_TYPE must be default_cards or all_cards')
    while True:
        failed = False
        for kind in (source,'rulings'):
            try:
                await sync_snapshot(db,kind)
            except asyncio.CancelledError:
                raise
            except Exception:
                failed = True
                log.exception('Bulk sync failed; keeping previous snapshot')
        await asyncio.sleep(300 if failed else 21600)
