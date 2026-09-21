"""Durable collection enrichment. One leased consumer; fenced, restartable jobs."""
from datetime import timedelta
import asyncio
import hashlib
import json
import logging
import time
import uuid

from src.services.scryfall_transport import request
from src.config import settings
from src.services.scryfall import ScryfallClient
from src.services.card_utils import is_playable_card
from src.worker import Worker

log = logging.getLogger('mtg_worker.priority')
MAX_ATTEMPTS = 8


def name_key(value):
    return ' '.join(value.strip().lower().split())


def identifier_key(identifier):
    return hashlib.sha256(json.dumps(identifier,sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def obj(value):
    return json.loads(value) if isinstance(value,str) else value


def matches(identifier, card):
    if 'id' in identifier:
        return identifier['id'] == card.get('id')
    if identifier.get('set') and identifier['set'].lower() != card.get('set','').lower():
        return False
    if 'collector_number' in identifier:
        return identifier['collector_number'] == card.get('collector_number')
    target = name_key(identifier['name'])
    return target == name_key(card.get('name','')) or any(target == name_key(f.get('name','')) for f in card.get('card_faces',[]))


async def enqueue_names(db,names):
    entries = {}
    for name in names:
        if name and name.strip():
            ident = {'name':name_key(name)}
            key = identifier_key(ident)
            entries[key] = {'key':key,'identifier':ident}
    ordered = sorted(entries.values(),key=lambda v:v['key'])
    for start in range(0,len(ordered),500):
        await db.execute_raw('''INSERT INTO enrichment_jobs(key,identifier)
            SELECT key,identifier FROM jsonb_to_recordset($1::jsonb) AS x(key text,identifier jsonb)
            ON CONFLICT(key) DO UPDATE SET
            status=CASE WHEN enrichment_jobs.updated_at<NOW()-INTERVAL '24 hours' AND enrichment_jobs.status<>'running' THEN 'queued' ELSE enrichment_jobs.status END,
            phase=CASE WHEN enrichment_jobs.updated_at<NOW()-INTERVAL '24 hours' AND enrichment_jobs.status<>'running' THEN 'resolve' ELSE enrichment_jobs.phase END,
            attempts=CASE WHEN enrichment_jobs.updated_at<NOW()-INTERVAL '24 hours' AND enrichment_jobs.status<>'running' THEN 0 ELSE enrichment_jobs.attempts END,
            next_attempt_at=LEAST(enrichment_jobs.next_attempt_at,NOW())''',json.dumps(ordered[start:start+500]))
    return len(entries)


async def recover_pending(db):
    rows = await db.query_raw("SELECT id,card_name,set_code,collector_number FROM user_collections WHERE enrichment_key IS NULL AND card_scryfall_id LIKE 'pending:%' ORDER BY id LIMIT 500")
    if rows:
        async with db.tx(timeout=timedelta(seconds=30)) as tx:
            for row in rows:
                if row['set_code'] and row['collector_number']:
                    identifier={'set':row['set_code'].lower(),'collector_number':row['collector_number']}
                else:
                    identifier={'name':name_key(row['card_name'])}
                    if row['set_code']:
                        identifier['set']=row['set_code'].lower()
                key=identifier_key(identifier)
                await tx.execute_raw("""INSERT INTO enrichment_jobs(key,identifier) VALUES($1,$2::jsonb)
                    ON CONFLICT(key) DO UPDATE SET status=CASE WHEN enrichment_jobs.status='done' THEN 'queued' ELSE enrichment_jobs.status END,
                    phase=CASE WHEN enrichment_jobs.status='done' THEN 'resolve' ELSE enrichment_jobs.phase END""",key,json.dumps(identifier))
                await tx.execute_raw('UPDATE user_collections SET enrichment_key=$2 WHERE id=$1 AND enrichment_key IS NULL',row['id'],key)
    decks=await db.query_raw("SELECT DISTINCT card_name FROM deck_cards WHERE card_scryfall_id LIKE 'pending:%' LIMIT 500")
    if decks:
        await enqueue_names(db,[r['card_name'] for r in decks])


async def local_cards(db,identifier):
    # Only the active complete generation is visible. Never pick an arbitrary
    # name collision; one oracle identity is required for name-only resolution.
    if 'id' in identifier:
        condition,args = 'b.id=$1',[identifier['id']]
    elif 'collector_number' in identifier:
        condition,args = "b.set_code=$1 AND b.collector_number=$2 AND b.lang='en'",[identifier['set'],identifier['collector_number']]
    else:
        condition,args = "b.name_key=$1 AND b.lang='en'",[name_key(identifier['name'])]
        if identifier.get('set'):
            condition += ' AND b.set_code=$2'
            args.append(identifier['set'])
    rows = await db.query_raw(f'''SELECT b.payload FROM scryfall_bulk_cards b JOIN scryfall_bulk_state s
        ON s.generation=b.generation AND s.kind='cards' WHERE {condition}''',*args)
    cards = [obj(r['payload']) for r in rows]
    cards = [c for c in cards if is_playable_card(c)]
    if len(cards) > 1 and 'name' in identifier:
        if len({c.get('oracle_id',c['id']) for c in cards}) > 1:
            return [],True
    if len(cards)>1 and 'collector_number' in identifier:
        return [],True
    return sorted(cards,key=lambda c:(c.get('released_at',''),c['id']),reverse=True)[:1],False


async def finish_resolution(db,job,card):
    key,token = job['key'],job['lease_token']
    async with db.tx(timeout=timedelta(seconds=30)) as tx:
        owned = await tx.query_raw("SELECT key FROM enrichment_jobs WHERE key=$1 AND lease_token=$2 AND status='running' AND lease_until>NOW() FOR UPDATE",key,token)
        if not owned:
            return
        identifier = obj(job['identifier'])
        if identifier.get('name'):
            await tx.execute_raw("""UPDATE user_collections SET enrichment_key=$1 WHERE enrichment_key IS NULL
                AND card_scryfall_id LIKE 'pending:%' AND lower(btrim(card_name))=$2
                AND (set_code IS NULL OR lower(set_code)=$3)""", key,identifier['name'],card.get('set'))
        # Legacy deck rows have no per-import mapping. Only unresolved rows for
        # this name and compatible set can be merged, never resolved editions.
        if identifier.get('name'):
            await tx.execute_raw("""WITH moved AS (
                DELETE FROM deck_cards WHERE card_scryfall_id LIKE 'pending:%' AND lower(btrim(card_name))=$1
                AND (set_code IS NULL OR lower(set_code)=$2) RETURNING *
            ) INSERT INTO deck_cards(id,deck_id,card_scryfall_id,card_name,quantity,assigned_quantity,is_sideboard,is_commander,mana_cost,type_line,set_code)
              SELECT min(id),deck_id,$3,$4,sum(quantity)::int,sum(assigned_quantity)::int,is_sideboard,bool_or(is_commander),$5,$6,$2
              FROM moved GROUP BY deck_id,is_sideboard
              ON CONFLICT(deck_id,card_scryfall_id,is_sideboard) DO UPDATE SET
              quantity=deck_cards.quantity+EXCLUDED.quantity,assigned_quantity=deck_cards.assigned_quantity+EXCLUDED.assigned_quantity,
              is_commander=deck_cards.is_commander OR EXCLUDED.is_commander,mana_cost=EXCLUDED.mana_cost,type_line=EXCLUDED.type_line""",
              identifier['name'],card.get('set'),card['id'],card['name'],card.get('mana_cost'),card.get('type_line'))
        # Merge only rows owned by this exact identifier; never rewrite every
        # printing of a name. Delete+upsert+job transition commit atomically.
        await tx.execute_raw('''WITH moved AS (
            DELETE FROM user_collections WHERE enrichment_key=$1 AND card_scryfall_id<>$2 RETURNING *
        ) INSERT INTO user_collections(id,user_id,card_scryfall_id,card_name,quantity,is_foil,set_code,collector_number,mana_cost,type_line,image_uri,enrichment_key,updated_at)
          SELECT min(id),user_id,$2,$3,sum(quantity)::int,is_foil,$4,$5,$6,$7,NULL,$1,NOW()
          FROM moved GROUP BY user_id,is_foil
          ON CONFLICT(user_id,card_scryfall_id,is_foil) DO UPDATE SET quantity=user_collections.quantity+EXCLUDED.quantity,
          mana_cost=EXCLUDED.mana_cost,type_line=EXCLUDED.type_line,set_code=EXCLUDED.set_code,
          collector_number=EXCLUDED.collector_number,enrichment_key=$1,updated_at=NOW()''',
          key,card['id'],card['name'],card.get('set'),card.get('collector_number'),card.get('mana_cost'),card.get('type_line'))
        await tx.execute_raw('''UPDATE user_collections SET card_name=$2,mana_cost=$3,type_line=$4,set_code=$5,collector_number=$6,updated_at=NOW()
            WHERE enrichment_key=$1''',key,card['name'],card.get('mana_cost'),card.get('type_line'),card.get('set'),card.get('collector_number'))
        await tx.execute_raw("UPDATE enrichment_jobs SET card=$3::jsonb,status='queued',phase='enrich',attempts=0,next_attempt_at=NOW(),lease_token=NULL,lease_until=NULL,updated_at=NOW(),last_error=NULL WHERE key=$1 AND lease_token=$2",key,token,json.dumps(card))


async def terminal(db,job,status,error=None):
    await db.execute_raw('''UPDATE enrichment_jobs SET status=$3,last_error=$4,lease_token=NULL,lease_until=NULL,updated_at=NOW()
        WHERE key=$1 AND lease_token=$2''',job['key'],job['lease_token'],status,error)


async def retry(db,job,error):
    attempts = job['attempts']
    status = 'failed' if attempts >= MAX_ATTEMPTS else 'retry'
    delay = min(30 * 2**min(attempts,6),1800)
    await db.execute_raw('''UPDATE enrichment_jobs SET status=$3,last_error=$4,next_attempt_at=NOW()+$5*INTERVAL '1 second',
        lease_token=NULL,lease_until=NULL,updated_at=NOW() WHERE key=$1 AND lease_token=$2''',
        job['key'],job['lease_token'],status,str(error)[:1000],delay)


async def resolve_batch(db,jobs,client):
    remote = []
    for job in jobs:
        try:
            cards,ambiguous = await local_cards(db,obj(job['identifier']))
            if ambiguous:
                await terminal(db,job,'ambiguous','Indica edición y número de coleccionista.')
            elif cards:
                await finish_resolution(db,job,cards[0])
            else:
                remote.append(job)
        except Exception as error:
            await retry(db,job,error)
    if not remote:
        return
    try:
        response = await request('POST',f'{client.base_url}/cards/collection',headers=client.headers,max_429_retries=0,
                                 json={'identifiers':[obj(j['identifier']) for j in remote]})
        response.raise_for_status()
        payload = response.json()
        for job in remote:
            try:
                candidates = list({c['id']:c for c in payload.get('data',[]) if matches(obj(job['identifier']),c) and is_playable_card(c)}.values())
                if len(candidates)==1:
                    await finish_resolution(db,job,candidates[0])
                elif candidates:
                    await terminal(db,job,'ambiguous','Más de una carta coincide con el identificador.')
                elif obj(job['identifier']) in payload.get('not_found',[]):
                    await terminal(db,job,'not_found','Scryfall no encontró esta carta.')
                else:
                    await retry(db,job,'Respuesta incompleta: falta el identificador solicitado.')
            except Exception as error:
                await retry(db,job,error)
    except Exception as error:
        for job in remote:
            await retry(db,job,error)


async def enrich_one(db,job,worker):
    try:
        card = obj(job['card'])
        result = await worker.download_priority_cards([],prepared_cards=[card],update_linked_cards=False,strict=True)
        if result['errors'] or result['downloaded'] != 1:
            raise RuntimeError('Enriquecimiento incompleto')
        images = await worker.image_storage.store_card_images(card['id'],ScryfallClient.extract_image_uris(card))
        card['_local_image'] = images.get('image_uri')
        async with db.tx(timeout=timedelta(seconds=30)) as tx:
            owned = await tx.query_raw("SELECT key FROM enrichment_jobs WHERE key=$1 AND lease_token=$2 AND status='running' AND lease_until>NOW() FOR UPDATE",job['key'],job['lease_token'])
            if not owned:
                return
            await tx.execute_raw('''UPDATE user_collections SET image_uri=COALESCE($2,image_uri),updated_at=NOW()
                WHERE card_scryfall_id=$1''',card['id'],card['_local_image'])
            await tx.execute_raw('''UPDATE deck_cards SET image_uri=COALESCE($2,image_uri),mana_cost=$3,type_line=$4
                WHERE card_scryfall_id=$1''',card['id'],card['_local_image'],card.get('mana_cost'),card.get('type_line'))
            await tx.execute_raw("UPDATE enrichment_jobs SET status='done',card=$3::jsonb,lease_until=NULL,lease_token=NULL,updated_at=NOW(),last_error=NULL WHERE key=$1 AND lease_token=$2",job['key'],job['lease_token'],json.dumps(card))
    except Exception as error:
        await retry(db,job,error)


async def claim(db,token):
    await db.execute_raw("UPDATE enrichment_jobs SET status='failed',lease_token=NULL,lease_until=NULL,last_error='Worker interrupted repeatedly',updated_at=NOW() WHERE status='running' AND lease_until<NOW() AND attempts>=$1",MAX_ATTEMPTS)
    return await db.query_raw("""WITH eligible AS (
        SELECT key,phase,next_attempt_at FROM enrichment_jobs WHERE
        (status IN ('queued','retry') AND next_attempt_at<=NOW()) OR (status='running' AND lease_until<NOW())
    ), batch_phase AS (
        SELECT phase FROM eligible ORDER BY CASE WHEN phase='resolve' THEN 0 ELSE 1 END LIMIT 1
    ), picked AS (
        SELECT j.key FROM enrichment_jobs j JOIN eligible e ON e.key=j.key
        WHERE j.phase=(SELECT phase FROM batch_phase)
        ORDER BY j.next_attempt_at,j.key
        LIMIT (SELECT CASE WHEN phase='resolve' THEN 75 ELSE $2 END FROM batch_phase)
        FOR UPDATE OF j SKIP LOCKED
    ) UPDATE enrichment_jobs j SET status='running',attempts=attempts+1,lease_token=$1,lease_until=NOW()+INTERVAL '120 seconds'
      FROM picked WHERE j.key=picked.key RETURNING j.*""",token,settings.ENRICH_CONCURRENCY)



async def consume(db):
    worker = Worker(db_client=db)
    worker.scryfall.max_429_retries = 0
    next_recovery = 0.0
    try:
        while True:
            token = str(uuid.uuid4())
            lease = await db.query_raw('''INSERT INTO scryfall_bulk_state(kind,lease_token,lease_until) VALUES('priority-consumer',$1,NOW()+INTERVAL '120 seconds')
                ON CONFLICT(kind) DO UPDATE SET lease_token=$1,lease_until=NOW()+INTERVAL '120 seconds'
                WHERE scryfall_bulk_state.lease_until<NOW() OR scryfall_bulk_state.lease_until IS NULL RETURNING kind''',token)
            if not lease:
                await asyncio.sleep(3)
                continue
            async def heartbeat():
                while True:
                    await asyncio.sleep(20)
                    renewed = await db.execute_raw("UPDATE scryfall_bulk_state SET lease_until=NOW()+INTERVAL '120 seconds' WHERE kind='priority-consumer' AND lease_token=$1",token)
                    if not renewed:
                        raise RuntimeError('Lost consumer lease')
                    await db.execute_raw("UPDATE enrichment_jobs SET lease_until=NOW()+INTERVAL '120 seconds' WHERE status='running' AND lease_token=$1",token)
            async def process():
                nonlocal next_recovery
                if time.monotonic() >= next_recovery:
                    await recover_pending(db)
                    next_recovery = time.monotonic() + 60
                jobs = await claim(db,token)
                resolves = [j for j in jobs if j['phase']=='resolve']
                if resolves:
                    await resolve_batch(db,resolves,worker.scryfall)
                enriches = [j for j in jobs if j['phase']=='enrich']
                if enriches:
                    await asyncio.gather(*(enrich_one(db,job,worker) for job in enriches))
                return bool(jobs)
            beat,work = asyncio.create_task(heartbeat()),asyncio.create_task(process())
            try:
                done,_ = await asyncio.wait([beat,work],return_when=asyncio.FIRST_COMPLETED)
                if beat in done:
                    await beat
                busy = await work
            finally:
                beat.cancel()
                work.cancel()
                await asyncio.gather(beat,work,return_exceptions=True)
                await db.execute_raw("UPDATE scryfall_bulk_state SET lease_until=NULL,lease_token=NULL WHERE kind='priority-consumer' AND lease_token=$1",token)
            if not busy:
                await asyncio.sleep(2)
    finally:
        await worker.close()


async def supervisor(db):
    while True:
        try:
            await consume(db)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('Priority consumer failed; durable jobs will be resumed')
            await asyncio.sleep(5)
