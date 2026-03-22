#!/usr/bin/env python3
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path('/root/.openclaw/workspace-coder/projects/LightFee')
EVENTS = ROOT / 'runtime' / 'live-events.jsonl'
STATE = ROOT / 'runtime' / 'live-state.json'
DB = ROOT / 'runtime' / 'cache' / 'live.sqlite3'


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def hour_bucket_end_ms(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    floored = dt.replace(minute=0, second=0, microsecond=0)
    return int((floored + timedelta(hours=1)).timestamp() * 1000)


def load_open_positions_count() -> int:
    if not STATE.exists():
        return 0
    try:
        data = json.loads(STATE.read_text(encoding='utf-8'))
    except Exception:
        return 0
    positions = data.get('open_positions') or data.get('positions') or []
    return len(positions)


def ensure_schema(conn: sqlite3.Connection):
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS hourly_closed_reconcile (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT,
          symbol TEXT,
          position_id TEXT NOT NULL,
          reason TEXT,
          close_event_ts_ms INTEGER NOT NULL,
          hour_bucket_end_ms INTEGER NOT NULL,
          net_quote REAL,
          reconciled_realized_price_pnl_quote REAL,
          reconciled_realized_exit_fee_quote REAL,
          payload_json TEXT NOT NULL,
          recorded_at_ms INTEGER NOT NULL,
          UNIQUE(position_id, close_event_ts_ms)
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS reconcile_job_checkpoint (
          job_name TEXT PRIMARY KEY,
          last_hour_bucket_end_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )
        '''
    )
    conn.commit()


def get_checkpoint(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT last_hour_bucket_end_ms FROM reconcile_job_checkpoint WHERE job_name=?",
        ('hourly_closed_reconcile',),
    ).fetchone()
    return int(row[0]) if row else 0


def set_checkpoint(conn: sqlite3.Connection, val: int):
    conn.execute(
        '''
        INSERT INTO reconcile_job_checkpoint(job_name, last_hour_bucket_end_ms, updated_at_ms)
        VALUES(?, ?, ?)
        ON CONFLICT(job_name) DO UPDATE SET
          last_hour_bucket_end_ms=excluded.last_hour_bucket_end_ms,
          updated_at_ms=excluded.updated_at_ms
        ''',
        ('hourly_closed_reconcile', val, now_ms()),
    )
    conn.commit()


def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    ensure_schema(conn)

    # 只在空闲期落库：当前无持仓
    if load_open_positions_count() > 0:
        print('SKIP: open positions > 0')
        return

    if not EVENTS.exists():
        print('SKIP: events file missing')
        return

    current_hour_end = hour_bucket_end_ms(now_ms())
    # 本次只处理“已完整结束的上一小时及之前”，不碰当前小时
    upper_bound = current_hour_end - 3600_000

    checkpoint = get_checkpoint(conn)

    inserted = 0
    max_bucket = checkpoint

    with EVENTS.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            if rec.get('kind') != 'exit.reconciled':
                continue

            ts_ms = rec.get('ts_ms')
            if not isinstance(ts_ms, int):
                continue

            hb = hour_bucket_end_ms(ts_ms)
            # 只入“上一小时及以前”，并基于 checkpoint 去重小时范围
            if hb <= checkpoint or hb > upper_bound:
                continue

            payload = rec.get('payload') or {}
            position_id = payload.get('position_id')
            if not position_id:
                continue

            conn.execute(
                '''
                INSERT OR IGNORE INTO hourly_closed_reconcile(
                  run_id, symbol, position_id, reason, close_event_ts_ms,
                  hour_bucket_end_ms, net_quote,
                  reconciled_realized_price_pnl_quote,
                  reconciled_realized_exit_fee_quote,
                  payload_json, recorded_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    rec.get('run_id'),
                    payload.get('symbol'),
                    position_id,
                    payload.get('reason'),
                    ts_ms,
                    hb,
                    payload.get('net_quote'),
                    payload.get('reconciled_realized_price_pnl_quote'),
                    payload.get('reconciled_realized_exit_fee_quote'),
                    json.dumps(payload, ensure_ascii=False),
                    now_ms(),
                ),
            )
            if conn.total_changes > inserted:
                inserted = conn.total_changes
            if hb > max_bucket:
                max_bucket = hb

    conn.commit()
    if max_bucket > checkpoint:
        set_checkpoint(conn, max_bucket)

    print(f'OK: inserted_total_changes={conn.total_changes}, checkpoint={max_bucket}')


if __name__ == '__main__':
    main()
