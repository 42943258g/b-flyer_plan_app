import os
from typing import Any, Dict, List
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse


APP_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = APP_DIR / "index.html"
XLSX_PATH = APP_DIR / "list_format.xlsx"

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

app = FastAPI()


def ensure_tables(conn: psycopg.Connection):
    conn.execute("""
    create table if not exists shop_master (
      shop_code text primary key,
      payload jsonb not null,
      updated_at timestamptz default now()
    );
    """)
    conn.execute("""
    create table if not exists syoken (
      shop_code text primary key,
      payload jsonb not null,
      updated_at timestamptz default now()
    );
    """)

    # 行順保持用
    conn.execute("alter table shop_master add column if not exists row_no integer;")
    conn.execute("alter table syoken add column if not exists row_no integer;")
    conn.execute("create index if not exists shop_master_row_no_idx on shop_master(row_no);")
    conn.execute("create index if not exists syoken_row_no_idx on syoken(row_no);")

    # 既存 row_no が空なら埋める（初期は shop_code順で採番）
    conn.execute("""
    with t as (
      select shop_code, row_number() over(order by shop_code) as rn
      from shop_master
    )
    update shop_master s
      set row_no = t.rn
    from t
    where s.shop_code = t.shop_code and s.row_no is null;
    """)
    conn.execute("""
    with t as (
      select shop_code, row_number() over(order by shop_code) as rn
      from syoken
    )
    update syoken s
      set row_no = t.rn
    from t
    where s.shop_code = t.shop_code and s.row_no is null;
    """)

    conn.commit()


KEY_ALIASES = ["shop_code", "店番", "店舗コード", "店コード", "shop_cd", "店舗番号"]


def pick_key(row: Dict[str, Any], fallback: str) -> str:
    for k in KEY_ALIASES:
        v = row.get(k, "")
        v = "" if v is None else str(v).strip()
        if v:
            return v
    return fallback


def is_empty_row(row: Dict[str, Any]) -> bool:
    return all(str(v or "").strip() == "" for v in row.values())


@app.on_event("startup")
def startup():
    with psycopg.connect(DATABASE_URL) as conn:
        ensure_tables(conn)


# ★ root は同フォルダの index.html を返す（static不要）
@app.get("/", response_class=HTMLResponse)
def root():
    if not INDEX_HTML_PATH.is_file():
        raise HTTPException(404, f"index.html not found next to app.py: {INDEX_HTML_PATH}")
    return FileResponse(str(INDEX_HTML_PATH), media_type="text/html; charset=utf-8")


@app.get("/api/table/{target}")
def get_table(target: str, limit: int = 5000):
    if target not in ("shop_master", "syoken"):
        raise HTTPException(400, "target must be shop_master or syoken")

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_tables(conn)
        rows_db = conn.execute(
            f"select shop_code, row_no, payload from {target} "
            f"order by row_no nulls last, shop_code limit %s",
            (limit,),
        ).fetchall()

    rows: List[Dict[str, Any]] = []
    colset = set(["shop_code"])

    for shop_code, row_no, payload in rows_db:
        payload = payload or {}
        row = {"shop_code": shop_code, **payload}
        rows.append(row)
        colset.update(row.keys())

    columns = ["shop_code"] + sorted([c for c in colset if c != "shop_code"])
    return {"columns": columns, "rows": rows}


@app.post("/api/save/{target}")
def save_table(target: str, body: Dict[str, Any]):
    if target not in ("shop_master", "syoken"):
        raise HTTPException(400, "target must be shop_master or syoken")

    rows = body.get("rows") or []
    if not isinstance(rows, list):
        raise HTTPException(400, "rows must be a list")

    used_keys = set()

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_tables(conn)
        conn.execute(f"delete from {target}")

        saved = 0
        for i, r in enumerate(rows, start=1):
            if not isinstance(r, dict):
                continue
            if is_empty_row(r):
                continue

            key = pick_key(r, fallback=str(i))

            if key in used_keys:
                n = 2
                while f"{key}-{n}" in used_keys:
                    n += 1
                key = f"{key}-{n}"
            used_keys.add(key)

            payload = dict(r)
            payload.pop("shop_code", None)

            row_no = saved + 1
            conn.execute(
                f"""
                insert into {target} (shop_code, row_no, payload)
                values (%s, %s, %s::jsonb)
                """,
                (key, row_no, psycopg.types.json.Jsonb(payload)),
            )
            saved += 1

        conn.commit()

    return {"saved_rows": saved}


# ★ Excelは app.py と同フォルダの list_format.xlsx だけ返す
@app.get("/api/export-xlsx")
def export_xlsx():
    if not XLSX_PATH.is_file():
        raise HTTPException(404, f"list_format.xlsx not found next to app.py: {XLSX_PATH}")

    return FileResponse(
        str(XLSX_PATH),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="list_format.xlsx",
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
