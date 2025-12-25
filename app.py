import os
import tempfile
from typing import Any, Dict, List, Optional
from pathlib import Path
from copy import copy

import psycopg
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import column_index_from_string
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import column_index_from_string, get_column_letter
from psycopg.types.json import Jsonb
import json


APP_DIR = Path(__file__).resolve().parent
INDEX_HTML_PATH = APP_DIR / "index.html"
XLSX_PATH = APP_DIR / "list_format.xlsx"

# ローカル用: 同フォルダに置ける
DB_URL_TXT = APP_DIR / "database_url.txt"
DOTENV_PATH = APP_DIR / ".env"

app = FastAPI()




import re

_FULLWIDTH_TRANS = str.maketrans("０１２３４５６７８９．－", "0123456789.-")

def _as_number_or_text(v):
    """
    v が数値っぽければ int/float にして返す。
    それ以外は元のまま（文字列なら文字列）で返す。
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v

    s = str(v).strip()
    if s == "":
        return None

    # 全角→半角、カンマ除去
    s2 = s.translate(_FULLWIDTH_TRANS).replace(",", "")

    # 例: "1,234" "12.5" "-3" に対応（% や 単位付きは対象外）
    if re.fullmatch(r"-?\d+", s2):
        try:
            return int(s2)
        except Exception:
            return s
    if re.fullmatch(r"-?\d+\.\d+", s2):
        try:
            return float(s2)
        except Exception:
            return s

    return s  # 数値じゃなさそうなら文字のまま


def _read_database_url_from_txt() -> Optional[str]:
    if not DB_URL_TXT.is_file():
        return None
    s = DB_URL_TXT.read_text(encoding="utf-8").strip()
    return s or None


def _read_database_url_from_dotenv() -> Optional[str]:
    """
    .env を最低限だけパースして DATABASE_URL を拾う（python-dotenv不要）
    形式例: DATABASE_URL=postgresql://...
    """
    if not DOTENV_PATH.is_file():
        return None

    for line in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip() == "DATABASE_URL":
            v = v.strip().strip('"').strip("'")
            return v or None
    return None


def get_database_url() -> Optional[str]:
    # 1) まず環境変数（Render は基本これ）
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # 2) ローカル用: database_url.txt
    url = _read_database_url_from_txt()
    if url:
        return url

    # 3) ローカル用: .env
    url = _read_database_url_from_dotenv()
    if url:
        return url

    return None


def db_url_or_die() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError(
            "DATABASE_URL が見つかりません。\n"
            "いずれかで設定してください:\n"
            "  (1) 環境変数 DATABASE_URL\n"
            "  (2) app.py と同フォルダに database_url.txt（1行でURL）\n"
            "  (3) app.py と同フォルダに .env（DATABASE_URL=...）\n"
        )
    return url

def _is_empty_row(d: dict) -> bool:
    if not isinstance(d, dict) or len(d) == 0:
        return True
    for v in d.values():
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True


def ensure_tables(conn: psycopg.Connection):
    conn.execute("""
    create table if not exists shop_master (
      row_no integer,
      payload jsonb not null,
      updated_at timestamptz default now()
    );
    """)
    conn.execute("create index if not exists shop_master_row_no_idx on shop_master(row_no);")

    conn.execute("""
    create table if not exists syoken (
      row_no integer,
      payload jsonb not null,
      updated_at timestamptz default now()
    );
    """)
    conn.execute("create index if not exists syoken_row_no_idx on syoken(row_no);")

    conn.execute("""
    create table if not exists schedule (
      row_no integer,
      payload jsonb not null,
      updated_at timestamptz default now()
    );
    """)
    conn.execute("create index if not exists schedule_row_no_idx on schedule(row_no);")

    # ★列定義（列名の配列）を保存
    conn.execute("""
    create table if not exists table_schema (
      target text not null,
      columns jsonb not null,
      updated_at timestamptz default now()
    );
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
    # 起動時に URL を確定して app.state に保持
    url = db_url_or_die()
    app.state.database_url = url

    with psycopg.connect(url) as conn:
        ensure_tables(conn)


def connect_db() -> psycopg.Connection:
    # 念のため startup 前でも落ち方が分かるように
    url = getattr(app.state, "database_url", None) or db_url_or_die()
    return psycopg.connect(url)


# ★ root は同フォルダの index.html を返す（static不要）
@app.get("/", response_class=HTMLResponse)
def root():
    if not INDEX_HTML_PATH.is_file():
        raise HTTPException(404, f"index.html not found next to app.py: {INDEX_HTML_PATH}")
    return FileResponse(str(INDEX_HTML_PATH), media_type="text/html; charset=utf-8")


ALLOWED_TARGETS = ("shop_master", "syoken", "schedule")

@app.get("/api/table/{target}")
def get_table(target: str):
    if target not in ALLOWED_TARGETS:
        raise HTTPException(400, f"target must be one of {ALLOWED_TARGETS}")

    with connect_db() as conn:
        ensure_tables(conn)

        rows = conn.execute(
            f"select payload from {target} order by row_no nulls last"
        ).fetchall()
        data_rows = [r[0] for r in rows]

        sch = conn.execute(
            "select columns from table_schema where target=%s order by updated_at desc limit 1",
            (target,),
        ).fetchone()
        columns = sch[0] if sch else []
        # 万一 text として返ってきた場合も復旧
        if isinstance(columns, str):
            try:
                columns = json.loads(columns)
            except Exception:
                columns = []


    return {"rows": data_rows, "columns": columns}




from typing import List, Dict, Any
from fastapi import Body

from typing import Any, Dict, List
from fastapi import Body, HTTPException

ALLOWED_TARGETS = ("shop_master", "syoken", "schedule")

@app.post("/api/save/{target}")
def save_table(target: str, payload: Any = Body(...)):
    if target not in ALLOWED_TARGETS:
        raise HTTPException(400, f"target must be one of {ALLOWED_TARGETS}")

    # 受け取り：配列 or {rows:[...], columns:[...]}
    if isinstance(payload, list):
        rows = payload
        columns = []
    elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        rows = payload["rows"]
        columns = payload.get("columns", [])
    else:
        raise HTTPException(422, "Invalid body. Expected a JSON array or {rows:[...]}")

    # columns が無い場合は rows から推定（キーの和集合）
    if not (isinstance(columns, list) and all(isinstance(c, str) for c in columns) and columns):
        seen = []
        seen_set = set()
        for r in rows:
            if isinstance(r, dict):
                for k in r.keys():
                    if k not in seen_set:
                        seen.append(k)
                        seen_set.add(k)
        columns = seen

    with connect_db() as conn:
        ensure_tables(conn)

        # ★列定義は target 単位で毎回入れ替え（ON CONFLICTを使わない）
        conn.execute("delete from table_schema where target=%s", (target,))
        conn.execute(
            "insert into table_schema (target, columns) values (%s, %s)",
            (target, Jsonb(columns)),
        )



        # データは全入れ替え
        conn.execute(f"delete from {target};")

        row_no = 1
        for row in rows:
            if isinstance(row, dict) and _is_empty_row(row):
                continue
            conn.execute(
                f"insert into {target} (row_no, payload) values (%s, %s)",
                (row_no, Jsonb(row)),
            )
            row_no += 1

        conn.commit()

    return {"ok": True, "saved": row_no - 1, "columns": columns}




def _payload_get(payload: Any, keys: List[str]) -> str:
    """payload(dict)から候補キーの最初に見つかった値を文字列で返す。"""
    if not isinstance(payload, dict):
        return ""
    for k in keys:
        if k in payload:
            v = payload.get(k)
            s = "" if v is None else str(v).strip()
            if s != "":
                return s
    return ""


def _to_int_or_none(s: str) -> Optional[int]:
    s = (s or "").strip()
    if s == "":
        return None
    try:
        s2 = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        return int(s2)
    except Exception:
        return None


from copy import copy

def _copy_row_style(ws, src_row: int, dst_row: int, max_col: int) -> None:
    ws.row_dimensions[dst_row].height = ws.row_dimensions[src_row].height
    for c in range(1, max_col + 1):
        src = ws.cell(row=src_row, column=c)
        dst = ws.cell(row=dst_row, column=c)

        # 結合セル途中(MergedCell)は触らない
        if isinstance(dst, MergedCell):
            continue

        dst._style = copy(src._style)
        dst.font = copy(src.font)
        dst.border = copy(src.border)
        dst.fill = copy(src.fill)
        dst.number_format = src.number_format
        dst.protection = copy(src.protection)
        dst.alignment = copy(src.alignment)

def _unmerge_intersecting(ws, min_row: int, max_row: int, min_col: int, max_col: int) -> None:
    """指定範囲に少しでもかぶる結合を解除"""
    for cr in list(ws.merged_cells.ranges):
        if not (cr.max_row < min_row or cr.min_row > max_row or cr.max_col < min_col or cr.min_col > max_col):
            ws.unmerge_cells(str(cr))


def _copy_merges_from_row(ws, src_row: int, dst_row: int, max_col: int) -> None:
    """
    src_row を含む結合範囲を、dst_row に「同じ相対位置」でコピーする。
    例）結合が 999-1001 行に跨っていても、同じ高さで dst_row 周辺に作り直す。
    """
    merges = list(ws.merged_cells.ranges)

    for cr in merges:
        # src_row を含み、かつ A..max_col にかぶる結合だけ対象
        if not (cr.min_row <= src_row <= cr.max_row):
            continue
        if cr.max_col < 1 or cr.min_col > max_col:
            continue

        height = cr.max_row - cr.min_row  # 0なら1行結合
        row_offset = src_row - cr.min_row

        dst_min_row = dst_row - row_offset
        dst_max_row = dst_min_row + height

        dst_min_col = max(1, cr.min_col)
        dst_max_col = min(max_col, cr.max_col)

        # 既存結合と衝突する可能性があるので、対象範囲の結合を解除してから作る
        _unmerge_intersecting(ws, dst_min_row, dst_max_row, dst_min_col, dst_max_col)

        rng = (
            f"{get_column_letter(dst_min_col)}{dst_min_row}:"
            f"{get_column_letter(dst_max_col)}{dst_max_row}"
        )
        ws.merge_cells(rng)



# ★ Excel: テンプレ(list_format.xlsx)に shop_master を貼り付けて返す
@app.get("/api/export-xlsx")
def export_xlsx(background_tasks: BackgroundTasks):
    if not XLSX_PATH.is_file():
        raise HTTPException(404, f"list_format.xlsx not found next to app.py: {XLSX_PATH}")

    # shop_master / schedule から取得
    with connect_db() as conn:
        ensure_tables(conn)
        db_rows = conn.execute(
            "select payload from shop_master order by row_no nulls last"
        ).fetchall()

        schedule_rows = conn.execute(
            "select payload from schedule order by row_no nulls last"
        ).fetchall()

    # ---- フィールド名（payload のキー候補） ----
    # ※ もしあなたのJSONキー名が違うなら、ここに追加/修正してください
    SHOP_CODE_KEYS = ["店番", "店番フィールド", "shop_code", "shop_cd", "店舗コード", "店コード", "店舗番号"]
    SHOP_NAME_KEYS = ["店名", "店名フィールド", "店舗名", "shop_name", "店舗"]
    B_ALL_KEYS = ["B全", "B全フィールド", "B_all", "B_ALL", "B1", "B1フィールド"]
    B2_KEYS = ["B2", "B2フィールド"]
    B3_KEYS = ["B3", "B3フィールド"]
    B4_KEYS = ["B4", "B4フィールド"]
    GROUP_NO_KEYS = ["グループ番", "グループ番フィールド", "group_no", "group_num", "グループ番号"]
    GROUP_NAME_KEYS = ["グループ", "グループフィールド", "group", "group_name"]

    # schedule 用
    SCHEDULE_TITLE_KEYS = ["タイトル", "タイトルフィールド", "title", "タイトル名"]
    SCHEDULE_START_KEYS = ["開始日", "開始日フィールド", "start_date", "start", "開始"]
    SCHEDULE_END_KEYS   = ["終了日", "終了日フィールド", "end_date", "end", "終了"]
    SCHEDULE_SIZE_KEYS  = ["サイズ", "サイズフィールド", "size"]

    # payload から「元の型のまま」拾う（数値/日付が入ってても潰さない）
    def _payload_get_any(payload: Any, keys: List[str]):
        if not isinstance(payload, dict):
            return None
        for k in keys:
            if k in payload:
                v = payload.get(k)
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                return v
        return None

    # 文字列の日付を date にする（Excelは内部で数値＝日付シリアルになる）
    def _to_excel_date_value(v):
        import datetime as _dt
        import re as _re

        if v is None:
            return None
        if isinstance(v, _dt.datetime):
            return v.date()
        if isinstance(v, _dt.date):
            return v
        if isinstance(v, (int, float)):
            # 既に数値ならそのまま（Excelシリアルを想定）
            return v

        s = str(v).strip()
        if s == "":
            return None

        # 全角数字→半角
        s2 = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))

        # 例: 2025年12月25日
        m = _re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日", s2)
        if m:
            try:
                return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                return s

        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d", "%Y%m%d"):
            try:
                return _dt.datetime.strptime(s2, fmt).date()
            except Exception:
                pass

        # "45678" みたいな数値文字列なら数値化（＝シリアルとして扱える）
        n = _as_number_or_text(s2)
        if isinstance(n, (int, float)):
            return n

        return s  # パースできないものは文字のまま

    # 並び替え＆グルーピング用に整形
    items = []
    for (payload,) in db_rows:
        payload = payload or {}
        shop_code_v = _payload_get(payload, SHOP_CODE_KEYS)
        shop_name_v = _payload_get(payload, SHOP_NAME_KEYS)
        b_all_v = _payload_get(payload, B_ALL_KEYS)
        b2_v = _payload_get(payload, B2_KEYS)
        b3_v = _payload_get(payload, B3_KEYS)
        b4_v = _payload_get(payload, B4_KEYS)
        group_no_v = _payload_get(payload, GROUP_NO_KEYS)
        group_name_v = _payload_get(payload, GROUP_NAME_KEYS)

        group_no_int = _to_int_or_none(group_no_v)
        shop_no_int = _to_int_or_none(shop_code_v)

        items.append({
            "shop_code": shop_code_v,
            "shop_name": shop_name_v,
            "b_all": b_all_v,
            "b2": b2_v,
            "b3": b3_v,
            "b4": b4_v,
            "group_no": group_no_v,
            "group_no_int": group_no_int,
            "group_name": group_name_v,
            "shop_no_int": shop_no_int,
        })

    # ★ グループ番が 0 のものは除外
    filtered = []
    for it in items:
        if it.get("group_no_int") == 0:
            continue
        if (it.get("group_no") or "").strip() in ("0", "０"):
            continue
        filtered.append(it)

    # 出力行（店舗行→最後にグループ行）を構築
    out_rows: List[Dict[str, str]] = []

    current_marker = None
    current_label = ""
    group_shop_rows: List[Dict[str, str]] = []

    def flush_group():
        nonlocal group_shop_rows, current_label
        if not group_shop_rows:
            return
        out_rows.extend(group_shop_rows)
        if (current_label or "").strip() != "":
            out_rows.append({"A": "G", "B": "", "C": current_label, "D": "", "E": "", "F": "", "G": ""})
        group_shop_rows = []

    for it in filtered:
        marker = it["group_no"] or it["group_name"] or ""
        label = it["group_name"] or it["group_no"] or ""

        if current_marker is None:
            current_marker = marker
            current_label = label
        elif marker != current_marker:
            flush_group()
            current_marker = marker
            current_label = label

        group_shop_rows.append({
            "A": "D",
            "B": it["shop_code"],
            "C": it["shop_name"],
            "D": it["b_all"],
            "E": it["b2"],
            "F": it["b3"],
            "G": it["b4"],
        })

    flush_group()

    def _set_value_safe(ws, row: int, col: int, value):
        """MergedCell には書かない。必要なら結合範囲の左上に書く。"""
        from openpyxl.cell.cell import MergedCell
        cell = ws.cell(row=row, column=col)
        if isinstance(cell, MergedCell):
            if value is None or value == "":
                return
            for cr in ws.merged_cells.ranges:
                if cr.min_row <= row <= cr.max_row and cr.min_col <= col <= cr.max_col:
                    ws.cell(row=cr.min_row, column=cr.min_col).value = value
                    return
            return
        cell.value = value

    # テンプレに貼り付け
    wb = load_workbook(str(XLSX_PATH))
    ws = wb.active

    # =========================
    # ★ schedule をヘッダへ書き込み
    #   H5=開始日 / J5=終了日 / H7=サイズ
    #   さらに H6=タイトル
    #   次は L5/N5/L6/L7 → 以降も 4列おき
    # =========================
    base_col = column_index_from_string("H")  # H=8
    step = 4                                  # H→L→P...
    row_start = 5
    row_title = 6
    row_size = 7
    max_col = column_index_from_string("BY")  # テンプレの右端想定

    sch_items = []
    for (payload,) in schedule_rows:
        payload = payload or {}
        title_v = _payload_get_any(payload, SCHEDULE_TITLE_KEYS)
        start_v = _payload_get_any(payload, SCHEDULE_START_KEYS)
        end_v   = _payload_get_any(payload, SCHEDULE_END_KEYS)
        size_v  = _payload_get_any(payload, SCHEDULE_SIZE_KEYS)

        if (str(title_v or "").strip() == "" and
            str(start_v or "").strip() == "" and
            str(end_v or "").strip() == "" and
            str(size_v or "").strip() == ""):
            continue

        sch_items.append({
            "title": "" if title_v is None else title_v,
            "start": start_v,
            "end": end_v,
            "size": "" if size_v is None else size_v,
        })

    for i, sch in enumerate(sch_items):
        col_h = base_col + step * i
        col_j = col_h + 2  # H→J / L→N

        if col_j > max_col:
            break

        # タイトル（H6, L6, ...)
        _set_value_safe(ws, row_title, col_h, "" if sch["title"] is None else str(sch["title"]).strip())

        # 開始日/終了日（Excel日付シリアル＝数値扱いになる）
        _set_value_safe(ws, row_start, col_h, _to_excel_date_value(sch["start"]))
        _set_value_safe(ws, row_start, col_j, _to_excel_date_value(sch["end"]))

        # サイズ
        _set_value_safe(ws, row_size, col_h, "" if sch["size"] is None else str(sch["size"]).strip())

    # ---- ここから明細（11行目開始） ----
    START_ROW = 11
    GROUP_STYLE_ROW = 1000
    GROUP_STYLE_MAX_COL = column_index_from_string("BY")  # 77

    for i, rr in enumerate(out_rows):
        r = START_ROW + i

        if rr.get("A") == "G":
            _unmerge_intersecting(ws, r, r, 1, GROUP_STYLE_MAX_COL)
            _copy_row_style(ws, GROUP_STYLE_ROW, r, max_col=GROUP_STYLE_MAX_COL)
            _copy_merges_from_row(ws, GROUP_STYLE_ROW, r, max_col=GROUP_STYLE_MAX_COL)

            _set_value_safe(ws, r, 1, "G")
            _set_value_safe(ws, r, 3, rr.get("C", ""))

        else:
            vals = [
                rr["A"],
                rr["B"],
                rr["C"],
                _as_number_or_text(rr["D"]),
                _as_number_or_text(rr["E"]),
                _as_number_or_text(rr["F"]),
                _as_number_or_text(rr["G"]),
            ]
            for c, v in enumerate(vals, start=1):
                _set_value_safe(ws, r, c, v)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    wb.save(tmp_path)

    background_tasks.add_task(lambda p: os.path.exists(p) and os.remove(p), tmp_path)

    return FileResponse(
        tmp_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="list_format.xlsx",
    )



if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
