"""
Imports HR data from D:\Onboarding into a local SQLite database (onboarding.db).

File types and schemas
──────────────────────
  employee_export_*              → employees      sheet: EmployeeInfo, headers row 3
                                                  col A (hidden) = bayzat_id
  *Payroll Configuration*        → payroll_config  sheet: DATA, headers row 1
                                                  col A (hidden) = bayzat_id
                                                  columns up to UAE Account Number / IBAN only
  *Custom Field Employee Values* → custom_fields  sheet: DATA, headers row 1
                                                  col A (hidden) = bayzat_id
  WPS / SIF files                → payroll_wps    raw import (no fixed schema)

Duplicate protection
────────────────────
  1. File-level  — _imported_files table; re-runs skip already-processed files.
  2. Row-level   — deduplication by bayzat_id > employee_id > first_name + last_name.

Company ID
──────────
  Parsed from the employee_export filename (UUID) and applied to ALL files
  found under the same company folder, not just the export file itself.

Usage
─────
  python import_to_db.py

  NOTE: Delete onboarding.db before running to clear any previously imported data.

Requirements
────────────
  pip install pandas openpyxl xlrd
"""

import os
import re
import sqlite3
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path

import pandas as pd

FILE_TIMEOUT = 60  # seconds per file before skipping

warnings.filterwarnings("ignore", message=".*Data validation extension.*", category=UserWarning)

# ── Config ────────────────────────────────────────────────────────────────────

METADATA_COLS     = {"company_name", "company_id", "source_file", "imported_at"}
PAYROLL_STOP_COLS = {"uae_account_number", "iban"}   # truncate payroll_config here


def _prompt_paths() -> tuple[Path, Path, Path]:
    """Ask the user for paths before the import starts."""
    print("\n── Data Consolidation Script ────────────────────")

    while True:
        raw = input("  Root folder (contains company subfolders): ").strip().strip('"')
        root = Path(raw)
        if root.is_dir():
            break
        print(f"  ! Directory not found: {raw}  — please try again.")

    default_db  = root / "onboarding.db"
    default_log = root / "import_log.txt"

    db_raw  = input(f"  Database file       [{default_db}]: ").strip().strip('"')
    log_raw = input(f"  Log file            [{default_log}]: ").strip().strip('"')

    db_path  = Path(db_raw)  if db_raw  else default_db
    log_path = Path(log_raw) if log_raw else default_log

    print("─────────────────────────────────────────────────\n")
    return root, db_path, log_path


# ── File routing ──────────────────────────────────────────────────────────────

def _match_table(filename: str) -> str | None:
    stem = Path(filename).stem.lower()
    name = filename.lower()
    if stem.startswith("employee_export_"):
        return "employees"
    if "payroll configuration" in name:
        return "payroll_config"
    if "custom field employee values" in name:
        return "custom_fields"
    if "wps" in name or "sif" in name:
        return "payroll_wps"
    return None


# ── Company ID pre-scan ───────────────────────────────────────────────────────

def _parse_company_id(filename: str) -> str | None:
    m = re.search(
        r"employee_export_\d{4}-\d{2}-\d{2}_"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        filename, re.IGNORECASE,
    )
    return m.group(1) if m else None


def _build_company_id_map(root_dir: Path) -> dict[str, str]:
    """
    Returns {company_folder_name: company_id}.
    Each company's ID is extracted from the first employee_export file found
    anywhere under that company's top-level folder.
    """
    cid_map: dict[str, str] = {}
    for dirpath, _, filenames in os.walk(root_dir):
        rel = Path(dirpath).relative_to(root_dir)
        if not rel.parts:
            continue
        company = rel.parts[0]
        if company in cid_map:
            continue
        for fn in filenames:
            if Path(fn).stem.lower().startswith("employee_export_"):
                cid = _parse_company_id(fn)
                if cid:
                    cid_map[company] = cid
                    break
    return cid_map


# ── Data cleaning ─────────────────────────────────────────────────────────────

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Drop fully-empty rows and columns; treat blank strings as missing."""
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df.reset_index(drop=True)


def _set_bayzat_col(df: pd.DataFrame) -> pd.DataFrame:
    """Rename column index 0 to bayzat_id (hidden column A in source files)."""
    if df.columns.size > 0 and df.columns[0] != "bayzat_id":
        cols = list(df.columns)
        cols[0] = "bayzat_id"
        df.columns = cols
    return df


def _truncate_at_column(df: pd.DataFrame, stop_cols: set[str]) -> pd.DataFrame:
    """Keep columns up to and including the first column whose name is in stop_cols."""
    for i, col in enumerate(df.columns):
        if col in stop_cols:
            return df.iloc[:, : i + 1]
    return df  # stop column not found — keep all columns


# ── File readers ──────────────────────────────────────────────────────────────

def _engine(path: Path) -> str:
    return "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"


def _read_employee_info(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path, sheet_name="EmployeeInfo", header=2, dtype=str, engine=_engine(path)
    )
    df = _normalize_columns(df)
    df = _clean(df)
    df = _set_bayzat_col(df)
    return df


def _read_payroll_config(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path, sheet_name="DATA", header=0, dtype=str, engine=_engine(path)
    )
    df = _normalize_columns(df)
    df = _clean(df)
    df = _set_bayzat_col(df)
    df = _truncate_at_column(df, PAYROLL_STOP_COLS)
    return df


def _read_custom_fields(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path, sheet_name="DATA", header=0, dtype=str, engine=_engine(path)
    )
    df = _normalize_columns(df)
    df = _clean(df)
    df = _set_bayzat_col(df)
    return df


def _read_wps(path: Path) -> list[pd.DataFrame]:
    """Raw import — no fixed schema."""
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        return [_clean(_normalize_columns(df))]
    xl = pd.ExcelFile(path, engine=_engine(path))
    return [
        _clean(_normalize_columns(pd.read_excel(xl, sheet_name=s, dtype=str)))
        for s in xl.sheet_names
    ]


def _read_file(path: Path, table: str) -> list[pd.DataFrame]:
    try:
        if table == "employees":
            return [_read_employee_info(path)]
        if table == "payroll_config":
            return [_read_payroll_config(path)]
        if table == "custom_fields":
            return [_read_custom_fields(path)]
        if table == "payroll_wps":
            return _read_wps(path)
    except Exception as e:
        raise RuntimeError(str(e)) from e
    return []


def _read_file_timed(path: Path, table: str) -> list[pd.DataFrame]:
    """Run _read_file with a timeout; raises RuntimeError if it exceeds FILE_TIMEOUT."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_read_file, path, table)
        try:
            return future.result(timeout=FILE_TIMEOUT)
        except FuturesTimeout:
            raise RuntimeError(f"Timed out after {FILE_TIMEOUT}s — file may be corrupted or too large")


# ── SQLite helpers ────────────────────────────────────────────────────────────

def _init_file_tracker(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _imported_files (
            source_file TEXT PRIMARY KEY,
            imported_at TEXT NOT NULL
        )
    """)
    conn.commit()


def _is_file_imported(conn: sqlite3.Connection, rel_path: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM _imported_files WHERE source_file = ?", (rel_path,)
    ).fetchone() is not None


def _mark_file_imported(conn: sqlite3.Connection, rel_path: str, imported_at: str):
    conn.execute(
        "INSERT OR REPLACE INTO _imported_files (source_file, imported_at) VALUES (?, ?)",
        (rel_path, imported_at),
    )
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection, table: str, df: pd.DataFrame):
    existing = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
    for col in df.columns:
        if col not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
    conn.commit()


# ── Deduplication ─────────────────────────────────────────────────────────────

def _dedup_key_cols(df: pd.DataFrame) -> list[str] | None:
    """
    Priority: bayzat_id → employee_id → first_name + last_name → None (all cols).
    """
    cols = set(df.columns)
    if "bayzat_id" in cols:
        return ["bayzat_id"]
    for c in ("employee_id", "emp_id"):
        if c in cols:
            return [c]
    name_cols: list[str] = []
    for c in ("first_name", "firstname"):
        if c in cols:
            name_cols.append(c)
            break
    for c in ("last_name", "lastname", "surname"):
        if c in cols:
            name_cols.append(c)
            break
    return name_cols if len(name_cols) == 2 else None


def _deduplicate(
    conn: sqlite3.Connection, table: str, df: pd.DataFrame
) -> tuple[pd.DataFrame, int]:
    """Remove rows already in the table. Returns (filtered_df, n_dupes_dropped)."""
    data_cols = [c for c in df.columns if c not in METADATA_COLS]
    if not data_cols:
        return df, 0

    key_cols = _dedup_key_cols(df)
    compare_cols = key_cols if key_cols else data_cols

    try:
        col_expr = ", ".join(f'"{c}"' for c in compare_cols if c in data_cols)
        if not col_expr:
            return df, 0
        existing = pd.read_sql(f'SELECT {col_expr} FROM "{table}"', conn, dtype=str)
        if existing.empty:
            return df, 0

        merged = df[compare_cols].merge(existing, on=compare_cols, how="left", indicator=True)
        mask   = (merged["_merge"] == "left_only").values
        result = df[mask].reset_index(drop=True)
        return result, int((~mask).sum())
    except Exception:
        return df, 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ROOT_DIR, DB_PATH, LOG_PATH = _prompt_paths()

    logging.basicConfig(
        filename=LOG_PATH, filemode="w", level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    conn        = sqlite3.connect(DB_PATH)
    imported_at = datetime.now().isoformat(timespec="seconds")
    _init_file_tracker(conn)

    logging.info("Pre-scanning folders for company IDs…")
    company_id_map = _build_company_id_map(ROOT_DIR)
    logging.info("Found company IDs for %d companies.", len(company_id_map))

    stats = {"files": 0, "rows": 0, "skipped": 0, "dupes": 0, "errors": 0}

    for dirpath, _dirs, filenames in os.walk(ROOT_DIR):
        rel          = Path(dirpath).relative_to(ROOT_DIR)
        parts        = rel.parts
        company_name = parts[0] if parts else ""
        company_id   = company_id_map.get(company_name)

        for filename in filenames:
            if Path(filename).suffix.lower() not in {".xlsx", ".xls", ".csv"}:
                continue

            table = _match_table(filename)
            if table is None:
                stats["skipped"] += 1
                continue

            filepath = Path(dirpath) / filename
            rel_path = str(filepath.relative_to(ROOT_DIR))

            if _is_file_imported(conn, rel_path):
                logging.info("SKIP (already imported)  %s", rel_path)
                stats["skipped"] += 1
                continue

            logging.info("READING  [%s]  %s", table, rel_path)
            try:
                sheets = _read_file_timed(filepath, table)
            except RuntimeError as e:
                logging.error("ERROR  %s — %s", rel_path, e)
                stats["errors"] += 1
                continue

            file_rows = 0
            for df in sheets:
                if df.empty:
                    continue

                df["company_name"] = company_name
                df["company_id"]   = company_id
                df["source_file"]  = rel_path
                df["imported_at"]  = imported_at

                table_exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,)
                ).fetchone()

                dupes = 0
                if table_exists:
                    df, dupes = _deduplicate(conn, table, df)
                    if dupes:
                        logging.info("DEDUP  %s — %d rows dropped", rel_path, dupes)
                        stats["dupes"] += dupes

                if df.empty:
                    continue

                try:
                    if not table_exists:
                        df.to_sql(table, conn, if_exists="replace", index=False)
                    else:
                        _ensure_columns(conn, table, df)
                        df.to_sql(table, conn, if_exists="append", index=False)
                    file_rows += len(df)
                except Exception as e:
                    logging.error("DB ERROR  %s — %s", rel_path, e)
                    stats["errors"] += 1
                    continue

            if file_rows:
                logging.info("OK  [%s]  %s  (%d rows)", table, rel_path, file_rows)
                stats["files"] += 1
                stats["rows"]  += file_rows

            _mark_file_imported(conn, rel_path, imported_at)

    conn.close()

    print("\n── Import complete ──────────────────────────────")
    print(f"  Files imported   : {stats['files']}")
    print(f"  Rows inserted    : {stats['rows']}")
    print(f"  Duplicate rows   : {stats['dupes']}  (dropped)")
    print(f"  Files skipped    : {stats['skipped']}  (no match or already imported)")
    print(f"  Errors           : {stats['errors']}")
    print(f"  Database         : {DB_PATH}")
    print(f"  Log              : {LOG_PATH}")
    print("─────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
