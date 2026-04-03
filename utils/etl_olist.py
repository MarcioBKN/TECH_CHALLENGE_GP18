"""
ETL padronizado — Olist (CSV) → SQLite.

- Tipos e descrições vêm de ``dicionario.xlsx`` (abas por entidade).
- Conexão: use ``with_connection`` de ``utils.db`` (sem repetir strings de conexão no notebook).
- Ordem de carga respeita chaves estrangeiras do modelo Olist.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from .db import with_connection

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
# CSVs e dicionário versionados em ``arquivos/`` e ``dicionario.xlsx`` na raiz do projeto
DEFAULT_ARCHIVE = _PROJECT_ROOT / "arquivos"
DEFAULT_DICIONARIO = _PROJECT_ROOT / "dicionario.xlsx"

# Nome do CSV → função que identifica a aba correta no Excel (evita depender de encoding)
def _resolve_sheet_for_csv(csv_name: str, sheet_names: list[str]) -> str:
    def pick(pred: Callable[[str], bool]) -> str:
        for s in sheet_names:
            if pred(s.lower()):
                return s
        raise KeyError(f"Nenhuma aba corresponde a {csv_name}: {sheet_names}")

    if csv_name == "olist_customers_dataset.csv":
        return pick(lambda z: "clientes" in z and "pedido" not in z)
    if csv_name == "olist_orders_dataset.csv":
        return pick(lambda z: z == "pedidos")
    if csv_name == "olist_order_items_dataset.csv":
        return pick(lambda z: "itens" in z)
    if csv_name == "olist_order_payments_dataset.csv":
        return pick(lambda z: "pagamentos" in z)
    if csv_name == "olist_order_reviews_dataset.csv":
        return pick(lambda z: "avalia" in z)
    if csv_name == "olist_products_dataset.csv":
        return pick(lambda z: "produtos" in z and "trad" not in z)
    if csv_name == "olist_sellers_dataset.csv":
        return pick(lambda z: "vendedores" in z)
    if csv_name == "olist_geolocation_dataset.csv":
        return pick(lambda z: "geolocal" in z)
    if csv_name == "product_category_name_translation.csv":
        return pick(lambda z: "trad" in z and "categor" in z)
    raise ValueError(f"CSV desconhecido: {csv_name}")

# Ordem de DROP (filhos antes dos pais) e de CREATE/INSERT (pais antes dos filhos)
TABLE_LOAD_ORDER: list[str] = [
    "olist_geolocation_dataset",
    "product_category_name_translation",
    "olist_customers_dataset",
    "olist_sellers_dataset",
    "olist_products_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
]

TABLE_TO_CSV: dict[str, str] = {
    "olist_geolocation_dataset": "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
    "olist_customers_dataset": "olist_customers_dataset.csv",
    "olist_sellers_dataset": "olist_sellers_dataset.csv",
    "olist_products_dataset": "olist_products_dataset.csv",
    "olist_orders_dataset": "olist_orders_dataset.csv",
    "olist_order_items_dataset": "olist_order_items_dataset.csv",
    "olist_order_payments_dataset": "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset": "olist_order_reviews_dataset.csv",
}

# Renomear colunas do CSV de produtos para alinhar ao dicionário (typos oficiais Olist)
RENAME_PRODUCTS = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}


def load_dicionario(path: Path) -> dict[str, pd.DataFrame]:
    """Lê todas as abas do dicionário (Campo, Tipo, Descrição)."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)
    xl = pd.ExcelFile(path)
    return {sheet: pd.read_excel(path, sheet_name=sheet) for sheet in xl.sheet_names}


def _tipo_to_pandas(serie: pd.Series, tipo: str) -> pd.Series:
    t = str(tipo).strip().lower()
    if t in ("string", "str", "text", "texto"):
        return serie.astype("string")
    if t in ("int", "integer", "inteiro"):
        return pd.to_numeric(serie, errors="coerce").astype("Int64")
    if t in ("float", "double", "decimal", "real"):
        return pd.to_numeric(serie, errors="coerce").astype("float64")
    if t in ("datetime", "date", "data", "timestamp"):
        return pd.to_datetime(serie, errors="coerce")
    return serie


def apply_dictionary_types(df: pd.DataFrame, spec: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica tipos declarados na aba do dicionário (colunas ``Campo`` e ``Tipo``).
    Mantém apenas colunas presentes no dicionário e na ordem do dicionário.
    """
    if "Campo" not in spec.columns or "Tipo" not in spec.columns:
        raise ValueError("Dicionário deve conter colunas 'Campo' e 'Tipo'.")

    out = df.copy()
    cols_order: list[str] = []
    for _, row in spec.iterrows():
        campo = str(row["Campo"]).strip()
        tipo = str(row["Tipo"]).strip()
        if campo not in out.columns:
            continue
        out[campo] = _tipo_to_pandas(out[campo], tipo)
        cols_order.append(campo)

    extra = [c for c in out.columns if c not in cols_order]
    if extra:
        out = out.drop(columns=extra)
    return out[cols_order]


def extract_csv(archive: Path, csv_name: str) -> pd.DataFrame:
    """Extract: leitura bruta do CSV."""
    p = Path(archive) / csv_name
    if not p.is_file():
        raise FileNotFoundError(p)
    return pd.read_csv(p, low_memory=False)


def transform_table(
    df: pd.DataFrame,
    csv_name: str,
    dicionario_por_aba: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Transform: padronização de nomes + tipos do dicionário."""
    if csv_name == "olist_products_dataset.csv":
        df = df.rename(columns=RENAME_PRODUCTS)

    sheet = _resolve_sheet_for_csv(csv_name, list(dicionario_por_aba.keys()))
    spec = dicionario_por_aba[sheet]
    return apply_dictionary_types(df, spec)


# --- DDL SQLite (integridade referencial alinhada ao diagrama Olist) ---

DDL_STATEMENTS: list[str] = [
    """
    CREATE TABLE olist_geolocation_dataset (
        geolocation_zip_code_prefix INTEGER NOT NULL,
        geolocation_lat REAL,
        geolocation_lng REAL,
        geolocation_city TEXT,
        geolocation_state TEXT
    );
    """,
    """
    CREATE TABLE product_category_name_translation (
        product_category_name TEXT NOT NULL PRIMARY KEY,
        product_category_name_english TEXT
    );
    """,
    """
    CREATE TABLE olist_customers_dataset (
        customer_id TEXT NOT NULL PRIMARY KEY,
        customer_unique_id TEXT,
        customer_zip_code_prefix INTEGER,
        customer_city TEXT,
        customer_state TEXT
    );
    """,
    """
    CREATE TABLE olist_sellers_dataset (
        seller_id TEXT NOT NULL PRIMARY KEY,
        seller_zip_code_prefix INTEGER,
        seller_city TEXT,
        seller_state TEXT
    );
    """,
    """
    CREATE TABLE olist_products_dataset (
        product_id TEXT NOT NULL PRIMARY KEY,
        product_category_name TEXT,
        product_name_length INTEGER,
        product_description_length INTEGER,
        product_photos_qty INTEGER,
        product_weight_g INTEGER,
        product_length_cm INTEGER,
        product_height_cm INTEGER,
        product_width_cm INTEGER
    );
    """,
    """
    CREATE TABLE olist_orders_dataset (
        order_id TEXT NOT NULL PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_status TEXT,
        order_purchase_timestamp TEXT,
        order_approved_at TEXT,
        order_delivered_carrier_date TEXT,
        order_delivered_customer_date TEXT,
        order_estimated_delivery_date TEXT,
        FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset (customer_id)
    );
    """,
    """
    CREATE TABLE olist_order_items_dataset (
        order_id TEXT NOT NULL,
        order_item_id INTEGER NOT NULL,
        product_id TEXT NOT NULL,
        seller_id TEXT NOT NULL,
        shipping_limit_date TEXT,
        price REAL,
        freight_value REAL,
        PRIMARY KEY (order_id, order_item_id),
        FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id),
        FOREIGN KEY (product_id) REFERENCES olist_products_dataset (product_id),
        FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset (seller_id)
    );
    """,
    """
    CREATE TABLE olist_order_payments_dataset (
        order_id TEXT NOT NULL,
        payment_sequential INTEGER NOT NULL,
        payment_type TEXT,
        payment_installments INTEGER,
        payment_value REAL,
        PRIMARY KEY (order_id, payment_sequential),
        FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id)
    );
    """,
    """
    CREATE TABLE olist_order_reviews_dataset (
        review_id TEXT NOT NULL PRIMARY KEY,
        order_id TEXT NOT NULL,
        review_score INTEGER,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date TEXT,
        review_answer_timestamp TEXT,
        FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id)
    );
    """,
]


def _datetime_cols_to_iso(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out


def _drop_all_tables(conn) -> None:
    for name in reversed(TABLE_LOAD_ORDER):
        conn.execute(f"DROP TABLE IF EXISTS {name};")


def _create_schema(conn) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    for stmt in DDL_STATEMENTS:
        conn.execute(stmt)


def _load_one_table(
    conn,
    archive: Path,
    csv_name: str,
    dicionario_por_aba: dict[str, pd.DataFrame],
) -> int:
    table = Path(csv_name).stem
    df = extract_csv(archive, csv_name)
    df = transform_table(df, csv_name, dicionario_por_aba)
    if csv_name == "olist_order_reviews_dataset.csv":
        # dataset público pode trazer review_id repetido
        df = df.drop_duplicates(subset=["review_id"], keep="first")
    df = _datetime_cols_to_iso(df)
    # SQLite: object/string
    for c in df.columns:
        if df[c].dtype == "string":
            df[c] = df[c].astype(object)
    n = len(df)
    df.to_sql(table, conn, if_exists="append", index=False)
    return n


def run_olist_etl(
    archive: Path | None = None,
    dicionario: Path | None = None,
    *,
    dry_run: bool = False,
    log: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """
    Executa o pipeline completo: DROP → CREATE → carga na ordem correta.

    Retorna dicionário ``tabela -> número de linhas inseridas``.
    """
    archive = Path(archive or DEFAULT_ARCHIVE)
    dicionario_path = Path(dicionario or DEFAULT_DICIONARIO)

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg)

    if not archive.is_dir():
        raise FileNotFoundError(f"Pasta de CSVs não encontrada: {archive}")

    dic = load_dicionario(dicionario_path)

    csv_sequence = [TABLE_TO_CSV[t] for t in TABLE_LOAD_ORDER]

    counts: dict[str, int] = {}

    if dry_run:
        for csv_name in csv_sequence:
            df = extract_csv(archive, csv_name)
            df = transform_table(df, csv_name, dic)
            counts[Path(csv_name).stem] = len(df)
            _log(f"[dry-run] {csv_name}: {len(df)} linhas (tipos aplicados)")
        return counts

    with with_connection() as conn:
        conn.execute("PRAGMA foreign_keys = OFF;")  # DROP sem ordem estrita
        _drop_all_tables(conn)
        conn.commit()
        conn.execute("PRAGMA foreign_keys = ON;")
        _create_schema(conn)
        conn.commit()

        for csv_name in csv_sequence:
            table = Path(csv_name).stem
            n = _load_one_table(conn, archive, csv_name, dic)
            counts[table] = n
            conn.commit()
            _log(f"Carga: {table} — {n} linhas")

    return counts


def resumo_dicionario_markdown(dicionario_path: Path | None = None) -> str:
    """Texto para documentação (notebook / relatório)."""
    path = Path(dicionario_path or DEFAULT_DICIONARIO)
    dic = load_dicionario(path)
    lines = ["| Aba | Campos |", "|-----|--------|"]
    for aba, df in dic.items():
        n = len(df)
        lines.append(f"| {aba} | {n} |")
    return "\n".join(lines)
