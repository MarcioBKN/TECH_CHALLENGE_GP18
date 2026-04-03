"""Conexões SQLite centralizadas."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _PROJECT_ROOT / "database.db"


def get_connection(*, row_factory: type | None = None) -> sqlite3.Connection:
    """Abre uma nova conexão com o banco padrão do projeto."""
    conn = sqlite3.connect(DB_PATH)
    if row_factory is not None:
        conn.row_factory = row_factory
    return conn


@contextmanager
def with_connection(*, row_factory: type | None = None):
    """Context manager: abre a conexão, faz yield e fecha ao sair."""
    conn = get_connection(row_factory=row_factory)
    try:
        yield conn
    finally:
        conn.close()
