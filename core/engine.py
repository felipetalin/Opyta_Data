from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_engine() -> Engine:
    """
    Prioridade:
    1) DATABASE_URL
    2) DB_USER / DB_PASSWORD / DB_HOST / DB_NAME / DB_PORT
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, pool_pre_ping=True)

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT", "5432")

    faltando = [k for k, v in {
        "DB_USER": db_user,
        "DB_PASSWORD": db_password,
        "DB_HOST": db_host,
        "DB_NAME": db_name,
    }.items() if not v]

    if faltando:
        raise RuntimeError(
            "Variáveis de conexão ausentes: " + ", ".join(faltando)
        )

    url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(url, pool_pre_ping=True)