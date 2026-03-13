from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT", "5432")

    faltando = [
        k for k, v in {
            "DB_USER": db_user,
            "DB_PASSWORD": db_password,
            "DB_HOST": db_host,
            "DB_NAME": db_name,
        }.items() if not v
    ]

    if faltando:
        raise RuntimeError("Variáveis de conexão ausentes: " + ", ".join(faltando))

    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


def get_engine() -> Engine:
    return create_engine(get_database_url(), pool_pre_ping=True)