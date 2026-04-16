from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def _normalize_database_url(raw_url: str) -> str:
    url = raw_url.strip()

    # SQLAlchemy expects postgresql:// instead of postgres://
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]

    # Supabase Postgres requires SSL in most environments.
    if "supabase.co" in url and "sslmode=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}sslmode=require"

    return url


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return _normalize_database_url(database_url)

    supabase_db_url = os.getenv("SUPABASE_DB_URL")
    if supabase_db_url:
        return _normalize_database_url(supabase_db_url)

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT", "5432")

    faltando = [
        chave
        for chave, valor in {
            "DB_USER": db_user,
            "DB_PASSWORD": db_password,
            "DB_HOST": db_host,
            "DB_NAME": db_name,
        }.items()
        if not valor
    ]

    if faltando:
        supabase_user = os.getenv("SUPABASE_DB_USER")
        supabase_password = os.getenv("SUPABASE_DB_PASSWORD")
        supabase_host = os.getenv("SUPABASE_DB_HOST")
        supabase_name = os.getenv("SUPABASE_DB_NAME", "postgres")
        supabase_port = os.getenv("SUPABASE_DB_PORT", "5432")

        supabase_missing = [
            chave
            for chave, valor in {
                "SUPABASE_DB_USER": supabase_user,
                "SUPABASE_DB_PASSWORD": supabase_password,
                "SUPABASE_DB_HOST": supabase_host,
            }.items()
            if not valor
        ]

        if not supabase_missing:
            return _normalize_database_url(
                f"postgresql://{supabase_user}:{supabase_password}@{supabase_host}:{supabase_port}/{supabase_name}"
            )

        raise RuntimeError(
            "Variáveis de conexão ausentes. Configure uma das opções: "
            "DATABASE_URL, SUPABASE_DB_URL, DB_* ou SUPABASE_DB_*"
        )

    return _normalize_database_url(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def get_engine() -> Engine:
    database_url = get_database_url()

    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=30,
    )