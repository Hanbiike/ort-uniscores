"""Command-line entrypoint for HTML import into MySQL."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.database import build_engine
from etl.importer import HTMLImporter


def build_argument_parser() -> argparse.ArgumentParser:
    """Create CLI parser for import options."""
    parser = argparse.ArgumentParser(
        description=(
            "Parse downloaded admissions HTML pages and load normalized MySQL tables"
        )
    )
    parser.add_argument(
        "--site-root",
        type=Path,
        default=settings.site_root,
        help="Path to site mirror root (default: SITE_ROOT from .env)",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=settings.database_url,
        help="SQLAlchemy DB URL (default: DATABASE_URL from .env)",
    )
    return parser


def main() -> None:
    """Execute import run and print JSON summary."""
    parser = build_argument_parser()
    args = parser.parse_args()

    site_root = args.site_root.resolve()
    if not site_root.exists():
        raise SystemExit(f"Site root does not exist: {site_root}")

    engine = build_engine(args.db_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    with session_factory() as db:
        importer = HTMLImporter(db=db, site_root=site_root)
        stats = importer.import_all()

    print(json.dumps(asdict(stats), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
