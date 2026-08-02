from __future__ import annotations

import argparse
import json
from pathlib import Path

from .bootstrap_corpus import DEFAULT_DB
from .evaluation import evaluate_all
from .storage import connect, migrate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    with connect(args.db) as connection:
        migrate(connection)
        result = evaluate_all(connection)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
