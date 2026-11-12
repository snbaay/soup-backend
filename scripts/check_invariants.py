"""Re-run the SQL audit for a completed chaos run."""
import argparse
import json
import os
import sys

from scripts.chaos import audit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", required=True, help="prefix from the run_started event")
    parser.add_argument("--jobs", required=True, type=int)
    args = parser.parse_args()
    url = os.getenv("CHAOS_DATABASE_URL", "postgresql+psycopg://soup:soup@localhost:5434/soup_chaos")
    result = audit(url, args.prefix, args.jobs)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
