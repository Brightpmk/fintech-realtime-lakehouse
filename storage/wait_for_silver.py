"""Wait for Silver layer transactions to be populated in Trino/Iceberg."""

import json
import os
import sys
import time
import urllib.request


def main() -> None:
    url = os.environ.get("TRINO_STATEMENT_URL", "http://trino:8080/v1/statement")
    user = os.environ.get("TRINO_USER", "admin")
    timeout = int(os.environ.get("DBT_SILVER_WAIT_TIMEOUT_SECONDS", "1800"))
    start_time = time.time()

    def post(query: str) -> dict:
        req = urllib.request.Request(
            url,
            data=query.encode("utf-8"),
            headers={"X-Trino-User": user, "Content-Type": "text/plain"},
        )
        return json.loads(urllib.request.urlopen(req, timeout=30).read())

    print("Waiting for Silver rows...", flush=True)
    while True:
        if time.time() - start_time > timeout:
            print(
                f"Timed out waiting for Silver rows after {timeout} seconds.",
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)

        try:
            r = post("SELECT count(*) FROM iceberg.silver.transactions")
            uri = r.get("nextUri")
            while uri:
                r = json.loads(urllib.request.urlopen(uri, timeout=30).read())
                uri = r.get("nextUri")

            data = r.get("data")
            rows = data[0][0] if data else 0
            if rows and rows > 0:
                print(f"Silver ready: {rows} rows", flush=True)
                break
            print(
                f"Silver not ready yet ({rows} rows), retrying in 15s...",
                flush=True,
            )
        except Exception as e:
            print(f"Error querying Trino: {e}, retrying in 15s...", flush=True)

        time.sleep(15)


if __name__ == "__main__":
    main()
