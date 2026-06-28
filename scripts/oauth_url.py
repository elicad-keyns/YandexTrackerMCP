from __future__ import annotations

import os
from urllib.parse import urlencode


def main() -> None:
    client_id = os.getenv("YANDEX_CLIENT_ID", "").strip()
    if not client_id:
        raise SystemExit("Set YANDEX_CLIENT_ID before running this script.")
    query = urlencode({"response_type": "token", "client_id": client_id})
    print(f"https://oauth.yandex.ru/authorize?{query}")


if __name__ == "__main__":
    main()

