#!/usr/bin/env python3
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dnse import DNSEClient


def main():
    client = DNSEClient(
        api_key="replace-with-api-key",
        api_secret="replace-with-api-secret",
        base_url="https://openapi.dnse.com.vn",
    )

    status, body = client.get_trades(symbol="GAS", board_id="G1", from_date=1773282637, to_date=1773289837, limit = 100, order = "DESC", next_page_token=None, dry_run=False)
    print(status, body)


if __name__ == "__main__":
    main()
