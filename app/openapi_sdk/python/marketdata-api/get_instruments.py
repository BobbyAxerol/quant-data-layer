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

    status, body = client.get_instruments(symbol="SSI,SHS,ACB,AAA,VGI", market_id="", security_group_id="", index_name="", limit=100, page=1, dry_run=False)
    print(status, body)


if __name__ == "__main__":
    main()
