#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
import uuid

import jwt


keys = json.loads(os.environ["QDL_BETA_JWT_KEYS_JSON"])
key_id, secret = sorted(keys.items())[0]
now = int(time.time())
print(jwt.encode({
    "sub": "spiffe://qdl/beta/phase7-capacity-binance",
    "iss": os.environ["QDL_BETA_JWT_ISSUER"],
    "aud": os.environ["QDL_BETA_JWT_AUDIENCE"],
    "iat": now,
    "nbf": now - 1,
    "exp": now + 300,
    "jti": str(uuid.uuid4()),
    "environment": "paper",
    "roles": ["market_data_reader", "historical_reader", "stream_consumer"],
    "consumer_manifest_revision": 1,
}, secret, algorithm="HS256", headers={"kid": key_id}))
