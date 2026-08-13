from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Annotated, Mapping

from fastapi import Header, HTTPException, Request

from qdl.security.policy import Permission, RbacAuthorizer, ServiceTokenVerifier


@dataclass(frozen=True, slots=True)
class ControlSecurityConfig:
    environment: str
    issuer: str
    audience: str
    keys_by_id: Mapping[str, str]
    algorithms: tuple[str, ...]

    @classmethod
    def from_environment(cls) -> "ControlSecurityConfig":
        try:
            keys = json.loads(os.environ["QDL_CONTROL_JWT_KEYS_JSON"])
            issuer = os.environ["QDL_CONTROL_JWT_ISSUER"]
            audience = os.environ["QDL_CONTROL_JWT_AUDIENCE"]
        except (KeyError, json.JSONDecodeError) as error:
            raise RuntimeError("control-plane identity configuration is incomplete") from error
        if not isinstance(keys, dict) or not keys:
            raise RuntimeError("QDL_CONTROL_JWT_KEYS_JSON must be a non-empty object")
        algorithms = tuple(
            item.strip()
            for item in os.environ.get("QDL_CONTROL_JWT_ALGORITHMS", "RS256,ES256").split(",")
            if item.strip()
        )
        return cls(
            environment=os.environ.get("QDL_ENVIRONMENT", "paper"),
            issuer=issuer,
            audience=audience,
            keys_by_id={str(key): str(value) for key, value in keys.items()},
            algorithms=algorithms,
        )


class ControlPlaneGuard:
    def __init__(self, config: ControlSecurityConfig):
        self._config = config
        self._verifier = ServiceTokenVerifier(
            issuer=config.issuer,
            audience=config.audience,
            keys_by_id=config.keys_by_id,
            algorithms=config.algorithms,
        )
        self._authorizer = RbacAuthorizer()

    async def __call__(
        self,
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="workload bearer token required")
        try:
            principal = self._verifier.verify(
                authorization.removeprefix("Bearer ").strip(),
                expected_environment=self._config.environment,
            )
            self._authorizer.require(
                principal,
                Permission.VENUE_OPERATE,
                environment=self._config.environment,
            )
        except PermissionError as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        request.state.qdl_principal = principal
        return principal
