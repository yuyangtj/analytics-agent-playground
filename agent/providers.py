"""Provider registry: maps a provider name to how to reach it via the anthropic SDK.

Claude and Kimi (Moonshot AI's K2/K3 models) both speak the Anthropic Messages API,
so switching providers is just a base_url/api_key/model swap - no separate tool-use
loop needed.
"""

import os
from dataclasses import dataclass


@dataclass
class ProviderConfig:
    name: str
    api_key_env: str
    default_model: str
    base_url_env: str | None = None  # None means use the SDK default (api.anthropic.com)
    default_reasoning_effort: str | None = None  # only meaningful for Kimi k3/k3-256k models


PROVIDERS: dict[str, ProviderConfig] = {
    "claude": ProviderConfig(
        name="claude",
        api_key_env="ANTHROPIC_API_KEY",
        default_model="claude-sonnet-5",
        base_url_env=None,
    ),
    "kimi": ProviderConfig(
        name="kimi",
        api_key_env="KIMI_API_KEY",
        default_model="k3-256k",
        base_url_env="KIMI_BASE_URL",
        default_reasoning_effort="low",
    ),
}


class ProviderConfigError(RuntimeError):
    pass


def resolve(
    provider_name: str, model_override: str | None = None
) -> tuple[str, str | None, str, str | None]:
    """Returns (api_key, base_url, model, reasoning_effort) for the named provider, failing fast on missing env vars."""
    if provider_name not in PROVIDERS:
        raise ProviderConfigError(
            f"Unknown provider '{provider_name}'. Available: {', '.join(PROVIDERS)}"
        )
    cfg = PROVIDERS[provider_name]

    api_key = os.environ.get(cfg.api_key_env)
    if not api_key:
        raise ProviderConfigError(
            f"Provider '{provider_name}' requires env var {cfg.api_key_env} to be set."
        )

    base_url = None
    if cfg.base_url_env:
        base_url = os.environ.get(cfg.base_url_env)
        if not base_url:
            raise ProviderConfigError(
                f"Provider '{provider_name}' requires env var {cfg.base_url_env} to be set "
                "(the Anthropic-compatible endpoint URL)."
            )

    model = model_override or cfg.default_model
    return api_key, base_url, model, cfg.default_reasoning_effort
