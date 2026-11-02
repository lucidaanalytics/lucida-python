"""Lucida server SDK."""


from ._ai_config import AiConfig, ai_config
from ._agent import AgentConversation
from ._build import BeforeSend, OnError
from ._context import (
    DISTINCT_ID_HEADER,
    SESSION_ID_HEADER,
    Context,
    client_ip,
    get_context,
    identify_context,
    new_context,
    read_tracing_headers,
    request_context_args,
    request_properties,
    scoped,
    set_context_session,
    tag,
)
from ._core import VERSION as __version__
from ._core import ImportEvent, LucidaError
from ._flagcache import FlagDefinitionCacheProvider, InMemoryFlagDefinitionCache
from ._flagcache_kv import RedisFlagDefinitionCache, RedisLike
from ._flags import FlagDetails, FlagValue
from ._snapshot import FlagSnapshot, device_scope, flag_event_props
from ._global import delegate as _delegate
from ._global import shutdown as shutdown
from .aio import AsyncLucida
from .client import Lucida

# Module-level facade: set these, then call lucida.capture(...) etc. (see README "Global client").
api_key: str | None = None
host: str | None = None
secret_key: str | None = None
debug: bool | None = None
disabled: bool | None = None
flush_at: int | None = None
flush_interval: float | None = None
max_batch_size: int | None = None
before_send: BeforeSend | None = None
on_error: OnError | None = None
sync_mode: bool | None = None
disable_geoip: bool | None = None
flag_timeout: float | None = None
default_properties: dict[str, object] | None = None
timeout: float | None = None

capture = _delegate("capture")
capture_immediate = _delegate("capture_immediate")
identify = _delegate("identify")
alias = _delegate("alias")
group_identify = _delegate("group_identify")
flush = _delegate("flush")
register = _delegate("register")
unregister = _delegate("unregister")
get_feature_flag = _delegate("get_feature_flag")
is_feature_enabled = _delegate("is_feature_enabled")
get_feature_flag_payload = _delegate("get_feature_flag_payload")
get_ai_config = _delegate("get_ai_config")
get_all_flags = _delegate("get_all_flags")
get_remote_config_payload = _delegate("get_remote_config_payload")
evaluate_flags = _delegate("evaluate_flags")

__all__: list[str] = [
    "AgentConversation", "DISTINCT_ID_HEADER", "SESSION_ID_HEADER", "AsyncLucida", "Context", "FlagDefinitionCacheProvider",
    "FlagDetails", "FlagValue", "ImportEvent", "InMemoryFlagDefinitionCache", "Lucida", "LucidaError", "RedisFlagDefinitionCache", "RedisLike", "__version__",
    "client_ip", "get_context", "identify_context", "new_context", "read_tracing_headers", "request_context_args", "request_properties",
    "scoped", "set_context_session", "tag", "shutdown", "FlagSnapshot", "device_scope", "flag_event_props",
    "AiConfig", "ai_config",
]  # fmt: skip
