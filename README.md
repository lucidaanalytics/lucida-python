# lucida (Python)

Server SDK for Lucida: sync and asyncio clients (httpx), Python >= 3.10, typed (`py.typed`, `mypy --strict` clean).
Pass a `distinct_id` to every call, or open a context (below) and pass `None`.

## Install

**Source install until the registry release** (the package is not on its registry yet):

```sh
pip install "git+https://github.com/lucidaanalytics/lucida-python.git"
```

The distribution is `lucida-analytics`; the import is `lucida`. Do not `pip install lucida`: that PyPI name is someone else's package.


```python
from lucida import Lucida

with Lucida("lc_pk_...", host="https://capture.lucidace.com", secret_key="lc_sk_...") as lc:
    lc.capture("u1", "purchase", {"total": 12}, groups={"company": "acme"})
    lc.identify("u1", {"plan": "pro"})
    lc.alias("u1", "anon-9")
    lc.group_identify("company", "acme", {"seats": 5})
    if lc.is_feature_enabled("new-checkout", "u1", person_properties={"plan": "pro"}):
        ...
# leaving the block flushes and stops the background flusher
```

```python
from lucida import AsyncLucida

async with AsyncLucida("lc_pk_...", secret_key="lc_sk_...") as lc:
    lc.capture("u1", "purchase")                      # non-blocking
    variant = await lc.get_feature_flag("exp", "u1")  # flag reads are coroutines
```

## Behaviour

- Batching: flush at 20 events or 5 s (`flush_at`, `flush_interval`), by a daemon thread (sync) or an asyncio task.
- Bodies over 1 KB are gzipped and sent with `?compression=gzip`.
- Retry (network error, 408, 429, 5xx): exponential backoff with jitter, honouring `Retry-After`; gives up after
  `max_retries` (5) and drops the batch. The queue is bounded (`max_queue_size`, 1000): the oldest events are dropped.
  `dropped_events` counts both.
- `flush(timeout=None)`, `shutdown(timeout=10)`, `disabled=True` (no events, no network), `debug=True` (logger `lucida`).
- `import_events([...])`: historical import via `POST /v1/s2s/batch` (needs `secret_key`, timestamps preserved,
  chunks of 1000, raises `LucidaError` on a rejected chunk).

## Contexts and request middleware

```python
from lucida import new_context, tag, identify_context, set_context_session, scoped

with new_context(distinct_id="u1", session_id="s1", properties={"svc": "api"}):
    tag("plan", "pro")
    lc.capture(None, "purchase")          # distinct id, $session_id and tags come from the context
    with new_context(fresh=True): ...      # fresh = do not inherit the parent; changes never leak out

@scoped                                    # also async functions; @scoped(fresh=True)
def job(): identify_context("u2"); lc.capture(None, "ran")
```

Context values are merged under the event's own properties; explicit arguments always win. Built on `contextvars`, so it
works in threads and asyncio (also `AsyncLucida`). `read_tracing_headers(get)` reads the browser SDK's
`X-Lucida-Distinct-Id` / `X-Lucida-Session-Id` (control chars stripped, trimmed, 1000 chars max; client-controlled, never
use for auth).

- ASGI (FastAPI, Starlette, Litestar): `app.add_middleware(LucidaASGIMiddleware, trust_proxy=True)` from `lucida.asgi`.
- Flask: `LucidaFlask(app)` or `LucidaFlask().init_app(app)` from `lucida.flask_integration` (Flask imported lazily).
- Django: `sdks/django`.

Both stamp `$current_url`, `$request_method`, `$request_path`, `$user_agent`, `$ip` (first `X-Forwarded-For` entry only with
`trust_proxy=True`) and accept `request_filter`, `extra_properties`, `capture_tracing_headers`.

## Delivery options

`before_send(event) -> event | None` (None drops; raising keeps the original), `on_error(exc, events)` when a batch is dropped,
`max_batch_size` (per request, separate from `flush_at`), `sync_mode=True` (post on the caller's thread, one attempt),
`capture_immediate(...)` (sync / awaitable; returns the uuid or None), `disable_geoip=True` or per call (sends
`$geoip_disable: true`), `register({...})` / `unregister(key)` super properties, `flag_timeout`, `flag_dedup_cache_size`.
Personless events: `capture(None, "e", process_person_profile=False)` sends `$process_person_profile: false` and a random
UUID distinct id when none is given.

## Global client

```python
import lucida
lucida.api_key = "lc_pk_..."        # also host, secret_key, debug, disabled, before_send, ...
lucida.capture("u1", "signed_up")   # builds the client on first call
lucida.shutdown()
```

## Flags

With a `secret_key` the client polls `GET /v1/flags/definitions` (ETag / `If-None-Match`, every `flag_poll_interval`
= 30 s; `lc.ready` is set after the first attempt, `reload_feature_flags()` forces one) and evaluates locally with the
shared evaluator (`testdata/flags`). Flags with cohort filters (`requires_server`) and unknown flags fall back to
`POST /v1/flags` unless `only_evaluate_locally=True` (then the result is `None`). Without a secret key every read is remote.

`get_feature_flag` emits `$feature_flag_called` + `$exposure` once per `(distinct_id, flag, value)` (bounded LRU of
10 000; `send_exposure=False` disables it). `get_feature_flag_payload`, `get_all_flags` and `get_all_flags_and_payloads` never emit.
`get_remote_config_payload(key)` (needs `secret_key`) reads a remote-config flag's payload, decrypted if it is stored encrypted.

### Definition cache provider

Share one definitions fetch between workers: implement `flag_definitions()`, `should_fetch_flag_definitions()`,
`on_flag_definitions_received(defs)`, `shutdown()` (`FlagDefinitionCacheProvider`) and pass `flag_definition_cache=`.
`InMemoryFlagDefinitionCache(ttl)` is the reference. `RedisFlagDefinitionCache(client, prefix=, ttl=, lock_ttl=)` is an optional adapter over
any client with `get(key)` and `set(key, value, ex=, nx=)` (redis-py, valkey, fakeredis); the SDK has no Redis dependency. The worker that wins the
`nx` lock fetches and publishes, the others read the stored copy:

```python
lc = Lucida("lc_pk_...", secret_key=sk, flag_definition_cache=RedisFlagDefinitionCache(redis.Redis.from_url(url)))
```

## Fallbacks and flag details

`Lucida(..., fallbacks={"new-checkout": False, "theme": "blue"})` (also `AsyncLucida`): when a flag cannot be evaluated
(network error, no definitions yet, quota limited, unknown key) the read returns the fallback with reason `"fallback"`;
a per-call `default=` applies only when the key has no fallback. Fallback reads send no `$feature_flag_called` / `$exposure`.
`get_feature_flag_details(key, distinct_id, ...)` returns a `FlagDetails` (`value`, `payload`, `variant`, `reason`,
`condition_index`, `request_id`, `evaluated_locally`) or `None`. `on_flags_change(cb)` / `on_flags_stale(cb)` fire when
definitions reload with a change / a reload fails or the flag stream drops.

## OpenFeature

`lucida.openfeature_provider.LucidaProvider(client)` is an OpenFeature server provider (uses `openfeature-sdk` when installed: `pip install openfeature-sdk`).
The targeting key is the distinct id and other context attributes are person properties; booleans and strings read the
flag value (a variant key is the string value, and any variant is `True` for booleans), integers, floats and objects read
the payload. Unknown flags raise `FlagNotFoundError`, a missing targeting key `TargetingKeyMissingError`, and a wrong
payload type `TypeMismatchError`. Raw reasons map through `testdata/openfeature/reasons.json`; `flag_metadata` carries `lucidaReason`, `conditionIndex`, `requestId`, `evaluatedLocally`; the `groups` / `groupProperties` attributes feed Lucida groups; `track()` captures an event; provider events (ready, configuration changed, stale, error) follow definition reloads. Stdlib tests: `python3 -I tests/test_openfeature_provider.py`.

## Agent analytics

```python
lc = Lucida("lc_pk_...", capture_text=False)
convo = lc.ai_conversation(user_id, agent_name="support-bot", trace_id=trace_id, topic="billing")
convo.message("user", question)
convo.message("assistant", answer)
convo.feedback("thumbs", -1)
convo.end("resolved")
```

`AsyncLucida` has the same `ai_conversation`.
Events: `$ai_conversation_started`, `$ai_message` (role, auto-incremented index, text length; for user
messages a simhash `$ai_text_hash` and a lexicon `$ai_sentiment` so rephrases and frustration show without the
text), `$ai_feedback` (thumbs 1/-1, rating 1..5, comment), `$ai_conversation_ended` and `$ai_outcome`
(`resolved`, `unresolved`, `escalated`, `abandoned`, `error`). Message text and feedback comments are sent only
with `capture_text=True`, after cards, emails and phone numbers are masked. `trace_id` (a W3C trace id) links the
conversation to its trace in Watchtower; tokens, latency, cost and model names are never captured here.
Events use the normal capture path, so consent and opt-out apply.

## Tests

```sh
cd sdks/python
uv run --with pytest --with pytest-asyncio --with httpx --with-editable . pytest -q
uv run --with mypy --with pytest --with pytest-asyncio --with httpx --with-editable . mypy
```

`tests/test_conformance.py` runs `testdata/flags/vectors*.json` (supplying only the context fields in each vector's
`needs`) and the `testdata/events/server_sdk.json` wire fixtures.

## Test mode

`lucida flags fixture --out flags.json`, then `Lucida("lc_pk_test", test_mode="flags.json")` (or `AsyncLucida`; a
`{"flags": [...]}` dict or a list of definitions also works): no network, flags evaluate locally from the fixture (unknown or
server-only flags return `None`), and `lc.sink.events` holds every captured event, including exposures, for assertions.

---

This repository is a read-only mirror of the Lucida monorepo (synced by a script, never edited by hand); releases and provenance attestations are built here by GitHub Actions. Issues are welcome, changes land upstream.
