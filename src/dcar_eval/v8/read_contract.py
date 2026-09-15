"""Side-effect-free gateway/read-service routing contract."""
import re

READ_KEY_HEADER = "X-Dcar-Read-Key"
READ_SCOPE_HEADER = "X-Dcar-Read-Scope"
READ_DOMAINS = frozenset({"accounts", "contents", "overview", "selling-points", "spu"})
_GET_DOMAINS = {
    "/api/v8/overview": "overview",
    "/api/v8/selling-points": "selling-points",
    "/api/v8/spu-audience/assets": "spu",
    "/api/v8/spu-audience/stats": "spu",
}
_POST_DOMAINS = {
    "/api/v8/accounts/search": "accounts",
    "/api/v8/contents/search": "contents",
}


def read_domain(method: str, path: str) -> str | None:
    # Exact paths/methods only: no prefix forwarding of account mutations,
    # exports, OAuth, health or job-control endpoints to the reader.
    if method == "GET":
        if re.fullmatch(r"/api/v8/accounts/directory/[1-9][0-9]*", path):
            return "accounts"
        return _GET_DOMAINS.get(path)
    return _POST_DOMAINS.get(path) if method == "POST" else None


def invalidation_domains(method: str, path: str) -> frozenset[str]:
    if method in {"GET", "HEAD", "OPTIONS"} or read_domain(method, path):
        return frozenset()
    if path.endswith(("/search", "/export", "/validate")):
        return frozenset()
    if path.startswith(("/api/v8/accounts", "/api/v8/account-roster", "/api/v8/account-directory")):
        return READ_DOMAINS
    if path.startswith("/api/v8/spu"):
        return frozenset({"spu", "contents"})
    if path.startswith("/api/v8/selling-points"):
        return frozenset({"selling-points", "contents", "overview", "spu"})
    if path.startswith("/api/v8/contents"):
        return frozenset({"accounts", "contents", "overview", "selling-points", "spu"})
    # Asynchronous capture/task completions bypass this path; semantic revision
    # checks plus the hard cache TTL handle them, without mislabelling dispatch
    # acknowledgement as completed data publication.
    return frozenset()
