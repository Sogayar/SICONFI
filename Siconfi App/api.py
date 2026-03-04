import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

API_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

_DEFAULT_TIMEOUT = 15 
_SESSION = None

def set_timeout(seconds: int):
    global _DEFAULT_TIMEOUT
    try:
        s = int(seconds)
        if s >= 1:
            _DEFAULT_TIMEOUT = s
    except Exception:
        pass

def _build_session():
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "siconfi-desktop/1.1"})
    return s

def _session():
    global _SESSION
    if _SESSION is None:
        _SESSION = _build_session()
    return _SESSION

def json_seguro(resp):
    try:
        return resp.json()
    except Exception:
        return None

def fetch_all_pages(url, params):
    """Percorre paginação do ORDS e retorna lista de itens."""
    dados = []
    cur_url = url
    cur_params = dict(params) if params else {}
    while True:
        r = _session().get(cur_url, params=cur_params, timeout=_DEFAULT_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} em {r.url}")
        data = json_seguro(r) or {}
        items = data.get("items", [])
        dados.extend(items)
        if not data.get("hasMore"):
            break
        links = {l.get("rel"): l.get("href") for l in data.get("links", []) if l.get("rel") and l.get("href")}
        if "next" in links:
            cur_url, cur_params = links["next"], {}
        else:
            cur_params["offset"] = cur_params.get("offset", 0) + data.get("count", len(items))
    return dados