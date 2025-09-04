import os, re, json, time, html
import difflib
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
from notion_client import Client

# ───────────────── env & props ─────────────────
NOTION_TOKEN   = os.getenv("NOTION_TOKEN")
DATABASE_ID    = os.getenv("DATABASE_ID")

TITLE_PROP     = os.getenv("TITLE_PROP", "책 제목")
AUTHOR_PROP    = os.getenv("AUTHOR_PROP", "저자")
PUBLISHER_PROP = os.getenv("PUBLISHER_PROP", "출판사")
PAGES_PROP     = os.getenv("PAGES_PROP", "페이지")
GENRE_PROP     = os.getenv("GENRE_PROP", "장르")
STATUS_PROP    = os.getenv("STATUS_PROP", "상태")
KY_URL_PROP    = os.getenv("KY_URL_PROP", "교보 URL")
REQUEST_PROP   = os.getenv("REQUEST_PROP", "수집요청")

GENRE_MAP_JSON = os.getenv("GENRE_MAP_JSON", "{}")
GENRE_MAP = json.loads(GENRE_MAP_JSON)

KYOBO_SEARCH_BASE = "https://search.kyobobook.co.kr/search?keyword="
KYOBO_DETAIL_BASE = "https://product.kyobobook.co.kr/detail"

UA = {"User-Agent": "Mozilla/5.0 (KyoboNotionBot)"}

# ───────────────── helpers ─────────────────
def notion() -> Client:
    if not NOTION_TOKEN or not DATABASE_ID:
        raise SystemExit("NOTION_TOKEN / DATABASE_ID 누락")
    return Client(auth=NOTION_TOKEN)

def extract_kyobo_id(s: str) -> str | None:
    m = re.search(r"(S\d{6,})", s)
    return m.group(1) if m else None

def get_db_schema(nc: Client):
    return nc.databases.retrieve(DATABASE_ID)

def ensure_status_option(nc: Client, db_schema: dict, option_name: str = "시작 전"):
    prop = db_schema["properties"].get(STATUS_PROP)
    if not prop or prop.get("type") != "select":
        return
    names = [o["name"] for o in prop["select"]["options"]]
    if option_name in names:
        return
    # add option
    prop["select"]["options"].append({"name": option_name, "color": "default"})
    nc.databases.update(
        database_id=DATABASE_ID,
        properties={STATUS_PROP: {"select": {"options": prop["select"]["options"]}}}
    )

def page_title_of(nc: Client, page: dict) -> str:
    for k, v in page["properties"].items():
        if v.get("type") == "title":
            return "".join([t["plain_text"] for t in v["title"]]).strip()
    return ""

def page_prop_type(page_or_schema_props: dict, name: str) -> str | None:
    p = page_or_schema_props.get(name)
    return p.get("type") if p else None

def build_value(ptype: str, value):
    if value is None: return None
    if ptype == "title":
        return {"title": [{"text": {"content": str(value)}}]}
    if ptype == "rich_text":
        return {"rich_text": [{"text": {"content": str(value)}}]}
    if ptype == "url":
        return {"url": str(value)}
    if ptype == "number":
        try: return {"number": int(value)}
        except: return None
    if ptype == "select":
        return {"select": {"name": str(value)}}
    if ptype == "multi_select":
        parts = [s.strip() for s in str(value).split(",") if s.strip()]
        return {"multi_select": [{"name": s} for s in parts]}
    if ptype == "checkbox":
        return {"checkbox": bool(value)}
    return None

# ───────────────── Kyobo scraping ─────────────────
def search_candidates_by_title(title: str) -> list[dict]:
    """교보 검색에서 상위 후보 여러 개 추출"""
    url = KYOBO_SEARCH_BASE + quote(title)
    html_text = requests.get(url, headers=UA, timeout=15).text
    # 후보: /detail/Sxxxx 링크들
    ids = list(dict.fromkeys(re.findall(r"/detail/(S\d{6,})", html_text)))
    cands = []
    for kid in ids[:10]:
        # 제목/저자/출판사 힌트를 검색 HTML에서 유추(없어도 OK)
        cands.append({"kyobo_id": kid})
    return cands

def choose_best_candidate(title: str, cands: list[dict]) -> str | None:
    """제목 유사도 기반으로 최적 후보 선택 (간단 점수)"""
    if not cands: return None
    # 디코딩/정리
    key = normalize(title)
    best_id, best_score = None, -1.0
    for c in cands:
        # 상세 한번 열어 정확한 제목을 본 뒤 유사도 채점 (정확도↑)
        info = fetch_detail(c["kyobo_id"], light=True)
        cand_title = info.get("title") or ""
        score = difflib.SequenceMatcher(a=key, b=normalize(cand_title)).ratio()
        if score > best_score:
            best_score, best_id = score, c["kyobo_id"]
    return best_id

def normalize(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def fetch_detail(kyobo_id: str, light: bool=False) -> dict:
    """교보 상세에서 메타 파싱 (light=True면 필수만)"""
    url = f"{KYOBO_DETAIL_BASE}/{kyobo_id}"
    r = requests.get(url, headers=UA, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title = (soup.find("meta", property="og:title") or {}).get("content")
    cover = (soup.find("meta", property="og:image") or {}).get("content")

    author = publisher = pages = genre = isbn = None

    # JSON-LD 시도(가장 신뢰)
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
            lst = data if isinstance(data, list) else [data]
            for it in lst:
                if not isinstance(it, dict): continue
                if it.get("@type") in ("Book","Product"):
                    title = title or it.get("name")
                    a = it.get("author")
                    if isinstance(a, list) and a:
                        author = author or ", ".join([aa.get("name") if isinstance(aa, dict) else str(aa) for aa in a])
                    elif isinstance(a, dict):
                        author = author or a.get("name")
                    elif isinstance(a, str):
                        author = author or a
                    p = it.get("publisher")
                    if isinstance(p, dict): publisher = publisher or p.get("name")
                    elif isinstance(p, str): publisher = publisher or p
                    pages = pages or it.get("numberOfPages")
                    genre = genre or it.get("genre")
                    isbn = isbn or it.get("isbn")
        except Exception:
            pass

    if not light:
        # 보조 추출
        if not pages:
            text = soup.get_text(" ", strip=True)
            m = re.search(r"(\d{1,5})\s*(쪽|페이지)", text)
            if m: pages = int(m.group(1))

    return {
        "kyobo_id": kyobo_id, "detail_url": url,
        "title": title, "cover": cover,
        "author": author, "publisher": publisher,
        "pages": int(pages) if (isinstance(pages,str) and pages.isdigit()) else pages,
        "genre": genre, "isbn": isbn
    }

# ───────────────── Genre mapping ─────────────────
def map_genre(raw: str | None) -> str | None:
    if not raw: return None
    r = normalize(raw)
    for keys, notion_value in GENRE_MAP.items():
        for k in keys.split(","):
            k = k.strip().lower()
            if not k: continue
            if k in r:
                return notion_value
    return None

# ───────────────── Notion processing ─────────────────
def query_pending_pages(nc: Client) -> list[dict]:
    """수집요청=TRUE 인 페이지들 가져오기"""
    results = []
    cursor = None
    while True:
        resp = nc.databases.query(
            database_id=DATABASE_ID,
            filter={"property": REQUEST_PROP, "checkbox": {"equals": True}},
            start_cursor=cursor
        )
        results.extend(resp["results"])
        cursor = resp.get("next_cursor")
        if not resp.get("has_more"): break
    return results

def update_page(nc: Client, page_id: str, info: dict, db_schema: dict):
    """커버 + 속성 업데이트, 상태=시작 전, 수집요청=false, 읽은 페이지=0(있으면)"""
    # 커버
    if info.get("cover"):
        nc.pages.update(page_id=page_id, cover={"external": {"url": info["cover"]}})

    props_now = nc.pages.retrieve(page_id=page_id)["properties"]
    patch = {}

    # 제목
    tprop = next((k for k,v in props_now.items() if v.get("type")=="title"), None)
    if tprop and info.get("title"):
        patch[tprop] = build_value("title", info["title"])

    # 필드들
    for key, prop_name in [
        ("author",    AUTHOR_PROP),
        ("publisher", PUBLISHER_PROP),
        ("pages",     PAGES_PROP),
        ("ky_url",    KY_URL_PROP),
    ]:
        value = info.get("author") if key=="author" else \
                info.get("publisher") if key=="publisher" else \
                info.get("pages") if key=="pages" else \
                info.get("detail_url") if key=="ky_url" else None

        ptype = page_prop_type(props_now, prop_name)
        if ptype and value is not None:
            pv = build_value(ptype, value)
            if pv: patch[prop_name] = pv

    # 장르 매핑
    mapped = map_genre(info.get("genre")) or map_genre(info.get("title")) or None
    if mapped and page_prop_type(props_now, GENRE_PROP) in ("select","multi_select"):
        pv = build_value(page_prop_type(props_now, GENRE_PROP), mapped)
        if pv: patch[GENRE_PROP] = pv

    # 상태 = 시작 전
    if page_prop_type(props_now, STATUS_PROP) == "select":
        patch[STATUS_PROP] = {"select": {"name": "시작 전"}}

    # 읽은 페이지 = 0 (있으면)
    if page_prop_type(props_now, "읽은 페이지") == "number":
        patch["읽은 페이지"] = {"number": 0}

    # 수집요청 해제
    if page_prop_type(props_now, REQUEST_PROP) == "checkbox":
        patch[REQUEST_PROP] = {"checkbox": False}

    if patch:
        nc.pages.update(page_id=page_id, properties=patch)

def run_once():
    nc = notion()
    db_schema = get_db_schema(nc)
    ensure_status_option(nc, db_schema, "시작 전")

    pages = query_pending_pages(nc)
    if not pages:
        print("[INFO] pending 없음"); return

    for pg in pages:
        page_id = pg["id"]
        title = page_title_of(nc, pg)
        ky_url_prop = pg["properties"].get(KY_URL_PROP)
        kyobo_id = None
        if ky_url_prop and ky_url_prop.get("type")=="url" and ky_url_prop.get("url"):
            kyobo_id = extract_kyobo_id(ky_url_prop["url"])

        # 후보 결정
        if not kyobo_id:
            cands = search_candidates_by_title(title)
            kyobo_id = choose_best_candidate(title, cands)

        if not kyobo_id:
            print(f"[WARN] 교보 ID 미결정: {title}")
            continue

        info = fetch_detail(kyobo_id, light=False)
        # 노션에 업데이트
        update_page(nc, page_id, info, db_schema)
        print(f"[OK] {title} <- {info.get('title')} ({kyobo_id})")

if __name__ == "__main__":
    run_once()
