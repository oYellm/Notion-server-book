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

# ⬇ 기존 fetch_detail / search 쪽만 교체

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

def _clean_space(s: str | None) -> str | None:
    if not s: return s
    return re.sub(r"\s+", " ", s).strip()

def _jsonld_extract_all(html_text: str):
    """JSON-LD에서 title/author/publisher/pages/genre/isbn 끌어오기 (여러 스크립트 안전 처리)"""
    title = author = publisher = pages = genre = isbn = None
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html_text, flags=re.S):
        try:
            data = json.loads(m.group(1))
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict): 
                    continue
                if it.get("@type") in ("Book", "Product"):
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
            continue
    return {
        "title": _clean_space(title),
        "author": _clean_space(author),
        "publisher": _clean_space(publisher),
        "pages": int(pages) if (isinstance(pages, str) and pages.isdigit()) else pages,
        "genre": _clean_space(genre),
        "isbn": _clean_space(isbn),
    }

def _og_meta(html_text: str):
    """Open Graph/Twitter 메타 백업 추출"""
    og_title = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html_text)
    tw_title = re.search(r'<meta[^>]+name="twitter:title"[^>]+content="([^"]+)"', html_text)
    og_img   = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html_text)
    return {
        "title": _clean_space((og_title or tw_title).group(1)) if (og_title or tw_title) else None,
        "cover": (og_img.group(1) if og_img else None)
    }

def _loose_text_fallback(compact_text: str):
    """라벨 텍스트에서 대충 뽑아내는 최후의 백업"""
    # 페이지수
    pages = None
    m = re.search(r'(\d{1,5})\s*(쪽|페이지)', compact_text)
    if m: pages = int(m.group(1))
    # 출판사/저자 근사치
    pub = None
    m = re.search(r'출판사[^\S\r\n]*[:\-]?\s*([가-힣A-Za-z0-9&\.\s]{2,40})', compact_text)
    if m: pub = _clean_space(m.group(1))
    aut = None
    m = re.search(r'저자[^\S\r\n]*[:\-]?\s*([가-힣A-Za-z0-9&\.\,\s]{2,60})', compact_text)
    if m: aut = _clean_space(m.group(1))
    return {"author": aut, "publisher": pub, "pages": pages}

def fetch_detail(kyobo_id: str, light: bool=False) -> dict:
    """교보 상세에서 메타 수집 (강인해진 버전)"""
    url_pc = f"https://product.kyobobook.co.kr/detail/{kyobo_id}"
    # 1) PC 페이지 시도
    r = requests.get(url_pc, headers=UA, timeout=20)
    r.raise_for_status()
    html_pc = r.text
    compact = re.sub(r"\s+", " ", html_pc)

    info = {"kyobo_id": kyobo_id, "detail_url": url_pc, "title": None, "cover": None,
            "author": None, "publisher": None, "pages": None, "genre": None, "isbn": None}

    # 1-a) JSON-LD
    jd = _jsonld_extract_all(html_pc)
    for k,v in jd.items():
        if v and info.get(k) in (None, "", 0): info[k] = v

    # 1-b) OG/Twitter 메타
    og = _og_meta(html_pc)
    if og.get("title") and not info["title"]: info["title"] = og["title"]
    if og.get("cover") and not info["cover"]: info["cover"] = og["cover"]

    # 1-c) 최후 텍스트 백업
    if not light:
        loose = _loose_text_fallback(compact)
        for k,v in loose.items():
            if v and not info.get(k): info[k] = v

    # 2) 핵심이 여전히 비었으면 모바일 검색 백업(HTML 구조가 더 단순)
    missing_core = not info["title"] or not info["cover"]
    if missing_core and not light:
        url_m = f"https://m.kyobobook.co.kr/search?keyword={quote(info['title'] or kyobo_id)}"
        try:
            hm = requests.get(url_m, headers=UA, timeout=15).text
            # 모바일 검색에서 첫 detail id 뽑기
            m_ids = list(dict.fromkeys(re.findall(r"/detail/(S\d{6,})", hm)))
            if m_ids:
                url_m_detail = f"https://product.kyobobook.co.kr/detail/{m_ids[0]}"
                hm2 = requests.get(url_m_detail, headers=UA, timeout=15).text
                og2 = _og_meta(hm2)
                if og2.get("title") and not info["title"]: info["title"] = og2["title"]
                if og2.get("cover") and not info["cover"]: info["cover"] = og2["cover"]
        except Exception:
            pass

    # 숫자 변환
    if isinstance(info["pages"], str) and info["pages"].isdigit():
        info["pages"] = int(info["pages"])

    # 디버그(필요 시 주석 해제해 로그에서 확인)
    print(f"[PARSE] kyobo={kyobo_id} title={info['title']!r} author={info['author']!r} "
          f"publisher={info['publisher']!r} pages={info['pages']!r} cover={bool(info['cover'])}")

    return info

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
