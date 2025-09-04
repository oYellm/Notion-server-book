import os, re, json, requests, html
# ── 제목 정제 + 본문 추출 ─────────────────────────────
def clean_book_title(t: str | None) -> str | None:
    if not t:
        return t
    t = re.sub(r"\s+", " ", t).strip()
    # ' | ' 뒤 부제/저자/사이트 꼬리표 제거
    if " | " in t:
        t = t.split(" | ", 1)[0].strip()
    # '- 교보문고' 류 제거
    t = re.sub(r"\s*[-–—]\s*(교보문고|Kyobo\s*Book\s*Centre)\s*$", "", t, flags=re.I)
    return t

def extract_title_from_body_html(html_text: str) -> str | None:
    """교보 상세 본문에서 제목을 직접 추출(선호 순서대로 시도)"""
    soup = BeautifulSoup(html_text, "html.parser")
    selectors = [
        "h1.prod_title",                    # 신사이트
        ".prod_detail_header h1",           # 변형 1
        ".bookDetail_header__title",        # React 컴포넌트 케이스
        "h1.tit",                           # 구사이트
        ".prod_title strong", ".prod_title em",
        ".product_detail .title",           # 기타
        "h1"
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            cand = node.get_text(strip=True)
            cand = clean_book_title(cand)
            # 과도하게 긴 문자열/사이트명 포함은 버림
            if cand and len(cand) <= 100 and "교보문고" not in cand:
                return cand
    return None

from urllib.parse import quote
from bs4 import BeautifulSoup
from notion_client import Client

# ============ ENV ============
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
READ_PAGES_PROP= os.getenv("READ_PAGES_PROP", "읽은 페이지")

GENRE_MAP = json.loads(os.getenv("GENRE_MAP_JSON", "{}"))

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"
}
KYOBO_SEARCH = "https://search.kyobobook.co.kr/search?keyword="
KYOBO_DETAIL = "https://product.kyobobook.co.kr/detail"

# ============ Notion utils ============
if not NOTION_TOKEN or not DATABASE_ID:
    raise SystemExit("NOTION_TOKEN / DATABASE_ID 누락")

nc = Client(auth=NOTION_TOKEN)

def page_prop_type(props: dict, name: str) -> str|None:
    p = props.get(name); return p.get("type") if p else None

def build_value(ptype: str, v):
    if v is None: return None
    if ptype == "title":     return {"title":     [{"text":{"content":str(v)}}]}
    if ptype == "rich_text": return {"rich_text": [{"text":{"content":str(v)}}]}
    if ptype == "url":       return {"url": str(v)}
    if ptype == "number":
        try: return {"number": int(v)}
        except: return None
    if ptype == "select":    return {"select": {"name": str(v)}}
    if ptype == "multi_select":
        items = [s.strip() for s in str(v).split(",") if s.strip()]
        return {"multi_select": [{"name": s} for s in items]}
    if ptype == "checkbox":  return {"checkbox": bool(v)}
    return None

def ensure_status_option(option="시작 전"):
    db = nc.databases.retrieve(DATABASE_ID)
    prop = db["properties"].get(STATUS_PROP)
    if not prop or prop.get("type") != "select": return
    names = [o["name"] for o in prop["select"]["options"]]
    if option in names: return
    prop["select"]["options"].append({"name": option, "color": "default"})
    nc.databases.update(
        database_id=DATABASE_ID,
        properties={STATUS_PROP: {"select": {"options": prop["select"]["options"]}}}
    )

def query_pending_pages():
    results, cursor = [], None
    while True:
        resp = nc.databases.query(
            database_id=DATABASE_ID,
            filter={"property": REQUEST_PROP, "checkbox": {"equals": True}},
            start_cursor=cursor
        )
        results += resp["results"]
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    return results

def page_title(props: dict) -> str:
    for k, v in props.items():
        if v.get("type") == "title":
            return "".join(t["plain_text"] for t in v["title"]).strip()
    return ""

# ============ Kyobo search ============
def extract_id(s: str) -> str|None:
    m = re.search(r"(S\d{6,})", s or ""); return m.group(1) if m else None

def search_candidates_by_title(title: str) -> list[str]:
    html = requests.get(KYOBO_SEARCH + quote(title), headers=UA, timeout=20).text
    ids = list(dict.fromkeys(re.findall(r"/detail/(S\d{6,})", html)))
    if ids: return ids
    # 모바일 백업
    mhtml = requests.get("https://m.kyobobook.co.kr/search?keyword=" + quote(title), headers=UA, timeout=20).text
    return list(dict.fromkeys(re.findall(r"/detail/(S\d{6,})", mhtml)))

def choose_best_id(title: str, ids: list[str]) -> str|None:
    if not ids: return None
    key = _norm(title)
    best = None; best_score = -1
    for kid in ids[:5]:
        brief = fetch_detail(kid, light=True)
        cand = _norm(brief.get("title") or "")
        score = _sim(key, cand)
        if score > best_score:
            best, best_score = kid, score
    return best

# ============ Kyobo detail parsing ============
def _clean(s): 
    if not s: return None
    return re.sub(r"\s+", " ", s).strip()

def _jsonld_extract_all(html_text: str):
    title = author = publisher = pages = genre = isbn = None
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html_text, re.S):
        try:
            data = json.loads(m.group(1))
            arr = data if isinstance(data, list) else [data]
            for it in arr:
                if not isinstance(it, dict): continue
                if it.get("@type") in ("Book","Product"):
                    title = title
                    if isinstance(a, list) and a:
                        author = author or ", ".join([aa.get("name") if isinstance(aa,dict) else str(aa) for aa in a])
                    elif isinstance(a, dict):
                        author = author or a.get("name")
                    elif isinstance(a, str):
                        author = author or a
                    p = it.get("publisher")
                    if isinstance(p, dict): publisher = publisher or p.get("name")
                    elif isinstance(p, str): publisher = publisher or p
                    pages = pages or it.get("numberOfPages")
                    genre = genre or it.get("genre")
                    isbn  = isbn  or it.get("isbn")
        except:  # noqa
            pass
    return {
        "title": _clean(title),
        "author": _clean(author),
        "publisher": _clean(publisher),
        "pages": int(pages) if isinstance(pages, str) and pages.isdigit() else pages,
        "genre": _clean(genre),
        "isbn": _clean(isbn),
    }

def _og_meta(html_text: str):
    t = re.search(r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"', html_text)
    i = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html_text)
    return {"title": _clean(t.group(1) if t else None), "cover": i.group(1) if i else None}

def fetch_detail_static(kyobo_id: str, light=False) -> dict:
    url = f"{KYOBO_DETAIL}/{kyobo_id}"
    r = requests.get(url, headers=UA, timeout=25); r.raise_for_status()
    html_txt = r.text
    info = {"kyobo_id": kyobo_id, "detail_url": url,
            "title": None, "cover": None, "author": None,
            "publisher": None, "pages": None, "genre": None, "isbn": None}

    # 1) JSON-LD 최우선
    jd = _jsonld_extract_all(html_txt)
    for k,v in jd.items():
        if v and not info.get(k): info[k] = v

    # 2) 본문에서 제목 직접 추출 (OG/title보다 우선)
    body_title = extract_title_from_body_html(html_txt)
    if body_title:
        info["title"] = body_title

    # 3) 커버만 OG에서 보완 (제목은 쓰지 않음)
    og = _og_meta(html_txt)
    if og["cover"] and not info["cover"]: info["cover"] = og["cover"]

    if not light and not info["pages"]:
        m = re.search(r'(\d{1,5})\s*(쪽|페이지)', re.sub(r'\s+',' ', html_txt))
        if m: info["pages"] = int(m.group(1))

    if info["title"]:
        info["title"] = clean_book_title(info["title"])
    return info


def fetch_detail_browser(kyobo_id: str) -> dict:
    from playwright.sync_api import sync_playwright
    url = f"{KYOBO_DETAIL}/{kyobo_id}"
    info = {"kyobo_id": kyobo_id, "detail_url": url,
            "title": None, "cover": None, "author": None,
            "publisher": None, "pages": None, "genre": None, "isbn": None}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
            ctx = browser.new_context(locale="ko-KR", user_agent=UA["User-Agent"])
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=45000)

            # 1) JSON-LD
            scripts = page.locator('script[type="application/ld+json"]').all()
            for sc in scripts:
                try:
                    data = json.loads(sc.inner_text())
                    arr = data if isinstance(data, list) else [data]
                    for it in arr:
                        if isinstance(it, dict) and it.get("@type") in ("Book","Product"):
                            info["title"] = info["title"] or clean_book_title(it.get("name"))
                            a = it.get("author")
                            if isinstance(a, list) and a:
                                info["author"] = info["author"] or ", ".join([aa.get("name") if isinstance(aa,dict) else str(aa) for aa in a])
                            elif isinstance(a, dict):
                                info["author"] = info["author"] or a.get("name")
                            elif isinstance(a, str):
                                info["author"] = info["author"] or a
                            pbl = it.get("publisher")
                            if isinstance(pbl, dict): info["publisher"] = info["publisher"] or pbl.get("name")
                            elif isinstance(pbl, str): info["publisher"] = info["publisher"] or pbl
                            info["pages"] = info["pages"] or it.get("numberOfPages")
                            info["genre"] = info["genre"] or it.get("genre")
                            info["isbn"]  = info["isbn"]  or it.get("isbn")
                except:
                    pass

            # 2) 본문에서 제목 직접 추출 (JSON-LD 없을 때/보완)
            if not info["title"]:
                selectors = [
                    "h1.prod_title", ".prod_detail_header h1",
                    ".bookDetail_header__title", "h1.tit", ".prod_title strong", "h1"
                ]
                for sel in selectors:
                    if page.locator(sel).count() > 0:
                        t = page.locator(sel).first.inner_text().strip()
                        t = clean_book_title(t)
                        if t and "교보문고" not in t and len(t) <= 100:
                            info["title"] = t
                            break

            # 3) 커버만 OG에서 보완
            i = page.locator('meta[property="og:image"]').first.get_attribute("content")
            if i and not info["cover"]:
                info["cover"] = i

            if not info["pages"]:
                txt = page.content()
                m = re.search(r'(\d{1,5})\s*(쪽|페이지)', re.sub(r'\s+',' ', txt))
                if m: info["pages"] = int(m.group(1))

            ctx.close(); browser.close()
    except Exception as e:
        print(f"[PLAYWRIGHT] fallback error: {e}")

    if info["title"]:
        info["title"] = clean_book_title(info["title"])
    return info


def fetch_detail(kyobo_id: str, light=False) -> dict:
    info = fetch_detail_static(kyobo_id, light=light)
    if not any([info.get("title"), info.get("author"), info.get("publisher"), info.get("cover")]):
        print("[PARSE] static failed → headless fallback")
        info2 = fetch_detail_browser(kyobo_id)
        for k,v in info2.items():
            if v and not info.get(k): info[k] = v
    print(f"[PARSE] kyobo={kyobo_id} title={info.get('title')} author={info.get('author')} "
          f"publisher={info.get('publisher')} pages={info.get('pages')} cover={bool(info.get('cover'))}")
    return info

# ============ genre map / similarity ============
def _norm(s): return re.sub(r"\s+"," ", (s or "").lower()).strip()
def _sim(a,b):
    A = {a[i:i+2] for i in range(max(len(a)-1,1))}
    B = {b[i:i+2] for i in range(max(len(b)-1,1))}
    inter = len(A & B)
    return inter/len(A) if A else 0

def map_genre(raw: str|None) -> str|None:
    if not raw: return None
    R = _norm(raw)
    for keys, val in GENRE_MAP.items():
        for k in keys.split(","):
            if k.strip() and k.strip().lower() in R:
                return val
    return None

# ============ updater ============
def update_page(page_id: str, info: dict):
    # 커버
    if info.get("cover"):
        nc.pages.update(page_id=page_id, cover={"external":{"url": info["cover"]}})

    props_now = nc.pages.retrieve(page_id=page_id)["properties"]
    patch = {}

    # 제목(실제 제목 속성명 찾기)
    title_prop = next((k for k,v in props_now.items() if v.get("type")=="title"), None)
    if title_prop and info.get("title"):
        patch[title_prop] = build_value("title", info["title"])

    # 저자/출판사/페이지/URL
    for key, name in [("author",AUTHOR_PROP), ("publisher",PUBLISHER_PROP),
                      ("pages",PAGES_PROP), ("url",KY_URL_PROP)]:
        val = info.get("author") if key=="author" else \
              info.get("publisher") if key=="publisher" else \
              info.get("pages") if key=="pages" else \
              info.get("detail_url") if key=="url" else None
        ptype = page_prop_type(props_now, name)
        if ptype and val is not None:
            pv = build_value(ptype, val)
            if pv: patch[name] = pv

    # 장르
    gtype = page_prop_type(props_now, GENRE_PROP)
    mapped = map_genre(info.get("genre") or info.get("title"))
    if mapped and gtype in ("select","multi_select"):
        patch[GENRE_PROP] = build_value(gtype, mapped)

    # 상태 = 시작 전
    if page_prop_type(props_now, STATUS_PROP) == "select":
        patch[STATUS_PROP] = {"select":{"name":"시작 전"}}

    

    # 수집요청 해제
    if page_prop_type(props_now, REQUEST_PROP) == "checkbox":
        patch[REQUEST_PROP] = {"checkbox": False}

    if patch:
        nc.pages.update(page_id=page_id, properties=patch)

# ============ main ============
def run_once():
    ensure_status_option("시작 전")
    pages = query_pending_pages()
    if not pages:
        print("[INFO] pending 없음"); return

    for pg in pages:
        page_id = pg["id"]
        props = pg["properties"]
        title = page_title(props)
        kyobo_id = None

        # 1) 교보 URL에 ID가 있으면 우선
        if KY_URL_PROP in props and props[KY_URL_PROP].get("type")=="url":
            url = props[KY_URL_PROP].get("url")
            kyobo_id = extract_id(url) if url else None

        # 2) 없으면 제목으로 검색→최적 선택
        if not kyobo_id:
            ids = search_candidates_by_title(title)
            kyobo_id = choose_best_id(title, ids)

        if not kyobo_id:
            print(f"[WARN] 교보 ID 선택 실패: {title}")
            continue

        info = fetch_detail(kyobo_id, light=False)
        update_page(page_id, info)
        print(f"[OK] {title} <- {info.get('title')} ({kyobo_id})")

if __name__ == "__main__":
    run_once()
