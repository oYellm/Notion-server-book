import os

TITLE_PROP = os.getenv("TITLE_PROP", "책 제목")
REQUEST_PROP = os.getenv("REQUEST_PROP", "수집요청")

def query_pending_pages_checkbox(notion, database_id):
    """수집요청(체크박스)이 체크된 페이지 + 제목이 있는 페이지만 조회"""
    base_filter = {
        "and": [
            { "property": REQUEST_PROP, "checkbox": { "equals": True } },
            { "property": TITLE_PROP, "title": { "is_not_empty": True } },
        ]
    }

    results = []
    start_cursor = None
    while True:
        payload = {
            "database_id": database_id,
            "filter": base_filter,
            "page_size": 50,
        }
        if start_cursor:
            payload["start_cursor"] = start_cursor

        resp = notion.databases.query(**payload)
        results.extend(resp.get("results", []))

        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")

    return results


def clear_request_checkbox(notion, page_id):
    """처리 성공 후 '수집요청' 체크박스 해제"""
    notion.pages.update(
        page_id=page_id,
        properties={
            REQUEST_PROP: { "checkbox": False }
        }
    )
