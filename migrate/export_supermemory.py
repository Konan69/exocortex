#!/usr/bin/env python3
"""Export every supermemory document to disk with full content.

Runs BEFORE supermemory is removed. /v3/documents/list omits `content`; only
GET /v3/documents/{id} returns it, so each document needs its own fetch.
Resumable: skips ids already written.
"""
import argparse, json, os, sys, time, urllib.request

BASE = os.environ.get("SUPERMEMORY_URL", "http://127.0.0.1:6767")
TAG = os.environ.get("SUPERMEMORY_TAG", "demo")
OUT = os.path.expanduser(os.environ.get("SUPERMEMORY_EXPORT_DIR", "./data/supermemory-export"))
LOG = os.path.expanduser(os.environ.get("SUPERMEMORY_EXPORT_LOG", "./data/export.log"))

def log(m):
    line = "[%s] %s" % (time.strftime("%H:%M:%S"), m)
    print(line, flush=True)
    open(LOG, "a").write(line + "\n")

def call(p, b=None, m="POST"):
    headers = {"Content-Type": "application/json"}
    if os.environ.get("SUPERMEMORY_API_KEY"):
        headers["Authorization"] = "Bearer " + os.environ["SUPERMEMORY_API_KEY"]
    r = urllib.request.Request(BASE + p,
        data=json.dumps(b).encode() if b is not None else None,
        headers=headers, method=m)
    with urllib.request.urlopen(r, timeout=180) as x:
        raw = x.read()
        return json.loads(raw) if raw else {}

def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(os.path.dirname(LOG) or ".", exist_ok=True)
    ids = []
    page = 1
    while True:
        d = call("/v3/documents/list", {"containerTags": [TAG], "limit": 100, "page": page})
        b = d.get("memories") or d.get("documents") or []
        if not b:
            break
        ids += [x["id"] for x in b]
        t = (d.get("pagination") or {}).get("totalItems")
        if t and page * 100 >= t:
            break
        page += 1
    log("%d documents to export" % len(ids))

    ok = skip = fail = chars = 0
    for i, did in enumerate(ids, 1):
        path = os.path.join(OUT, did + ".json")
        if os.path.exists(path):
            skip += 1
            continue
        try:
            doc = call("/v3/documents/" + did, None, "GET")
            body = doc.get("content") or doc.get("raw") or ""
            if not body:
                fail += 1
                log("no content: %s %s" % (did, str(doc.get("title"))[:50]))
                continue
            json.dump(doc, open(path, "w"))
            ok += 1
            chars += len(body)
        except Exception as e:
            fail += 1
            log("failed %s: %s" % (did, str(e)[:90]))
        if i % 100 == 0:
            log("%d/%d  exported=%d skipped=%d failed=%d  %s chars"
                % (i, len(ids), ok, skip, fail, format(chars, ",")))
    log("EXPORT DONE — %d exported, %d skipped, %d failed, %s chars"
        % (ok, skip, fail, format(chars, ",")))

if __name__ == "__main__":
    sys.exit(main())
