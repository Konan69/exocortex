#!/usr/bin/env python3
"""Bookmarks server — SQLite backend + REST API + static serving.
   v2: articles table, segmented content, proper blog extraction."""

import argparse, json, sqlite3, os, re, html as html_mod, urllib.request, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DB_PATH = os.path.expanduser(os.environ.get("KB_READER_DB", os.path.join(os.path.dirname(__file__), "data", "bookmarks.db")))
JSONL_PATH = os.path.expanduser(os.environ.get("KB_READER_SOURCE", os.path.join(os.path.dirname(__file__), "..", "fixtures", "notes")))
HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bookmarks.html")
PORT = int(os.environ.get("KB_READER_PORT", "8421"))
HOST = os.environ.get("KB_READER_HOST", "127.0.0.1")

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS bookmarks (
            id TEXT PRIMARY KEY, tweet_id TEXT, url TEXT, text TEXT,
            author_name TEXT, author_handle TEXT, author_img TEXT,
            posted_at TEXT, synced_at TEXT, status TEXT DEFAULT 'new',
            priority TEXT DEFAULT 'medium', category TEXT, notes TEXT,
            engagement_json TEXT DEFAULT '{}', created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE,
            url TEXT, sort_order INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE,
            url TEXT, display_url TEXT, domain TEXT,
            extracted_content TEXT, extraction_status TEXT DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS quoted_tweets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE,
            tweet_id TEXT, text TEXT, author_name TEXT, author_handle TEXT
        );
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE, tag TEXT
        );
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id INTEGER REFERENCES links(id) ON DELETE CASCADE,
            bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE,
            url TEXT, domain TEXT, title TEXT,
            content TEXT, excerpt TEXT,
            word_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(link_id)
        );
        CREATE INDEX IF NOT EXISTS idx_articles_bookmark ON articles(bookmark_id);
        CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain);
    """)
    db.commit(); db.close()

def init_db_minimal():
    """Run basic init first (create tables), then add indices separately"""
    db = get_db()
    try:
        db.execute("CREATE TABLE IF NOT EXISTS articles (id INTEGER PRIMARY KEY AUTOINCREMENT, link_id INTEGER REFERENCES links(id) ON DELETE CASCADE, bookmark_id TEXT REFERENCES bookmarks(id) ON DELETE CASCADE, url TEXT, domain TEXT, title TEXT, content TEXT, excerpt TEXT, word_count INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(link_id))")
        db.execute("CREATE INDEX IF NOT EXISTS idx_articles_bookmark ON articles(bookmark_id)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain)")
    except:
        pass
    db.commit(); db.close()

def import_jsonl():
    db = get_db()
    count = db.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
    if count > 0: db.close(); return count
    if not os.path.exists(JSONL_PATH): db.close(); return 0
    imported = 0
    if os.path.isdir(JSONL_PATH):
        for filename in sorted(os.listdir(JSONL_PATH)):
            if not filename.endswith(".json"): continue
            with open(os.path.join(JSONL_PATH, filename)) as f: doc = json.load(f)
            bid, url, content = doc['id'], doc['url'], doc['content']
            domain = urllib.parse.urlparse(url).hostname or ''
            db.execute("INSERT OR IGNORE INTO bookmarks (id,url,text,author_name,author_handle) VALUES (?,?,?,?,?)",
                (bid, url, content, "Demo Garden", "demo_garden"))
            link = db.execute("INSERT INTO links (bookmark_id,url,display_url,domain,extracted_content,extraction_status) VALUES (?,?,?,?,?,?)",
                (bid, url, url, domain, content, "done"))
            db.execute("INSERT INTO articles (link_id,bookmark_id,url,domain,title,content,excerpt,word_count) VALUES (?,?,?,?,?,?,?,?)",
                (link.lastrowid, bid, url, domain, doc['title'], content, content[:300], len(content.split())))
            imported += 1
        db.commit(); db.close()
        return imported
    with open(JSONL_PATH) as f:
        for line in f:
            try: d = json.loads(line.strip())
            except: continue
            bid = d.get('id','')
            db.execute("INSERT OR IGNORE INTO bookmarks (id,tweet_id,url,text,author_name,author_handle,author_img,posted_at,synced_at,engagement_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (bid, d.get('tweetId',''), d.get('url',''), d.get('text',''), d.get('authorName',''), d.get('authorHandle',''), d.get('authorProfileImageUrl',''), d.get('postedAt',''), d.get('syncedAt',''), json.dumps(d.get('engagement',{}))))
            for i,m in enumerate(d.get('media',[])):
                url = m if isinstance(m,str) else m.get('url','')
                if url: db.execute("INSERT INTO media (bookmark_id,url,sort_order) VALUES (?,?,?)",(bid,url,i))
            for l in d.get('links',[]):
                lurl = l if isinstance(l,str) else (l.get('expandedUrl') or l.get('url',''))
                display = l if isinstance(l,str) else (l.get('displayUrl') or lurl)
                domain = ''
                try: domain = urllib.parse.urlparse(lurl).hostname or ''
                except: pass
                if lurl: db.execute("INSERT INTO links (bookmark_id,url,display_url,domain) VALUES (?,?,?,?)",(bid,lurl,display,domain.replace('www.','')))
            for t in d.get('tags',[]):
                if t: db.execute("INSERT INTO tags (bookmark_id,tag) VALUES (?,?)",(bid,t))
            qt = d.get('quotedTweet')
            if qt and isinstance(qt,dict):
                db.execute("INSERT INTO quoted_tweets (bookmark_id,tweet_id,text,author_name,author_handle) VALUES (?,?,?,?,?)",(bid,qt.get('id',''),qt.get('text',''),qt.get('authorName',''),qt.get('authorHandle','')))
            imported += 1
    db.commit(); db.close()
    return imported

def bookmark_to_dict(row):
    b = dict(row)
    db = get_db()
    b['media'] = [r['url'] for r in db.execute("SELECT url FROM media WHERE bookmark_id=? ORDER BY sort_order",(b['id'],))]
    b['links'] = [dict(r) for r in db.execute("SELECT * FROM links WHERE bookmark_id=?",(b['id'],))]
    b['tags'] = [r['tag'] for r in db.execute("SELECT tag FROM tags WHERE bookmark_id=?",(b['id'],))]
    b['quoted_tweets'] = [dict(r) for r in db.execute("SELECT * FROM quoted_tweets WHERE bookmark_id=?",(b['id'],))]
    b['articles'] = [dict(r) for r in db.execute("SELECT * FROM articles WHERE bookmark_id=?",(b['id'],))]
    b['engagement'] = json.loads(b.get('engagement_json','{}'))
    db.close()
    return b

def extract_blog_content(url):
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0 (compatible; BookmarksBot/1.0)'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            ct = resp.headers.get('Content-Type','')
            if 'text/html' not in ct: return None, "[Not HTML: {}]".format(ct)
            html_bytes = resp.read()
            charset = 'utf-8'
            for part in ct.split(';'):
                if 'charset=' in part: charset = part.split('charset=')[-1].strip()
            html_text = html_bytes.decode(charset, errors='replace')
    except Exception as e:
        return None, "[Failed: {}]".format(e)
    for tag in ['script','style','nav','header','footer','noscript','iframe']:
        html_text = re.sub(r'<{}[^>]*>.*?</{}>'.format(tag,tag), '', html_text, flags=re.DOTALL|re.IGNORECASE)
    # Extract title
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html_text, re.IGNORECASE|re.DOTALL)
    title = html_mod.unescape(title_match.group(1).strip()) if title_match else url
    text = re.sub(r'<[^>]+>', ' ', html_text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()
    if len(text) > 50000: text = text[:50000] + "\n\n[... truncated]"
    return title, text

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Content-Length',len(body))
        self.end_headers(); self.wfile.write(body)
    def send_html(self, path):
        if not os.path.exists(path): self.send_error(404); return
        with open(path,'rb') as f: content = f.read()
        self.send_response(200)
        self.send_header('Content-Type','text/html; charset=utf-8')
        self.send_header('Content-Length',len(content))
        self.end_headers(); self.wfile.write(content)
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,PATCH,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type')
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path); path = p.path; params = parse_qs(p.query)
        if path == '/api/bookmarks': return self.api_list_bookmarks(params)
        if path == '/api/articles': return self.api_list_articles(params)
        if path.startswith('/api/articles/') and path.endswith('/extract'):
            return self.api_extract_article(path.split('/')[3], params)
        if path.startswith('/api/bookmarks/') and path.endswith('/extract'):
            return self.api_extract_link(path.split('/')[3], params)
        if path.startswith('/api/bookmarks/'):
            return self.api_get_bookmark(path.split('/')[3])
        if path.startswith('/api/articles/'):
            return self.api_get_article(path.split('/')[3])
        if path == '/api/stats': return self.api_stats()
        if path == '/api/categories': return self.api_categories()
        if path in ('/','/bookmarks.html'): return self.send_html(HTML_PATH)
        self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get('Content-Length',0))
        body = json.loads(self.rfile.read(length)) if length > 0 else {}
        p = urlparse(self.path); path = p.path
        if path.startswith('/api/bookmarks/') and path.endswith('/extract'):
            return self.api_extract_link(path.split('/')[3], {}, body)
        if path.startswith('/api/articles/') and path.endswith('/extract'):
            return self.api_extract_article(path.split('/')[3], {}, body)
        self.send_error(404)

    def do_PATCH(self):
        length = int(self.headers.get('Content-Length',0))
        body = json.loads(self.rfile.read(length)) if length > 0 else {}
        path = urlparse(self.path).path
        if path.startswith('/api/bookmarks/'):
            return self.api_update_bookmark(path.split('/')[3], body)
        self.send_error(404)

    # ── bookmarks ──
    def api_list_bookmarks(self, params):
        db = get_db()
        q = "SELECT * FROM bookmarks WHERE 1=1"; args = []
        for f in ['status','priority','category']:
            v = params.get(f,[None])[0]
            if v: q += " AND {}=?".format(f); args.append(v)
        s = params.get('search',[None])[0]
        if s:
            q += " AND (text LIKE ? OR author_name LIKE ? OR author_handle LIKE ?)"
            args.extend(["%{}%".format(s)]*3)
        grp = params.get('group',[None])[0]
        sort = params.get('sort',['posted_at DESC'])[0]
        q += " ORDER BY {}".format(sort)
        rows = db.execute(q,args).fetchall()
        bms = [bookmark_to_dict(r) for r in rows]; db.close()
        if grp:
            grouped = {}
            for b in bms:
                if grp == 'category': key = b.get('category') or 'uncategorized'
                elif grp == 'status': key = b.get('status','new')
                elif grp == 'priority': key = b.get('priority','medium')
                elif grp == 'domain':
                    domains = [l.get('domain','') for l in b.get('links',[]) if l.get('domain')]
                    key = domains[0] if domains else 'no-link'
                else: key = 'all'
                grouped.setdefault(key,[]).append(b)
            self.send_json(grouped)
        else:
            self.send_json(bms)

    def api_get_bookmark(self, bid):
        db = get_db()
        row = db.execute("SELECT * FROM bookmarks WHERE id=?",(bid,)).fetchone()
        if not row: db.close(); return self.send_json({"error":"not found"},404)
        b = bookmark_to_dict(row); db.close()
        self.send_json(b)

    def api_update_bookmark(self, bid, body):
        db = get_db()
        row = db.execute("SELECT * FROM bookmarks WHERE id=?",(bid,)).fetchone()
        if not row: db.close(); return self.send_json({"error":"not found"},404)
        allowed = ['status','priority','category','notes']
        updates = {k:body[k] for k in allowed if k in body}
        if not updates: db.close(); return self.send_json({"error":"no valid fields"},400)
        set_clause = ", ".join("{} = ?".format(k) for k in updates)
        values = list(updates.values()) + [bid]
        db.execute("UPDATE bookmarks SET {} WHERE id=?".format(set_clause), values)
        db.commit()
        b = bookmark_to_dict(db.execute("SELECT * FROM bookmarks WHERE id=?",(bid,)).fetchone())
        db.close(); self.send_json(b)

    def api_extract_link(self, bid, params, body=None):
        link_id = body.get('link_id') if body else params.get('link_id',[None])[0]
        db = get_db()
        if link_id:
            link = db.execute("SELECT * FROM links WHERE id=? AND bookmark_id=?",(link_id,bid)).fetchone()
        else:
            link = db.execute("SELECT * FROM links WHERE bookmark_id=? AND (extracted_content IS NULL OR extracted_content='') LIMIT 1",(bid,)).fetchone()
        if not link: db.close(); return self.send_json({"error":"no link"},404)
        db.execute("UPDATE links SET extraction_status='extracting' WHERE id=?",(link['id'],))
        db.commit()
        title, content = extract_blog_content(link['url'])
        if title and content:
            db.execute("UPDATE links SET extracted_content=?, extraction_status='done' WHERE id=?",(content,link['id']))
            # Create article entry
            wc = len(content.split())
            excerpt = content[:300].strip()
            db.execute("INSERT OR REPLACE INTO articles (link_id,bookmark_id,url,domain,title,content,excerpt,word_count) VALUES (?,?,?,?,?,?,?,?)",
                (link['id'],bid,link['url'],link['domain'],title,content,excerpt,wc))
        else:
            db.execute("UPDATE links SET extracted_content=?, extraction_status='failed' WHERE id=?",(content or '',link['id']))
        db.commit()
        link = dict(db.execute("SELECT * FROM links WHERE id=?",(link['id'],)).fetchone())
        db.close(); self.send_json(link)

    def api_extract_article(self, aid, params, body=None):
        db = get_db()
        art = db.execute("SELECT * FROM articles WHERE id=?",(aid,)).fetchone()
        if not art: db.close(); return self.send_json({"error":"not found"},404)
        title, content = extract_blog_content(art['url'])
        if title and content:
            wc = len(content.split())
            excerpt = content[:300].strip()
            db.execute("UPDATE articles SET title=?,content=?,excerpt=?,word_count=? WHERE id=?",(title,content,excerpt,wc,aid))
            db.execute("UPDATE links SET extracted_content=?,extraction_status='done' WHERE id=?",(content,art['link_id']))
        db.commit()
        art = dict(db.execute("SELECT * FROM articles WHERE id=?",(aid,)).fetchone())
        db.close(); self.send_json(art)

    # ── articles ──
    def api_list_articles(self, params):
        db = get_db()
        q = "SELECT * FROM articles ORDER BY created_at DESC"; args = []
        domain = params.get('domain',[None])[0]
        if domain: q = "SELECT * FROM articles WHERE domain=? ORDER BY created_at DESC"; args = [domain]
        rows = db.execute(q,args).fetchall()
        arts = [dict(r) for r in rows]; db.close()
        self.send_json(arts)

    def api_get_article(self, aid):
        db = get_db()
        row = db.execute("SELECT * FROM articles WHERE id=?",(aid,)).fetchone()
        if not row: db.close(); return self.send_json({"error":"not found"},404)
        art = dict(row); db.close(); self.send_json(art)

    # ── stats ──
    def api_stats(self):
        db = get_db()
        total = db.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0]
        by_status = {r['status']:r['cnt'] for r in db.execute("SELECT status, COUNT(*) as cnt FROM bookmarks GROUP BY status")}
        by_category = {r['category'] or 'uncategorized':r['cnt'] for r in db.execute("SELECT category, COUNT(*) as cnt FROM bookmarks GROUP BY category")}
        with_media = db.execute("SELECT COUNT(DISTINCT bookmark_id) FROM media").fetchone()[0]
        with_links = db.execute("SELECT COUNT(DISTINCT bookmark_id) FROM links").fetchone()[0]
        article_count = db.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        db.close()
        self.send_json({'total':total,'by_status':by_status,'by_category':by_category,'with_media':with_media,'with_links':with_links,'articles':article_count})

    def api_categories(self):
        db = get_db()
        cats = [r['category'] for r in db.execute("SELECT DISTINCT category FROM bookmarks WHERE category IS NOT NULL AND category != '' ORDER BY category")]
        db.close(); self.send_json(cats)

def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    print("Setting up database...")
    init_db()
    imported = import_jsonl()
    print("Database ready ({} bookmarks imported)".format(imported))
    server = HTTPServer((HOST, PORT), Handler)
    print("Serving on http://localhost:{}/bookmarks.html".format(PORT))
    try: server.serve_forever()
    except KeyboardInterrupt: print("\nShutting down."); server.shutdown()

if __name__ == '__main__': main()
