import os, time, json, requests, re, csv
from collections import defaultdict, Counter

# -----------------------
# CONFIG
# -----------------------
HITS_PER_PAGE = 100
QUERY = "rust"
USER_AGENT = "rust-sentiment-bot/0.1 by u/MihiNomenUsoris"
REDDIT_API = f"https://www.reddit.com/r/{QUERY}/search.json"
ANEW_PATH = "data/anew.csv"
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # segundos

# -----------------------
# LOAD ANEW LEXICON
# -----------------------
def load_anew(path=ANEW_PATH):
    anew = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            term = row["term"].lower()
            anew[term] = {
                "pleasure": float(row["pleasure"]),
                "arousal": float(row["arousal"]),
                "dominance": float(row["dominance"])
            }
    return anew

ANEW = load_anew()

# -----------------------
# SENTIMENT ANALYSIS
# -----------------------
word_re = re.compile(r"\b\w+\b", re.UNICODE)

def analyze_text(text):
    words = word_re.findall(text.lower())
    scores = defaultdict(list)
    for w in words:
        if w in ANEW:
            for k, v in ANEW[w].items():
                scores[k].append(v)

    if not scores:
        return {"pleasure": 0.0, "arousal": 0.0, "dominance": 0.0, "sentiment": "empty"}

    avg = {k: (sum(v)/len(v)) for k, v in scores.items() if v}
    avg["sentiment"] = "positive" if avg.get("pleasure", 0) > 50 else "negative"
    # ensure numeric keys always exist
    for k in ("pleasure", "arousal", "dominance"):
        avg.setdefault(k, 0.0)
    return avg

# -----------------------
# HTTP helpers
# -----------------------
def get_json_with_retries(url, *, headers=None, params=None, timeout=30):
    headers = headers or {}
    params = params or {}
    params.setdefault("raw_json", 1)

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            ct = resp.headers.get("content-type", "")
            if resp.status_code == 200 and "application/json" in ct:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(RETRY_BACKOFF * attempt)
                last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                continue
            raise RuntimeError(f"HTTP {resp.status_code} ({ct}): {resp.text[:200]}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            time.sleep(RETRY_BACKOFF * attempt)
        except requests.RequestException:
            raise
    raise last_err or RuntimeError("Falha ao obter JSON")

# -----------------------
# DATA COLLECTION
# -----------------------
def fetch_posts(query=QUERY):
    params = {"q": query, "restrict_sr": "on", "sort": "relevance", "limit": HITS_PER_PAGE}
    headers = {"User-Agent": USER_AGENT}

    r = get_json_with_retries(REDDIT_API, headers=headers, params=params)
    posts = []

    for child in r.get("data", {}).get("children", []):
        d = child.get("data", {})
        comments = []
        permalink = d.get("permalink")
        if permalink:
            try:
                cr = get_json_with_retries(f"https://www.reddit.com{permalink}.json", headers=headers)
                if isinstance(cr, list) and len(cr) >= 2:
                    raw_comments = cr[1].get("data", {}).get("children", [])
                    for c in raw_comments:
                        body = c.get("data", {}).get("body")
                        if body:
                            comments.append(body)
                time.sleep(1.0)  # gentil com a API
            except Exception:
                pass

        posts.append({
            "title": d.get("title") or "",
            "selftext": d.get("selftext", "") or "",
            "created_utc": d.get("created_utc"),
            "comments": comments,
        })
    return posts

# -----------------------
# PIPELINE (per-unit table)
# -----------------------
def main():
    os.makedirs("data", exist_ok=True)
    posts = fetch_posts(QUERY)

    # Build flat rows: 1 row per unit (title/selftext/comment)
    rows = []
    for p in posts:
        base = {
            "post_title": p["title"],
            "created_utc": p["created_utc"],
        }
        if p["title"].strip():
            rows.append({**base, "unit_type": "title", "text": p["title"]})
        if p["selftext"].strip():
            rows.append({**base, "unit_type": "selftext", "text": p["selftext"]})
        for c in p["comments"]:
            if c and c.strip():
                rows.append({**base, "unit_type": "comment", "text": c})

    # Analyze each unit
    for r in rows:
        metrics = analyze_text(r["text"])
        r["sentiment"] = metrics["sentiment"]
        r["pleasure"]  = metrics["pleasure"]
        r["arousal"]   = metrics["arousal"]
        r["dominance"] = metrics["dominance"]

    # Summary over ALL units (ignore "empty" by default; include if you prefer)
    counts = Counter(r["sentiment"] for r in rows if r["sentiment"] != "empty")
    summary = dict(counts)

    # Save JSON (table + summary)
    out_json = {
        "summary": summary,
        "units": [
            {
                "post_title": r["post_title"],
                "created_utc": r["created_utc"],
                "unit_type": r["unit_type"],
                "sentiment": r["sentiment"],
                "pleasure": r["pleasure"],
                "arousal": r["arousal"],
                "dominance": r["dominance"],
                # omit the raw text in JSON if it’s too large; include if you want:
                "text": r["text"]
            }
            for r in rows
        ]
    }
    with open(f"data/{QUERY}_units.json", "w", encoding="utf-8") as f:
        json.dump(out_json, f, indent=2, ensure_ascii=False)

    # Save CSV (easy to pivot later)
    csv_path = f"data/{QUERY}_units.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["created_utc", "unit_type", "sentiment", "pleasure", "arousal", "dominance", "post_title", "text"])
        for r in rows:
            writer.writerow([r["created_utc"], r["unit_type"], r["sentiment"], f"{r['pleasure']:.4f}", f"{r['arousal']:.4f}", f"{r['dominance']:.4f}", r["post_title"], r["text"]])

    # Print concise summary
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
