import os, time, json, requests, re, csv
from collections import defaultdict

# -----------------------
# CONFIG
# -----------------------
DAYS = 30
HITS_PER_PAGE = 100
QUERY = "rust"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0)"
REDDIT_API = f"https://www.reddit.com/r/{QUERY}/search.json"
ANEW_PATH = "data/anew.csv"

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
        return {"pleasure": 0, "arousal": 0, "dominance": 0, "sentiment": "empty"}

    avg = {k: sum(v)/len(v) for k, v in scores.items() if len(v) > 0}

    # simple rule: positive if pleasure > 50, negative if < 50
    avg["sentiment"] = "positive" if avg["pleasure"] > 50 else "negative"
    return avg

# -----------------------
# DATA COLLECTION
# -----------------------
def fetch_posts(query=QUERY):
    cutoff = int(time.time() - DAYS*24*60*60)
    params = {"q": query, "restrict_sr": "on", "sort": "new", "limit": HITS_PER_PAGE}
    headers = {"User-Agent": USER_AGENT}

    r = requests.get(REDDIT_API, params=params, headers=headers, timeout=30).json()
    posts = []

    for child in r.get("data", {}).get("children", []):
        d = child["data"]
        if d.get("created_utc", 0) < cutoff:
            continue

        # fetch comments
        comments = []
        if d.get("permalink"):
            try:
                cr = requests.get(
                    f"https://www.reddit.com{d['permalink']}.json",
                    headers=headers,
                    timeout=30
                ).json()
                raw_comments = cr[1]["data"]["children"]
                for c in raw_comments:
                    body = c["data"].get("body")
                    if body:
                        comments.append(body)
                time.sleep(1)  # be gentle with API
            except Exception:
                pass

        posts.append({
            "title": d.get("title"),
            "selftext": d.get("selftext", ""),
            "created_utc": d.get("created_utc"),
            "comments": comments,
        })

    return posts

# -----------------------
# PIPELINE
# -----------------------
def main():
    os.makedirs("data", exist_ok=True)

    posts = fetch_posts(QUERY)

    analyzed = []
    for p in posts:
        texts = [p["title"], p["selftext"]] + p["comments"]

        all_scores = [analyze_text(t) for t in texts if t]

        if not all_scores:
            sentiment = "not enough data"
        else:
            # majority vote by label
            labels = [s["sentiment"] for s in all_scores if s["sentiment"] != "empty"]
            if not labels:
                sentiment = "not enough data"
            else:
                sentiment = max(set(labels), key=labels.count)

        analyzed.append({
            "title": p["title"],
            "created_utc": p["created_utc"],
            "sentiment": sentiment,
            "comments_count": len(p["comments"])
        })

    # aggregate result
    totals = defaultdict(int)
    for a in analyzed:
        totals[a["sentiment"]] += 1

    results = {
        "summary": dict(totals),
        "posts": analyzed
    }

    with open(f"data/{QUERY}_sentiment.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(json.dumps(results["summary"], indent=2))

if __name__ == "__main__":
    main()
