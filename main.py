import time
import requests

DAYS = 3000
QUERY = "python"
TAGS = "story,comment"
HITS_PER_PAGE = 1000
FILTERS = f"created_at_i>{int(time.time() - DAYS * 24 * 60 * 60)}"
ALGOLIA_API = "https://hn.algolia.com/api/v1/search"

def main():
    params = {
        "query": QUERY,
        "tags": TAGS,
        # "numericFilters": FILTERS,
        "hitsPerPage": HITS_PER_PAGE,
    }

    page = 0
    total_hits = 0

    try:
        while True:
            params["page"] = page
            print(f"Fetching page {page} with URL: {ALGOLIA_API}?{requests.compat.urlencode(params)}")
            r = requests.get(ALGOLIA_API, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            hits = data.get("hits", [])
            total_hits += len(hits)
            print(f"Page {page}: {len(hits)} hits (Total so far: {total_hits})")

            for hit in hits:
                # Print title for stories, comment_text for comments
                text = hit.get("title") or hit.get("comment_text")
                print(f"Title/Comment: {text}")

            # Check if there are more pages
            if page >= data.get("nbPages", 0) - 1:
                break
            page += 1

        print(f"Total hits retrieved: {total_hits}")

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    main()