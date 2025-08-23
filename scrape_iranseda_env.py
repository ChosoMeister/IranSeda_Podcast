# -*- coding: utf-8 -*-
import os, csv, re, time, random
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

RUN_NAME = os.getenv("RUN_NAME", "latest")
RUNS_DIR = os.getenv("RUNS_DIR", "runs")
SOURCE_URL = os.getenv("SOURCE_URL", "https://book.iranseda.ir/taglist/?VALID=TRUE&t=%D8%A2%D8%AF%D8%A7%D8%A8%20%D9%88%20%D8%B1%D8%B3%D9%88%D9%85&pn={}").strip()
START_PAGE = int(os.getenv("START_PAGE", "1") or "1")
END_PAGE = int(os.getenv("END_PAGE", "1") or "1")

out_dir = os.path.join(RUNS_DIR, RUN_NAME, "raw")
os.makedirs(out_dir, exist_ok=True)
OUTPUT_FILE = os.path.join(out_dir, f"audiobooks_{RUN_NAME}.csv")

def abs_url(u: str) -> str:
    if u.startswith("http"):
        return u
    return urljoin("https://book.iranseda.ir/", u)

all_data = []
for page in range(START_PAGE, END_PAGE + 1):
    url = SOURCE_URL.format(page)
    print(f"[scrape] Page {page}: {url}")
    r = requests.get(url, timeout=30)
    r.encoding = "utf-8"
    if r.status_code != 200:
        print(f"  ! status={r.status_code}")
        continue
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select("a[href*='?VALID=TRUE&g=']"):
        href = a.get("href", "")
        m = re.search(r"[?&]g=(\d+)", href)
        if m:
            book_id = int(m.group(1))
            all_data.append([book_id, abs_url(href)])
    time.sleep(random.uniform(0.1, 0.3))

seen = set()
unique = []
for (bid, url) in all_data:
    if bid in seen: continue
    seen.add(bid)
    unique.append([bid, url])

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["AudioBook_ID", "URL"])
    w.writerows(unique)

print(f"[scrape] ✓ wrote {len(unique)} rows -> {OUTPUT_FILE}")
