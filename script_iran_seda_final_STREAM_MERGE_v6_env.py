# -*- coding: utf-8 -*-
import os, sys, re, csv, time, random
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import pandas as pd

RUN_NAME = os.getenv("RUN_NAME", "latest")
RUNS_DIR = os.getenv("RUNS_DIR", "runs")
IN_CSV_ENV = os.getenv("INPUT_CSV", "")

RAW_DIR = Path(RUNS_DIR) / RUN_NAME / "raw"
MERGED_DIR = Path(RUNS_DIR) / RUN_NAME / "merged"
ERROR_DIR = Path(RUNS_DIR) / RUN_NAME / "errors"
for d in (RAW_DIR, MERGED_DIR, ERROR_DIR):
    d.mkdir(parents=True, exist_ok=True)

INPUT_CSV = IN_CSV_ENV or str(RAW_DIR / f"audiobooks_{RUN_NAME}.csv")
OUT_CSV = str(MERGED_DIR / f"books_with_attid_{RUN_NAME}.csv")
ERR_CSV = str(ERROR_DIR / f"errors_{RUN_NAME}.csv")

CSV_FIELDS = [
    "AudioBook_ID","Book_Title","Book_Description","Book_Detail","Book_Language","Book_Country",
    "Book_Author","Book_Translator","Book_Narrator","Book_Director","Book_Producer",
    "Book_SoundEngineer","Book_Effector","Book_Actors","Book_Genre","Book_Category",
    "Book_Duration","Episode_Count","Cover_Image_URL","Player_Link",
    "FullBook_MP3_URL","All_MP3s_Found"
]

def abs_url(u: str) -> str:
    if u.startswith("http"): return u
    return urljoin("https://book.iranseda.ir/", u)

def req_get(url: str) -> requests.Response:
    r = requests.get(url, timeout=30)
    r.encoding = "utf-8"
    r.raise_for_status()
    return r

def text_or_none(el):
    return el.get_text(strip=True) if el else None

def parse_label_from_iteminfo(soup, label_fa):
    for li in soup.select(".item-info li"):
        span = li.find("span")
        if span and label_fa in span.get_text(strip=True):
            txt = li.get_text(" ", strip=True)
            return txt.replace(label_fa, "").strip()
    return None

def parse_from_metadata_list(soup, dt_text):
    for li in soup.select(".metadata-list li"):
        dt = li.find("dt")
        if dt and dt_text in dt.get_text(strip=True):
            dd = li.find("dd")
            return text_or_none(dd)
    return None

def get_og_image(soup):
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"): return og["content"]
    return None

def find_first_image_src(soup):
    img = soup.find("img", src=True)
    return img["src"] if img else None

def extract_attid(soup):
    og = get_og_image(soup)
    if og:
        m = re.search(r"[?&]AttID=(\d+)", og, re.I)
        if m: return int(m.group(1))
    for img in soup.find_all("img", src=True):
        m = re.search(r"[?&]AttID=(\d+)", img["src"], re.I)
        if m: return int(m.group(1))
    return None

def parse_duration_and_episodes(soup):
    dur = parse_label_from_iteminfo(soup, "مدت زمان:")
    eps = parse_label_from_iteminfo(soup, "تعداد قسمت:")
    return dur, eps

def build_player_link(audio_id, attid):
    return f"https://book.iranseda.ir/Details?VALID=TRUE&g={audio_id}&b=&attid={attid}"

def get_mp3s_from_api(g, attid):
    try:
        api_url = f"https://apisec.iranseda.ir/book/Details/?VALID=TRUE&g={g}&attid={attid}"
        r = req_get(api_url)
        data = r.json()
        urls = []
        best_url = None
        best_size = -1
        for it in data.get("items", []):
            for d in it.get("download", []):
                if str(d.get("extension","")).lower() == "mp3":
                    url = abs_url(d.get("downloadUrl",""))
                    size = int(d.get("fileSize","0") or 0)
                    urls.append(url)
                    if size > best_size:
                        best_size = size
                        best_url = url
        return best_url, ",".join(urls) if urls else None
    except Exception:
        return None, None

def parse_page(html: str, url: str):
    soup = BeautifulSoup(html, "html.parser")
    title = text_or_none(soup.find("h1"))
    desc = text_or_none(soup.find("div", class_="short-description")) or ""
    detail = text_or_none(soup.find("div", class_="full-description")) or ""
    lang = parse_from_metadata_list(soup, "زبان")
    country = parse_from_metadata_list(soup, "کشور")
    author = parse_from_metadata_list(soup, "نویسنده")
    translator = parse_from_metadata_list(soup, "مترجم")
    narrator = parse_from_metadata_list(soup, "گوینده")
    director = parse_from_metadata_list(soup, "کارگردان")
    producer = parse_from_metadata_list(soup, "تهیه‌کننده")
    se = parse_from_metadata_list(soup, "مهندس صدا")
    eff = parse_from_metadata_list(soup, "افکت‌گذار")
    actors = parse_from_metadata_list(soup, "بازیگران")
    genre = parse_from_metadata_list(soup, "ژانر")
    category = parse_from_metadata_list(soup, "دسته‌بندی")

    attid = extract_attid(soup)
    duration, episodes = parse_duration_and_episodes(soup)
    cover = get_og_image(soup) or find_first_image_src(soup)
    if cover:
        cover = abs_url(cover)

    q = parse_qs(urlparse(url).query)
    g = q.get("g", [None])[0]

    return {
        "AudioBook_ID": g,
        "Book_Title": title,
        "Book_Description": desc,
        "Book_Detail": detail,
        "Book_Language": lang,
        "Book_Country": country,
        "Book_Author": author,
        "Book_Translator": translator,
        "Book_Narrator": narrator,
        "Book_Director": director,
        "Book_Producer": producer,
        "Book_SoundEngineer": se,
        "Book_Effector": eff,
        "Book_Actors": actors,
        "Book_Genre": genre,
        "Book_Category": category,
        "Book_Duration": duration,
        "Episode_Count": episodes,
        "Cover_Image_URL": cover,
        "Player_Link": build_player_link(g, attid) if g and attid else None,
        "attid": attid,
    }

def main():
    in_path = Path(INPUT_CSV)
    if not in_path.exists():
        print(f"ERROR: {INPUT_CSV} not found.")
        sys.exit(1)

    df_in = pd.read_csv(in_path, encoding="utf-8")
    merged_rows = []
    error_rows = []

    for idx, row in df_in.iterrows():
        url = str(row["URL"]).strip()
        try:
            r = req_get(url)
            parsed = parse_page(r.text, url)
            attid = parsed.get("attid")
            best, all_mp3 = (None, None)
            if attid and parsed.get("AudioBook_ID"):
                best, all_mp3 = get_mp3s_from_api(parsed["AudioBook_ID"], attid)
            parsed["FullBook_MP3_URL"] = best
            parsed["All_MP3s_Found"] = all_mp3
            merged_rows.append(parsed)
            print(f"[{idx+1}/{len(df_in)}] ✓ {parsed.get('AudioBook_ID')}")
        except Exception as e:
            print(f"[{idx+1}/{len(df_in)}] ✗ {row.get('AudioBook_ID')}: {e}")
            error_rows.append({
                "AudioBook_ID": row.get("AudioBook_ID"),
                "Error": str(e),
            })
        time.sleep(random.uniform(0.1, 0.3))

    if error_rows:
        err_path = Path(ERR_CSV)
        err_path.parent.mkdir(parents=True, exist_ok=True)
        with err_path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["AudioBook_ID", "Error"])
            w.writeheader()
            w.writerows(error_rows)

    out_path = Path(OUT_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in merged_rows:
            w.writerow(r)

    print("✓ Wrote:", OUT_CSV)

if __name__ == "__main__":
    main()
