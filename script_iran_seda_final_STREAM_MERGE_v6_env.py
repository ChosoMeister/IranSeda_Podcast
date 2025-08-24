# -*- coding: utf-8 -*-
import os, sys, re, csv, time, random, io
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import pandas as pd

RUN_NAME = os.getenv("RUN_NAME", "latest")
RUNS_DIR = os.getenv("RUNS_DIR", "runs")
IN_CSV_ENV = os.getenv("INPUT_CSV", "")
GSHEET_URL = os.getenv("GOOGLE_SHEET_URL", "").strip()

RAW_DIR = Path(RUNS_DIR) / RUN_NAME / "raw"
MERGED_DIR = Path(RUNS_DIR) / RUN_NAME / "merged"
ERROR_DIR = Path(RUNS_DIR) / RUN_NAME / "errors"
for d in (RAW_DIR, MERGED_DIR, ERROR_DIR):
    d.mkdir(parents=True, exist_ok=True)

INPUT_CSV = IN_CSV_ENV or str(RAW_DIR / f"audiobooks_{RUN_NAME}.csv")
OUT_CSV = str(MERGED_DIR / f"books_with_attid_{RUN_NAME}.csv")
ERR_CSV = str(ERROR_DIR / f"errors_{RUN_NAME}.csv")

CSV_FIELDS = [
    "AudioBook_ID","Book_Title","Book_Description","Book_Detail","Book_Summary","Book_Language","Book_Country",
    "Book_Author","Book_Translator","Book_Narrator","Book_Director","Book_Producer",
    "Book_SoundEngineer","Book_Effector","Book_Actors","Book_Genre","Book_Category",
    "Book_Duration","Episode_Count","Cover_Image_URL","Player_Link",
    "FullBook_MP3_URL","All_MP3s_Found"
]

def read_gsheet(url: str):
    if not url:
        return None
    export = url
    if "export?format=csv" not in url:
        m = re.search(r"/d/([\w-]+)", url)
        if not m:
            raise ValueError("Invalid Google Sheet URL")
        sheet_id = m.group(1)
        gid_match = re.search(r"[?&]gid=(\d+)", url)
        gid = gid_match.group(1) if gid_match else "0"
        export = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    r = requests.get(export, timeout=30)
    r.raise_for_status()
    content = r.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        u = row.get("URL") or row.get("url")
        if not u:
            continue
        bid = None
        m = re.search(r"[?&]g=(\d+)", u)
        if m:
            bid = m.group(1)
        rows.append({"AudioBook_ID": bid, "URL": u, "Summary": row.get("Summary") or row.get("Book_Summary")})
    if not rows:
        reader2 = csv.reader(io.StringIO(content))
        for row in reader2:
            if not row:
                continue
            u = row[0].strip()
            if not u or u.lower() == "url":
                continue
            bid = None
            m = re.search(r"[?&]g=(\d+)", u)
            if m:
                bid = m.group(1)
            rows.append({"AudioBook_ID": bid, "URL": u, "Summary": None})
    return pd.DataFrame(rows)

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
    for dd in soup.select(".item-info dd.field"):
        strong = dd.find("strong")
        if strong and label_fa in strong.get_text(strip=True):
            items = [t.get_text(" ", strip=True) for t in dd.find_all(["a", "span"])]
            items = [t for t in items if t and t != label_fa]
            return "، ".join(dict.fromkeys(items)) or None
    return None


def parse_from_metadata_list(soup, dt_text):
    for dt in soup.select("#tags dt"):
        if dt_text in dt.get_text(strip=True):
            dd = dt.find_next_sibling("dd")
            if dd:
                vals = [sp.get_text(" ", strip=True) for sp in dd.select("span")]
                vals = [v for v in vals if v and v != ","]
                if vals:
                    return "، ".join(dict.fromkeys(vals))
    return None


def get_og_image(soup):
    tag = soup.find("meta", attrs={"property": "og:image"})
    if tag and (tag.get("content") or tag.get("value")):
        return tag.get("content") or tag.get("value")
    return None


def find_first_image_src(soup):
    img = (
        soup.select_one(".product-view .item .image img")
        or soup.select_one(".cover img")
        or soup.find("img")
    )
    if img and img.has_attr("src"):
        return img["src"]
    return None


def extract_attid(soup):
    og = get_og_image(soup)
    if og:
        m = re.search(r"[?&]AttID=(\d+)", og, re.I)
        if m:
            return int(m.group(1))
    for img in soup.find_all("img", src=True):
        m = re.search(r"[?&]AttID=(\d+)", img["src"], re.I)
        if m:
            return int(m.group(1))
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]attid=(\d+)", a["href"], re.I)
        if m:
            return int(m.group(1))
    return None


def parse_duration_and_episodes(soup):
    dur = None
    ep = None
    dur_keys = ["مدت", "مدت زمان", "زمان"]
    ep_keys = ["تعداد قسمت", "تعداد قطعه", "تعداد قطعات", "تعداد قسمت‌ها"]

    for dd in soup.select(".item-info dd.field"):
        s = dd.get_text(" ", strip=True)
        for k in dur_keys:
            if k in s and not dur:
                m = re.search(r"(\d{1,2}:\d{2}:\d{2}|\d{1,3}:\d{2})", s)
                if m:
                    dur = m.group(1)
        for k in ep_keys:
            if k in s and not ep:
                m = re.search(r"(\d+)", s)
                if m:
                    ep = int(m.group(1))

    for dt in soup.select("#tags dt"):
        t = dt.get_text(strip=True)
        if any(k in t for k in dur_keys) and not dur:
            dd = dt.find_next_sibling("dd")
            if dd:
                s = dd.get_text(" ", strip=True)
                m = re.search(r"(\d{1,2}:\d{2}:\d{2}|\d{1,3}:\d{2})", s)
                if m:
                    dur = m.group(1)
        if any(k in t for k in ep_keys) and not ep:
            dd = dt.find_next_sibling("dd")
            if dd:
                m = re.search(r"(\d+)", dd.get_text(" ", strip=True))
                if m:
                    ep = int(m.group(1))

    return dur, ep


def build_player_link(audio_id, attid):
    return f"https://player.iranseda.ir/book-player/?VALID=TRUE&g={audio_id}&attid={attid}"

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

    data = {}
    data["Book_Title"] = text_or_none(soup.select_one("h1.titel")) or text_or_none(soup.find("h1"))

    # Short description: prefer section content, fallback to meta tags
    desc = text_or_none(soup.select_one("#about .body-module"))
    if not desc:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find(
            "meta", attrs={"property": "og:description"}
        )
        if meta and meta.get("content"):
            desc = meta.get("content").strip()
    data["Book_Description"] = desc

    # Full detail
    detail = text_or_none(soup.select_one("#review .body-module .more")) or text_or_none(
        soup.select_one("#review .body-module")
    )
    if not detail:
        detail_el = soup.find(
            "div",
            class_=lambda c: c and "full" in c and "description" in c,
        )
        detail = text_or_none(detail_el)
    data["Book_Detail"] = detail

    lang_meta = soup.find("meta", {"property": "og:locale"})
    data["Book_Language"] = "فارسی" if (lang_meta and "fa" in lang_meta.get("content", "")) else None
    data["Book_Country"] = parse_from_metadata_list(soup, "کشور")

    data["Book_Author"] = (
        parse_label_from_iteminfo(soup, "نویسنده")
        or parse_from_metadata_list(soup, "عنوان كتاب مرجع")
        or parse_from_metadata_list(soup, "نویسنده")
    )
    data["Book_Translator"] = parse_from_metadata_list(soup, "ترجمه")
    data["Book_Narrator"] = parse_from_metadata_list(soup, "راوی")
    data["Book_Director"] = parse_label_from_iteminfo(soup, "کارگردان") or parse_from_metadata_list(
        soup, "کارگردان"
    )
    data["Book_Producer"] = parse_from_metadata_list(soup, "تهیه‌کننده")
    data["Book_SoundEngineer"] = parse_from_metadata_list(soup, "صدابردار")
    data["Book_Effector"] = parse_from_metadata_list(soup, "افکتور") or parse_from_metadata_list(
        soup, "افكتور"
    )
    data["Book_Actors"] = parse_from_metadata_list(soup, "بازیگران")
    data["Book_Genre"] = parse_from_metadata_list(soup, "کلمه کلیدی") or parse_from_metadata_list(
        soup, "نوع متن"
    )
    data["Book_Category"] = parse_from_metadata_list(soup, "دسته بندی ها") or parse_label_from_iteminfo(
        soup, "دسته‌بندی"
    )

    dur_txt, ep_cnt = parse_duration_and_episodes(soup)
    data["Book_Duration"] = dur_txt
    data["Episode_Count"] = ep_cnt

    cover = get_og_image(soup) or find_first_image_src(soup)
    if cover:
        cover = abs_url(cover)
    data["Cover_Image_URL"] = cover

    attid = extract_attid(soup)
    try:
        q = parse_qs(urlparse(url).query)
        g = q.get("g", [None])[0]
        data["AudioBook_ID"] = g
    except Exception:
        data["AudioBook_ID"] = None

    data["Player_Link"] = build_player_link(data.get("AudioBook_ID"), attid) if data.get("AudioBook_ID") and attid else None
    data["attid"] = attid

    return data

def main():
    if GSHEET_URL:
        df_in = read_gsheet(GSHEET_URL)
        if df_in is None or df_in.empty:
            print("ERROR: No data found in Google Sheet.")
            sys.exit(1)
    else:
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
            parsed["Book_Summary"] = row.get("Summary") or row.get("Book_Summary")
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
