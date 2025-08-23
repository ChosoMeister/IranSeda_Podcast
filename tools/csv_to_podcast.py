# -*- coding: utf-8 -*-
import argparse, csv, hashlib, os
from datetime import datetime, timezone
from xml.sax.saxutils import escape
import requests

# Maximum length allowed for the description field.  If the generated
# description exceeds this value the script will gradually drop optional
# metadata fields (see `field_map` in `build_item`).  The value is chosen to
# stay well below common limits enforced by podcast directories such as
# Apple Podcasts.
MAX_DESC_LENGTH = 4000

def now_rfc822():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")

def safe_get(row, key, default=""):
    v = row.get(key, default)
    if v is None: return default
    return str(v).strip()

def cdata(text):
    if text is None: text = ""
    text = str(text)
    if "]]>" in text: text = text.replace("]]>", "]]]]><![CDATA[>")
    return "<![CDATA[" + text + "]]>"

def read_rows(path):
    for enc in ("utf-8-sig","utf-8","cp1256"):
        try:
            import csv as _csv
            with open(path, "r", encoding=enc, newline="") as f:
                return [dict(r) for r in _csv.DictReader(f)]
        except Exception:
            continue
    raise RuntimeError("Cannot read CSV.")


def fetch_audio_length(url: str) -> int:
    """Return Content-Length of an MP3 after validating required headers.

    Raises RuntimeError if `Content-Type` is not `audio/mpeg` or if
    `Content-Length` is missing. The `Accept-Ranges` header used to be
    required, but many servers (including IranSeda) either omit it or set it
    to values other than ``bytes``. The script now tolerates such responses
    as long as a valid ``Content-Length`` is provided.
    """
    r = requests.head(url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    headers = {k.lower(): v for k, v in r.headers.items()}
    ctype = headers.get("content-type", "").split(";")[0].strip().lower()
    if ctype != "audio/mpeg":
        raise RuntimeError(f"Invalid Content-Type: {headers.get('content-type')}")
    # Accept-Ranges might be missing or set to 'none'. We no longer require
    # it to be ``bytes`` because some audio hosts do not advertise byte-range
    # support even though the content is downloadable.
    if "content-length" not in headers:
        raise RuntimeError("Missing Content-Length")
    return int(headers["content-length"])

def build_item(row, pubdate):
    title = safe_get(row, "Book_Title") or "عنوان بدون نام"
    audio = safe_get(row, "FullBook_MP3_URL") or safe_get(row, "Player_Link")
    if not audio:
        return None

    image = safe_get(row, "Cover_Image_URL")
    duration = safe_get(row, "Book_Duration")
    author = safe_get(row, "Book_Producer") or safe_get(row, "Book_Author")
    category = safe_get(row, "Book_Category") or safe_get(row, "Book_Genre")
    lang = safe_get(row, "Book_Language") or "fa"
    country = safe_get(row, "Book_Country")

    # Build a rich description from available metadata fields.  Each entry in
    # ``field_map`` is ``(csv_key, label, optional)`` where ``optional`` marks
    # fields that may be removed if the description grows too long.
    field_map = [
        ("Book_Title", "عنوان کتاب", False),
        ("Book_Description", "توضیحات کتاب", False),
        ("Book_Detail", "جزئیات/متن کامل معرفی", False),
        ("Book_Language", "زبان کتاب", True),
        ("Book_Country", "کشور (منشأ یا مخاطب)", True),
        ("Book_Author", "نویسنده", True),
        ("Book_Translator", "مترجم", True),
        ("Book_Narrator", "گوینده", True),
        ("Book_Director", "کارگردان", True),
        ("Book_Producer", "تهیه‌کننده/ناشر صوتی", True),
        ("Book_SoundEngineer", "صدابردار/مهندس صدا", True),
        ("Book_Effector", "افکت‌گذار", True),
        ("Book_Actors", "بازیگران (اگر نمایشی است)", True),
        ("Book_Genre", "ژانر", False),
        ("Book_Category", "دسته‌بندی", False),
        ("Book_Duration", "مدت زمان", False),
        ("Episode_Count", "تعداد اپیزود (اگر چندقسمتی است)", True),
        ("Cover_Image_URL", "آدرس تصویر کاور", True),
    ]

    # Collect available pieces of description
    desc_entries = []  # list of (optional, text)
    for key, label, optional in field_map:
        val = safe_get(row, key)
        if val:
            desc_entries.append((optional, f"{label}: {val}"))

    # If description is too long, iteratively drop optional fields from the end
    def _join(entries):
        return "<br/>".join(text for _, text in entries)

    desc = _join(desc_entries)
    if len(desc) > MAX_DESC_LENGTH:
        trimmed = desc_entries[:]
        while len(_join(trimmed)) > MAX_DESC_LENGTH and any(opt for opt, _ in trimmed):
            for i in range(len(trimmed) - 1, -1, -1):
                if trimmed[i][0]:
                    trimmed.pop(i)
                    break
        desc = _join(trimmed)

    try:
        length = fetch_audio_length(audio)
    except Exception as e:
        # If we cannot determine the length or content type, skip this entry
        # instead of terminating the whole feed generation process. This can
        # happen when the remote server responds with HTML or another
        # unexpected payload when a HEAD request is made.
        print(f"Skipping {audio}: {e}")
        return None
    guid_str = hashlib.sha1(audio.encode("utf-8")).hexdigest()
    parts = []
    parts.append("    <item>")
    parts.append("      <title>" + escape(title) + "</title>")
    parts.append("      <description>" + cdata(desc) + "</description>")
    parts.append('      <enclosure url="' + escape(audio) + '" length="' + str(length) + '" type="audio/mpeg"/>')
    parts.append('      <guid isPermaLink="false">' + guid_str + "</guid>")
    parts.append("      <pubDate>" + pubdate + "</pubDate>")
    if author:
        parts.append("      <itunes:author>" + escape(author) + "</itunes:author>")
    if duration:
        parts.append("      <itunes:duration>" + escape(duration) + "</itunes:duration>")
    if image:
        parts.append('      <itunes:image href="' + escape(image) + '"/>')
    if category:
        parts.append("      <category>" + escape(category) + "</category>")
    if lang:
        parts.append("      <language>" + escape(lang) + "</language>")
    if country:
        parts.append("      <itunes:keywords>" + escape(country) + "</itunes:keywords>")
    parts.append("    </item>")
    return "\n".join(parts)

def main():
    import argparse, os
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", help="Exact output file path")
    ap.add_argument("--out-dir", default="public/feeds")
    ap.add_argument("--run-name", default=os.getenv("RUN_NAME","latest"))
    ap.add_argument("--site", required=True)
    ap.add_argument("--channel-title", default="کتاب‌های صوتی من")
    ap.add_argument("--channel-author", default="Mustafa Tayefi")
    ap.add_argument("--channel-summary", default="جمع آوری بخشی از کتاب های صوتی موجود در سایت ایران صدا  در جهت استفاده در نرم افزار پادگیر")
    args = ap.parse_args()

    rows = read_rows(args.csv)
    pubdate = now_rfc822()
    cover = rows and safe_get(rows[0], "Cover_Image_URL") or ""

    items = []
    for r in rows:
        it = build_item(r, pubdate)
        if it: items.append(it)

    rss_parts = []
    rss_parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    rss_parts.append('<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:atom="http://www.w3.org/2005/Atom">')
    rss_parts.append("  <channel>")
    rss_parts.append("    <title>"+escape(args.channel_title)+"</title>")
    rss_parts.append("    <link>"+escape(args.site)+"</link>")
    rss_parts.append("    <language>fa</language>")
    rss_parts.append("    <lastBuildDate>"+pubdate+"</lastBuildDate>")
    rss_parts.append("    <itunes:author>"+escape(args.channel_author)+"</itunes:author>")
    rss_parts.append("    <itunes:summary>"+escape(args.channel_summary)+"</itunes:summary>")
    rss_parts.append("    <description>"+cdata(args.channel_summary)+"</description>")
    if cover: rss_parts.append('    <itunes:image href="'+escape(cover)+'"/>')
    rss_parts.append('    <atom:link href="'+escape(args.site.rstrip("/"))+'/feeds/'+escape(args.run_name)+'/podcast.xml" rel="self" type="application/rss+xml" />')
    rss_parts.append("\n".join(items))
    rss_parts.append("  </channel>")
    rss_parts.append("</rss>")

    out_file = args.out or os.path.join(args.out_dir, args.run_name, "podcast.xml")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(rss_parts))
    print("Wrote:", out_file)

if __name__ == "__main__":
    main()
