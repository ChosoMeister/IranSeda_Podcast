# Podcast-on-Pages (GitHub Pages)

این مخزن ابزاری برای تبدیل کتاب‌های صوتی ایران‌صدا به فید پادکست است.
اسکریپت‌ها مراحل جمع‌آوری لینک‌ها، استخراج اطلاعات و ساخت RSS را انجام می‌دهند و در نهایت می‌توان خروجی را روی **GitHub Pages** منتشر کرد.
هر اجرا با متغیر محیطی `RUN_NAME` جدا می‌شود تا خروجی‌ها در پوشه‌های مستقل ذخیره شوند.

## پیش‌نیازها
- Python 3.10 یا بالاتر
- کتابخانه‌های `requests`، `beautifulsoup4` و `pandas`

```bash
pip install requests beautifulsoup4 pandas
```

## مراحل اجرا

### ۱. دریافت فهرست کتاب‌ها
```bash
RUN_NAME=demo START_PAGE=1 END_PAGE=3 python scrape_iranseda_env.py
```
متغیرها:
- `RUN_NAME`: نام اجرا (پوشه‌ای با همین نام در `runs/` ساخته می‌شود).
- `SOURCE_URL`: آدرس صفحه تگ ایران‌صدا با `{}` برای شماره صفحه.
- `START_PAGE` و `END_PAGE`: محدوده صفحات.
- `RUNS_DIR`: مسیر ریشه ذخیره خروجی‌ها (پیش‌فرض `runs`).

خروجی: `runs/<RUN_NAME>/raw/audiobooks_<RUN_NAME>.csv`

### ۲. استخراج جزئیات و لینک MP3
```bash
RUN_NAME=demo python script_iran_seda_final_STREAM_MERGE_v6_env.py
```
این مرحله:
- برای هر سطر CSV مرحله قبل صفحه کتاب را باز می‌کند.
- اطلاعاتی مانند عنوان، توضیحات، نویسنده، تصویر و MP3 را استخراج می‌کند.
- فایل ادغام‌شده را در `runs/<RUN_NAME>/merged/books_with_attid_<RUN_NAME>.csv` ذخیره می‌کند.
- در صورت خطا، اطلاعات در `runs/<RUN_NAME>/errors/errors_<RUN_NAME>.csv` ثبت می‌شود.
- با تعیین متغیر محیطی `INPUT_CSV` می‌توان مسیر CSV ورودی دلخواه را مشخص کرد.

### ۳. ساخت فید پادکست
```bash
python tools/csv_to_podcast.py --csv runs/demo/merged/books_with_attid_demo.csv \
    --site https://<username>.github.io/<repo> --run-name demo
```
گزینه‌ها:
- `--channel-title`، `--channel-author` و `--channel-summary` برای سفارشی‌سازی اطلاعات کانال.
  مقادیر پیش‌فرض نویسنده و خلاصه به‌ترتیب «Mustafa Tayefi» و «جمع آوری بخشی از کتاب های صوتی موجود در سایت ایران صدا  در جهت استفاده در نرم افزار پادگیر» هستند.
خروجی: `public/feeds/<RUN_NAME>/podcast.xml`

توضیحات هر آیتم در فید از اطلاعات موجود در CSV ساخته می‌شود و شامل عنوان، توضیحات، نویسنده، مترجم، ژانر، مدت‌زمان و سایر متادیتا است.

### ۴. انتشار روی GitHub Pages
1. مخزن را روی گیت‌هاب آپلود کنید (برنچ `main`).
2. در **Settings → Pages**، حالت **Build and deployment: GitHub Actions** را انتخاب کنید.
3. ورک‌فلو **Build & Deploy Podcast RSS (GitHub Pages)** را اجرا کرده و مقادیر لازم را وارد کنید.
4. پس از اجرا فید در مسیر زیر در دسترس است:
   ```
   https://<username>.github.io/<repo>/feeds/<RUN_NAME>/podcast.xml
   ```

## ساختار پوشه‌ها
```
runs/<RUN_NAME>/raw/audiobooks_<RUN_NAME>.csv
runs/<RUN_NAME>/merged/books_with_attid_<RUN_NAME>.csv
runs/<RUN_NAME>/errors/errors_<RUN_NAME>.csv
public/feeds/<RUN_NAME>/podcast.xml
```

## تست‌ها
برای اطمینان از صحت عملکرد تجزیه صفحات، تست‌ها را اجرا کنید:
```bash
pytest
```
