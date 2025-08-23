import importlib.util
import pathlib

# Load the script module dynamically to avoid path issues
script_path = pathlib.Path(__file__).resolve().parents[1] / 'script_iran_seda_final_STREAM_MERGE_v6_env.py'
spec = importlib.util.spec_from_file_location('script_module', script_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

parse_page = mod.parse_page


def test_cover_image_url_is_absolute():
    html = '''<html><head>
    <meta property="og:image" content="/images/cover.jpg?AttID=1234" />
    </head><body>
    <h1>Example Book</h1>
    <div class="short-description">Short</div>
    <div class="full-description">Full</div>
    </body></html>'''
    url = "https://book.iranseda.ir/Details?VALID=TRUE&g=5678"
    parsed = parse_page(html, url)
    assert parsed["Cover_Image_URL"] == "https://book.iranseda.ir/images/cover.jpg?AttID=1234"
