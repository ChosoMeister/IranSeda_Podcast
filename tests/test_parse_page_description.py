import importlib.util
import pathlib

# Dynamically load the script module to avoid path issues
script_path = pathlib.Path(__file__).resolve().parents[1] / 'script_iran_seda_final_STREAM_MERGE_v6_env.py'
spec = importlib.util.spec_from_file_location('script_module', script_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

parse_page = mod.parse_page

def test_parse_page_uses_meta_description():
    html = """
    <html>
      <head>
        <meta name='description' content='meta description here'>
      </head>
      <body>
        <h1>عنوانی</h1>
      </body>
    </html>
    """
    result = parse_page(html, "http://example.com")
    assert result["Book_Description"] == "meta description here"
