import importlib.util
import pathlib
from unittest.mock import patch, MagicMock

script_path = pathlib.Path(__file__).resolve().parents[1] / 'script_iran_seda_final_STREAM_MERGE_v6_env.py'
spec = importlib.util.spec_from_file_location('script_module', script_path)
mod = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

@patch('script_module.requests.get')
def test_read_gsheet_single_url(mock_get):
    content = 'http://book.iranseda.ir/DetailsAlbum/?VALID=TRUE&g=674800'
    resp = MagicMock()
    resp.content = content.encode('utf-8')
    resp.raise_for_status = lambda: None
    mock_get.return_value = resp
    df = mod.read_gsheet('https://docs.google.com/spreadsheets/d/test/export?format=csv')
    assert not df.empty
    assert df.iloc[0]['URL'] == content
    assert df.iloc[0]['AudioBook_ID'] == '674800'
