from pathlib import Path
import pytest
import production

class GuardDB:
    def guard(self):
        raise RuntimeError('Storage guard: existing database near budget')

def test_guard_before_writes(monkeypatch):
    monkeypatch.delenv('YAHOO_IGNORE_STORAGE_BUDGET',raising=False)
    with pytest.raises(RuntimeError,match='Storage guard'):
        production.run(GuardDB(),[],[],[],'stamp','sha',True)

def test_override_rejects_nonpilot(monkeypatch):
    monkeypatch.setenv('YAHOO_IGNORE_STORAGE_BUDGET','true')
    with pytest.raises(RuntimeError,match='restricted to pilot'):
        production.run(GuardDB(),[{'symbol':'OTHER'}],[],[],'stamp','sha',True)

def test_workflow_override():
    workflow = Path('.github/workflows/yahoo.yml').read_text()
    assert 'python fleet.py --limit 500 --minutes 300' in workflow
    assert 'timeout-minutes: 350' in workflow
    assert 'cancel-in-progress: false' in workflow
    assert 'YAHOO_IGNORE_STORAGE_BUDGET' not in workflow
