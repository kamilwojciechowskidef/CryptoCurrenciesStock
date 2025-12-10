import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from db.db import list_coins, get_history_all
from datetime import datetime, timezone, timedelta

print(list_coins())

start = datetime.now(timezone.utc) - timedelta(days=30)
end = datetime.now(timezone.utc)

print(get_history_all(start, end).head())
