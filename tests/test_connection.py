from db.db import list_coins, get_history_all
from datetime import datetime, timezone, timedelta

print(list_coins())

start = datetime.now(timezone.utc) - timedelta(days=30)
end = datetime.now(timezone.utc)

print(get_history_all(start, end).head())
