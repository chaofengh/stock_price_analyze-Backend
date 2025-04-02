from datetime import datetime
import pytz

chicago_tz = pytz.timezone("America/Chicago")

timestamp = datetime.now(chicago_tz).strftime('%Y-%m-%d %H:%M:%S')
print(timestamp)