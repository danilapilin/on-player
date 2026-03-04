"""Configuration for call matcher."""

API_BASE = "http://assist.intmarksol.com/api/"
API_KEY = "cG9HF21o3y4K5B7G50wlgDU6be466Ha6kdWit1kH2JCKZa"

# Claude API for transcription analysis
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

# How many days back to check recordings
DEFAULT_LOOKBACK_DAYS = 5

# Max concurrent API requests
MAX_CONCURRENT_REQUESTS = 2  # сервер не выдерживает много параллельных
