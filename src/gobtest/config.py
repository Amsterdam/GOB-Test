import os

API_HOST = os.getenv('API_HOST', 'http://localhost:8141')
MANAGEMENT_API_HOST = os.getenv('MANAGEMENT_API_HOST', 'http://localhost:8143')
MANAGEMENT_API_BASE = f"{MANAGEMENT_API_HOST}/gob_management"
