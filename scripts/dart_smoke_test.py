import os
import requests

# API = os.getenv("DART_API_KEY")
API = "b347a315a55beb3d6826379f95c06d2b48b8131a"
assert API, "DART_API_KEY not set"

# 삼성전자 corp_code는 corpCode.xml에서 찾아야 하지만,
# 먼저 '키 자체가 동작'하는지 확인하기 위해 간단 endpoint를 호출:
url = "https://opendart.fss.or.kr/api/list.json"
params = {
    "crtfc_key": API,
    "page_no": 1,
    "page_count": 1,
    "bgn_de": "20240101",
    "end_de": "20240131",
}
r = requests.get(url, params=params, timeout=30)
print("status", r.status_code)
print(r.text[:300])
