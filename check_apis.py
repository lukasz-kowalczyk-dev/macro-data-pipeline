"""
Skrypt diagnostyczny — sprawdza co faktycznie zwracają IMF i OECD API.
Uruchom lokalnie: python check_apis.py
"""

import json
import requests

# =============================================================================
# IMF
# =============================================================================
print("=" * 60)
print("IMF API — test")
print("=" * 60)

imf_url = "https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH/USA"
print(f"URL: {imf_url}")

try:
    r = requests.get(imf_url, timeout=15)
    print(f"Status: {r.status_code}")
    data = r.json()
    print(f"Klucze główne: {list(data.keys())}")
    # Pokaż pierwsze 300 znaków odpowiedzi
    print("Odpowiedź (pierwsze 500 znaków):")
    print(json.dumps(data, indent=2)[:500])
except Exception as e:
    print(f"BŁĄD: {e}")

# =============================================================================
# OECD — próba różnych URL-i
# =============================================================================
print()
print("=" * 60)
print("OECD API — test różnych URL-i")
print("=" * 60)

oecd_urls = [
    # Próba 1 — obecny URL z naszego kodu
    "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_QNA@DF_QNA,/Q.POL.GDP.VPVOBARSA....?startPeriod=2020&format=jsondata",
    # Próba 2 — prostszy format
    "https://sdmx.oecd.org/public/rest/data/OECD,QNA,/Q.POL.B1_GE.VPVOBARSA?startPeriod=2020&format=jsondata",
    # Próba 3 — stary endpoint stats.oecd.org
    "https://stats.oecd.org/SDMX-JSON/data/QNA/POL.B1_GE.VPVOBARSA.Q/OECD?startTime=2020&endTime=2023",
]

for i, url in enumerate(oecd_urls, 1):
    print(f"\nPróba {i}: {url[:80]}...")
    try:
        r = requests.get(url, timeout=15, headers={"Accept": "application/json"})
        print(f"  Status: {r.status_code}")
        if r.status_code == 200:
            print("  OK! Pierwsze 300 znaków:")
            print(f"  {r.text[:300]}")
            break
    except Exception as e:
        print(f"  BŁĄD: {e}")
