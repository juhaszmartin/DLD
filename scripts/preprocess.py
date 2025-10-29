# %% [code]
import json, csv
from pathlib import Path
from collections import defaultdict
import pandas as pd

DATA_DIR = Path("data")
OUT_CSV = DATA_DIR / "master_features_by_code.csv"
OUT_CODES = DATA_DIR / "data_codes.json"

ISO_TAB = DATA_DIR / "iso-639-3.tab"
GLOTTO_CSV = DATA_DIR / "glottolog_languages.csv"
WIKI_JSON = DATA_DIR / "wiki_code_to_iso_code.json"

JSON_FEATURES = {
    "adjustedwpsize": DATA_DIR / "iso_adjusted_wikipedia_sizes.json",
    "articles": DATA_DIR / "Articles.json",
    "wpincubatornew": DATA_DIR / "WPincubatornew.json",
    "wpsizeinchars": DATA_DIR / "WPsizeinchars.json",
    "realtotalratio": DATA_DIR / "Realtotalratio.json",
    "avggoodpagelength": DATA_DIR / "Avggoodpagelength.json",
}

ETHNO_CSV = DATA_DIR / "ethnologue_language_data.csv"
JW_CSV = DATA_DIR / "jw_availability_by_iso.csv"
OS_CSV = DATA_DIR / "os_support_windows.csv"
TATOEBA_CSV = DATA_DIR / "tatoeba_sentences_by_language.csv"
WALS_CSV = DATA_DIR / "wals_languages.csv"


def read_json(p):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_iso_tab(p=ISO_TAB):
    iso_set = set(); iso2map = {}
    if not p.exists(): return iso_set, iso2map
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.reader(fh, delimiter="\t")
        next(rdr, None)
        for row in rdr:
            if not row: continue
            iso3 = row[0].strip().lower()
            iso_set.add(iso3)
            for part in (row[1:4] if len(row) >= 4 else row[1:]):
                if part:
                    iso2map[part.strip().lower()] = iso3
    return iso_set, iso2map


def load_glottolog(p=GLOTTO_CSV):
    glotto_to_iso = {}; iso_to_glotto = defaultdict(list)
    if not p.exists(): return glotto_to_iso, iso_to_glotto
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            glt = (r.get("Glottocode") or "").strip().lower()
            iso = (r.get("ISO-639-3") or r.get("ISO_639_3") or "").strip().lower()
            if glt:
                glotto_to_iso[glt] = iso or None
            if iso and glt:
                iso_to_glotto[iso].append(glt)
    return glotto_to_iso, iso_to_glotto


def load_csv_map(p, candidates):
    out = {}
    if not p.exists(): return out
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.DictReader(fh)
        fld = None
        for c in candidates:
            if c in (rdr.fieldnames or []):
                fld = c; break
        for r in rdr:
            k = (r.get(fld) or "").strip().lower() if fld else ""
            if k:
                out[k] = r
    return out


def safe_int(s):
    if s is None: return ""
    s = str(s).replace(",", "").strip()
    try: return int(float(s))
    except: return s


# --- Load data ---
iso_set, iso2map = load_iso_tab()
glotto_to_iso, iso_to_glotto = load_glottolog()
wiki_map = read_json(WIKI_JSON) if WIKI_JSON.exists() else {}

json_features = {k: read_json(p) for k, p in JSON_FEATURES.items()}
ethno_rows = load_csv_map(ETHNO_CSV, ["ISO Code","ISO_Code"])
wals_rows = load_csv_map(WALS_CSV, ["ISO 639-3","iso","glottocode","wals_code"])
jw_rows = load_csv_map(JW_CSV, ["ISO 639-2 Code","ISO 639-3"])
os_rows = load_csv_map(OS_CSV, ["ISO 639-3 Code","ISO 639-3"])
tatoeba_rows = load_csv_map(TATOEBA_CSV, ["ISO 639-3","glottocode"])

jw_by_iso3 = {}
if JW_CSV.exists():
    with JW_CSV.open(encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh):
            iso2 = (r.get("ISO 639-2 Code") or "").strip().lower()
            val = (r.get("Does_have_bible") or "").strip()
            iso3 = iso2map.get(iso2)
            if iso3: jw_by_iso3[iso3] = val
            if iso2 and iso2 in iso_set: jw_by_iso3[iso2] = val

os_set = set(os_rows.keys())
wals_keys = set(wals_rows.keys())

# --- Build master list of codes ---
master = set(iso_set) | set(glotto_to_iso.keys())
for d in json_features.values():
    if isinstance(d, dict): master.update(k.lower() for k in d.keys() if isinstance(k, str))
for m in [ethno_rows, wals_rows, os_rows, tatoeba_rows]:
    master.update(k.lower() for k in m.keys())
master.discard("")

entries = []
for raw in sorted(master):
    iso = glotto_to_iso.get(raw) if raw in glotto_to_iso else (raw if raw in iso_set else "")
    glotto = ""
    if raw in iso_to_glotto:
        glottos = iso_to_glotto[raw]
        if glottos:
            glotto = glottos[0]
    if not iso and raw in iso_set:
        iso = raw
    if not glotto and raw in glotto_to_iso:
        glotto = raw
    master_code = iso or glotto
    entries.append((master_code, iso, glotto))


def fetch_json(feat, iso, glotto):
    d = json_features.get(feat) or {}
    for key in (iso, glotto):
        if key and key in d:
            return d[key]
    return ""


header = [
    "code", "iso639_3", "glottocode",
    "adjustedwpsize","articles","wpincubatornew","wpsizeinchars","realtotalratio","avggoodpagelength",
    "Population Size","Institutional (%)","Stable (%)","Endangered (%)","Extinct (%)","Digital Support",
    "has_glottolog","Does_have_bible","os_supported","tatoeba_sentences","has_wals"
]

rows = []
for master_code, iso, glotto in entries:
    r = {"code": master_code, "iso639_3": iso, "glottocode": glotto}
    for feat in ["adjustedwpsize","articles","wpincubatornew","wpsizeinchars","realtotalratio","avggoodpagelength"]:
        r[feat] = fetch_json(feat, iso, glotto)

    eth = ethno_rows.get(iso) or ethno_rows.get(glotto) or {}
    r["Population Size"] = eth.get("Population Size", eth.get("Population", "")) if isinstance(eth, dict) else ""
    for fld in ["Institutional (%)","Stable (%)","Endangered (%)","Extinct (%)","Digital Support"]:
        r[fld] = eth.get(fld, "") if isinstance(eth, dict) else ""

    r["has_glottolog"] = int(bool(iso_to_glotto.get(iso) or glotto_to_iso.get(glotto)))
    r["Does_have_bible"] = jw_by_iso3.get(iso, "")
    r["os_supported"] = 1 if (iso in os_rows or glotto in os_rows) else 0

    trow = tatoeba_rows.get(iso) or tatoeba_rows.get(glotto) or {}
    sentences = ""
    if isinstance(trow, dict):
        sentences = trow.get("Sentences") or trow.get(" Sentences") or ""
        sentences = safe_int(sentences)
    r["tatoeba_sentences"] = sentences

    r["has_wals"] = int(iso in wals_keys or glotto in wals_keys)
    rows.append(r)


# --- Save using pandas ---
df = pd.DataFrame(rows, columns=header)
df.to_csv(OUT_CSV, index=False, encoding="utf-8")
OUT_CODES.write_text(json.dumps(df["code"].tolist(), ensure_ascii=False, indent=2), encoding="utf-8")

print(f"Wrote {len(df)} codes -> {OUT_CODES}")
print(f"Wrote DataFrame -> {OUT_CSV}")
