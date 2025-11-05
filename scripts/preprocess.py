import json, csv, math
from pathlib import Path
from collections import defaultdict
import pandas as pd

# ---------- File paths ----------
DATA_DIR = Path("data")
OUT_CSV = DATA_DIR / "master_features_by_code.csv"
OUT_CODES = DATA_DIR / "data_codes.json"

ISO_TAB = DATA_DIR / "iso-639-3.tab"
GLOTTO_CSV = DATA_DIR / "glottolog_languages.csv"
WIKI_JSON = DATA_DIR / "wiki_code_to_iso_code.json"
ETHNO_CODE_LIST = DATA_DIR / "ethnologue_code_list.txt"
MACROLANG_CSV = DATA_DIR / "ethnologue_macrolanguages.csv"

JSON_FEATURES = {
    "AdjustedWPsize": DATA_DIR / "iso_adjusted_wikipedia_sizes.json",
    "Articles": DATA_DIR / "Articles.json",
    "WPincubatornew": DATA_DIR / "WPincubatornew.json",
    "WPsizeinchars": DATA_DIR / "WPsizeinchars.json",
    "Realtotalratio": DATA_DIR / "Realtotalratio.json",
    "Avggoodpagelength": DATA_DIR / "Avggoodpagelength.json",
}

ETHNO_CSV = DATA_DIR / "ethnologue_language_data.csv"
JW_CSV = DATA_DIR / "jw_availability_by_iso.csv"
OS_CSV = DATA_DIR / "os_support_windows.csv"
TATOEBA_CSV = DATA_DIR / "tatoeba_sentences_by_language.csv"
WALS_CSV = DATA_DIR / "wals_languages.csv"


# ---------- Helper functions ----------
def read_json(p):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_iso_tab(p=ISO_TAB):
    iso_set = set()
    iso2map = {}
    if not p.exists():
        return iso_set, iso2map
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.reader(fh, delimiter="\t")
        next(rdr, None)
        for row in rdr:
            if not row:
                continue
            iso3 = row[0].strip().lower()
            iso_set.add(iso3)
            for part in (row[1:4] if len(row) >= 4 else row[1:]):
                if part:
                    iso2map[part.strip().lower()] = iso3
    return iso_set, iso2map


def load_ethno_code_list(p=ETHNO_CODE_LIST):
    codes = set()
    if not p.exists():
        return codes
    with p.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            code = line.strip().lower()
            if code:
                codes.add(code)
    return codes


def load_glottolog(p=GLOTTO_CSV):
    glotto_to_iso = {}
    iso_to_glotto = defaultdict(list)
    if not p.exists():
        return glotto_to_iso, iso_to_glotto
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
    if not p.exists():
        return out
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.DictReader(fh)
        fld = None
        for c in candidates:
            if c in (rdr.fieldnames or []):
                fld = c
                break
        for r in rdr:
            k = (r.get(fld) or "").strip().lower() if fld else ""
            if k:
                out[k] = r
    return out


def load_macrolanguage_codes(p=MACROLANG_CSV):
    macros = set()
    if not p.exists():
        return macros
    with p.open(encoding="utf-8", errors="replace") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            code = (row.get("ISO Code") or "").strip().lower()
            if code:
                macros.add(code)
    return macros


def safe_int(s):
    if s is None:
        return ""
    s = str(s).replace(",", "").strip()
    try:
        return int(float(s))
    except:
        return s


# ---------- Load base data ----------
iso_set, iso2map = load_iso_tab()
ethno_code_set = load_ethno_code_list()
glotto_to_iso, iso_to_glotto = load_glottolog()
wiki_map = read_json(WIKI_JSON) if WIKI_JSON.exists() else {}
macrolanguage_codes = load_macrolanguage_codes()

json_features = {k: read_json(p) for k, p in JSON_FEATURES.items()}
ethno_rows = load_csv_map(ETHNO_CSV, ["ISO Code", "ISO_Code"])
wals_rows = load_csv_map(WALS_CSV, ["ISO 639-3", "iso", "glottocode", "wals_code"])
jw_rows = load_csv_map(JW_CSV, ["ISO 639-2 Code", "ISO 639-3"])
os_rows = load_csv_map(OS_CSV, ["ISO 639-3 Code", "ISO 639-3"])
tatoeba_rows = load_csv_map(TATOEBA_CSV, ["ISO 639-3", "glottocode"])

jw_by_iso3 = {}
if JW_CSV.exists():
    with JW_CSV.open(encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh):
            iso2 = (r.get("ISO 639-2 Code") or "").strip().lower()
            val = (r.get("Does_have_bible") or "").strip()
            iso3 = iso2map.get(iso2)
            if iso3:
                jw_by_iso3[iso3] = val
            if iso2 and iso2 in iso_set:
                jw_by_iso3[iso2] = val

os_set = set(os_rows.keys())
wals_keys = set(wals_rows.keys())


# ---------- Build master list (Ethnologue first, then Glottolog-only) ----------
master_iso = set(ethno_code_set)
glotto_only_iso = {iso for iso in glotto_to_iso.values() if iso and iso not in master_iso}
master_iso.update(glotto_only_iso)

entries = []
for iso in sorted(master_iso):
    glottos = iso_to_glotto.get(iso, [])
    glotto = glottos[0] if glottos else ""
    entries.append((iso, iso, glotto))

# Optional: Add Glottolog-only (no ISO)
for glotto, iso in glotto_to_iso.items():
    if not iso:
        entries.append((glotto, "", glotto))


# ---------- Data preparation helpers ----------
def fetch_json(feat, iso, glotto):
    d = json_features.get(feat) or {}
    for key in (iso, glotto):
        if key and key in d:
            return d[key]
    return ""


def normalize_percent(value):
    if not isinstance(value, str):
        return value
    v = value.strip()
    if v.endswith("%"):
        try:
            num = float(v[:-1])
            if num == 0:
                return 0
            if num == 100:
                return 1
        except ValueError:
            pass
    return value


DIGITAL_SUPPORT_MAP = {"still": 0, "emerging": 1, "ascending": 2, "vital": 3}

def normalize_digital_support(value):
    if not isinstance(value, str):
        return value
    v = value.strip().lower()
    return DIGITAL_SUPPORT_MAP.get(v, "")

# Map population ranges to numeric codes
POP_MAP = {
    "None": 0,
    "Less than 10K": 3,
    "10K to 1M": 5,
    "1M to 1B": 7,
    "More than 1B": 9,
}


# ---------- Build dataframe ----------
header = [
    "code", "iso639_3", "glottocode",
    "AdjustedWPsize", "Articles", "WPincubatornew", "WPsizeinchars", "Realtotalratio", "Avggoodpagelength",
    "Population Size", "Institutional (%)", "Stable (%)", "Endangered (%)", "Extinct (%)",
    "Digital Support", "has_glottolog", "Does_have_bible", "win11_os_supported",
    "tatoeba_sentences", "has_wals", "is_macrolanguage",
]

rows = []
for master_code, iso, glotto in entries:
    r = {"code": master_code, "iso639_3": iso, "glottocode": glotto}
    for feat in ["AdjustedWPsize", "Articles", "WPincubatornew", "WPsizeinchars", "Realtotalratio", "Avggoodpagelength"]:
        r[feat] = fetch_json(feat, iso, glotto)

    # --- Apply log10 transformation to selected numeric features ---
    for feat in ["AdjustedWPsize", "Articles", "WPsizeinchars", "Avggoodpagelength"]:
        val = r.get(feat)
        try:
            n = float(val)
            if n > 0:  # avoid log(0)
                r[feat] = round(math.log10(n), 6)
            else:
                r[feat] = ""
        except Exception:
            r[feat] = ""


    eth = ethno_rows.get(iso) or ethno_rows.get(glotto) or {}
    pop_val = eth.get("Population Size", "")

    v = pop_val.strip()
    if v in POP_MAP.keys():
        r["Population Size"] = POP_MAP[v]
    else:
        r["Population Size"] = ""
    

    for fld in ["Institutional (%)", "Stable (%)", "Endangered (%)", "Extinct (%)", "Digital Support"]:
        val = eth.get(fld, "") if isinstance(eth, dict) else ""
        if fld == "Digital Support":
            r[fld] = normalize_digital_support(val)
        else:
            r[fld] = normalize_percent(val)

    r["has_glottolog"] = int(bool(iso_to_glotto.get(iso) or glotto_to_iso.get(glotto)))
    r["Does_have_bible"] = jw_by_iso3.get(iso, "")
    r["win11_os_supported"] = 1 if (iso in os_rows or glotto in os_rows) else 0

    trow = tatoeba_rows.get(iso) or tatoeba_rows.get(glotto) or {}
    sentences = ""
    if isinstance(trow, dict):
        sentences = trow.get("Sentences") or trow.get(" Sentences") or ""
        sentences = safe_int(sentences)
    r["tatoeba_sentences"] = sentences

    r["has_wals"] = int(iso in wals_keys or glotto in wals_keys)
    r["is_macrolanguage"] = 1 if (iso in macrolanguage_codes or glotto in macrolanguage_codes) else 0

    rows.append(r)


# ---------- Save output ----------
df = pd.DataFrame(rows, columns=header)
rename_map = {
    "Institutional (%)": "Institutional",
    "Stable (%)": "Stable",
    "Endangered (%)": "Endangered",
    "Extinct (%)": "Extinct",
}
df.rename(columns=rename_map, inplace=True)
df.to_csv(OUT_CSV, index=False, encoding="utf-8")
OUT_CODES.write_text(json.dumps(df["code"].tolist(), ensure_ascii=False, indent=2), encoding="utf-8")

print(f"Wrote {len(df)} codes -> {OUT_CODES}")
print(f"Wrote DataFrame -> {OUT_CSV}")
