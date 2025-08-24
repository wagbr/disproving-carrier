#!/usr/bin/env python
# wikidata_dump.py  - add properties to qids previously selected

import sys, time, json, csv, re, requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry

DEFAULT_ROOT = Path(r"C:/Users/wagbr/OneDrive/Documentos/Artigos/Probabilidade de Jesus ter existido")
WD_URL       = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
DATE_RE      = re.compile(r'^[+-]\d{4}-\d{2}-\d{2}T')
QID_RE       = re.compile(r"^Q\d+$")

SLEEP_SEC = 1.0          # ↓ 1 req/s recomendado

session = requests.Session()
session.headers.update({
    "User-Agent": "wd-prob-jesus/0.2 (wagbr@example.com) python-requests/%s"
                  % requests.__version__
})
retries = Retry(backoff_factor=2,
                total=5, status_forcelist=[502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

args = sys.argv[1:]
root_dir = DEFAULT_ROOT

if args and (":" in args[0] or "\\" in args[0] or "/" in args[0]):
    root_dir = Path(args.pop(0))

csv_interval = None
explicit_qids = None
if args:
    token = args[0]
    if QID_RE.match(token) or "," in token:           # Q999,Q123…
        explicit_qids = [q.strip() for q in token.split(",") if QID_RE.match(q.strip())]
    else:                                             # 100-150
        start, end = map(int, token.split("-", maxsplit=1)) if "-" in token else (1, int(token))
        csv_interval = (start, end)

data_dir  = root_dir / "data"
data_dir.mkdir(parents=True, exist_ok=True)
in_csv    = data_dir / "raw/qid_selected.csv"
out_csv   = data_dir / "processed/all_properties_long.csv"
cache_dir = root_dir / ".cache_wikidata"
cache_dir.mkdir(exist_ok=True)

def load_qids_from_csv(csv_path: Path) -> list[str]:
    df = pd.read_csv(csv_path, sep=';', dtype=str).dropna(subset=["qid"])
    return df["qid"].unique().tolist()

def cached_get_json(qid: str) -> dict:
    cache_file = cache_dir / f"{qid}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    url = WD_URL.format(qid) + "?flavor=dump&format=json"

    while True:
        resp = session.get(url, timeout=30)
        if resp.status_code == 429: 
            wait = int(resp.headers.get("Retry-After", 60))
            time.sleep(wait)
            continue
        resp.raise_for_status()

        if "application/json" not in resp.headers.get("Content-Type", ""):
            time.sleep(5) 
            continue
        data = resp.json()
        cache_file.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(SLEEP_SEC)
        return data

def unpack_value(dv):
    if not dv: return None
    v = dv.get("value")
    if isinstance(v, dict):
        if "time"   in v: return v["time"]
        if "id"     in v: return v["id"]
        if "amount" in v: return v["amount"]
    return v if isinstance(v, str) else None

def extract_claims(qid, entity_json):
    rows = []
    for prop, stmts in entity_json["entities"][qid]["claims"].items():
        for st in stmts:
            if not isinstance(st, dict): continue
            rank     = st.get("rank")
            snaktype = st["mainsnak"].get("snaktype")
            dv       = st["mainsnak"].get("datavalue")
            val      = unpack_value(dv) if snaktype == "value" else None
            rows.append((qid, prop, rank, snaktype, val))
    return rows

def iso_to_year(s):
    if s and DATE_RE.match(s):
        sign = -1 if s[0] == '-' else 1
        return sign * int(s[1:5])
    return None

def get_label_desc(qid: str):

    cache_file = cache_dir / f"{qid}_label.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"

    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            ent  = data["entities"][qid]
            label = ent.get("labels", {}).get("en", {}).get("value")
            desc  = ent.get("descriptions", {}).get("en", {}).get("value")
            break
        except Exception as e:
            if attempt == 2:
                label = desc = None
            else:
                time.sleep(2)

    cache_file.write_text(json.dumps({"label": label, "desc": desc}),
                          encoding="utf-8")
    time.sleep(SLEEP_SEC)
    return {"label": label, "desc": desc}

def main():

    if explicit_qids:
        qids = explicit_qids
        print(f"🔎 Processing Q-IDs({len(qids)})")
    else:
        if not in_csv.exists():
            sys.exit(f"❌ CSV {in_csv} not found.")
        qids = load_qids_from_csv(in_csv)
        if csv_interval:
            start, end = csv_interval
            qids = qids[(start-1):(end)]
            print(f"🔎 Rows {start}–{end} from CSV ({len(qids)})")
        else:
            print(f"🔎 Processing full CSV ({len(qids)} Q-IDs)")

    rows = []
    for qid in tqdm(qids, ncols=80, desc="JSON"):
        rows.extend(extract_claims(qid, cached_get_json(qid)))

    df = pd.DataFrame(rows, columns=["qid","property","rank","snaktype","value_raw"])
    df["value_year"] = df["value_raw"].apply(iso_to_year)

    qid_mask = df["value_raw"].str.match(r"^Q\d+$", na=False)
    val_qids = df.loc[qid_mask, "value_raw"].unique()
    label_val = {q: get_label_desc(q) for q in tqdm(val_qids, desc="value labels")}
    df["value_label"] = df["value_raw"].map(
        lambda x: label_val.get(x, {}).get("label") if isinstance(x, str) and x.startswith("Q") else None)
    df["value_desc"]  = df["value_raw"].map(
        lambda x: label_val.get(x, {}).get("desc")  if isinstance(x, str) and x.startswith("Q") else None)

    label_main = {q: get_label_desc(q) for q in tqdm(qids, desc="qid labels")}
    df["qid_label"] = df["qid"].map(lambda x: label_main[x]["label"])
    df["qid_desc"]  = df["qid"].map(lambda x: label_main[x]["desc"])

    df.to_csv(out_csv, sep=";", index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅  {len(df):,} rows recorded in {out_csv}")

if __name__ == "__main__":
    main()
