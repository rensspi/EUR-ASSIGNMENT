
from pathlib import Path
import pandas as pd
import numpy as np

# =============================================================================
# CONFIG – PAS ALLEEN HIER AAN
# =============================================================================
INPUT_CSV      = Path(r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\70_Resultaten\DAMO_W.HydroObject_H.csv")  # <— wijzig naar jouw CSV-pad
OUTPUT_XLSX    = Path(r"C:\Users\RESP\Documents\Bronbestanden\DAMO_H_CSV\DAMO_W.HydroObject_H_Classified.xlsx")                              # None = automatisch: classified_<input>.xlsx
SUFFIX         = "_last_changed"                  # suffix van datumkolommen (bv. "_last_edit")
AUTO_DETECT    = False                              # True = autodetectie van datumkolommen (>=70% parsebaar)
DATE_COLS      = None                               # expliciet: ["KolomA", "KolomB", ...] of None
SEP            = ","                                 # scheidingsteken van CSV (bv. ";" )
ENCODING       = None                                # bv. "utf-8" of "latin1"; None = auto/fallback
REFERENCE_NOW  = None                                # bv. "2025-12-30 00:00:00"; None = huidige tijd
# =============================================================================

# -------------------------------
# Detecteer datumkolommen
# -------------------------------
def detect_date_columns(df: pd.DataFrame, suffix: str = "_last_changed",
                        auto_detect: bool = False, explicit: list = None):
    if explicit:
        cols = [c for c in explicit if c in df.columns]
    else:
        cols = [c for c in df.columns if c.lower().endswith(suffix.lower())]
        if auto_detect:
            for c in df.columns:
                if c in cols:
                    continue
                s = df[c].astype(str).str.strip()
                non_empty = s.replace({"": np.nan}).dropna()
                if len(non_empty) == 0:
                    continue
                parsed = pd.to_datetime(non_empty, errors="coerce", utc=False)
                ratio = parsed.notna().mean()
                if ratio >= 0.7:
                    cols.append(c)
    return cols

# -------------------------------------------
# Classificeer leeftijds-buckets per record
# -------------------------------------------
def classify_age_buckets(df_dates: pd.DataFrame, now: pd.Timestamp):
    df_dates = df_dates.apply(pd.to_datetime, errors="coerce")
    cutoff_3m = now - pd.DateOffset(months=3)
    cutoff_6m = now - pd.DateOffset(months=6)
    cutoff_1y = now - pd.DateOffset(years=1)
    cutoff_2y = now - pd.DateOffset(years=2)
    cutoff_5y = now - pd.DateOffset(years=5)

    valid_past    = (df_dates.notna()) & (df_dates <= now)
    n_valid_past  = valid_past.sum(axis=1)

    cnt_0_3m      = ((df_dates >= cutoff_3m) & (df_dates <= now)).sum(axis=1)
    cnt_3_6m      = ((df_dates >= cutoff_6m) & (df_dates <  cutoff_3m)).sum(axis=1)
    cnt_6_12m     = ((df_dates >= cutoff_1y) & (df_dates <  cutoff_6m)).sum(axis=1)
    cnt_1_2y      = ((df_dates >= cutoff_2y) & (df_dates <  cutoff_1y)).sum(axis=1)
    cnt_2_5y      = ((df_dates >= cutoff_5y) & (df_dates <  cutoff_2y)).sum(axis=1)
    cnt_5y_plus   = ((df_dates <  cutoff_5y)).sum(axis=1)

    future_cnt    = ((df_dates >  now)).sum(axis=1)
    missing_cnt   = df_dates.isna().sum(axis=1)
    with_date_cnt = df_dates.notna().sum(axis=1)

    # Percentages tov geldige verleden-datums; 0% als er geen geldige waarden zijn
    denom = n_valid_past.replace(0, np.nan)
    pct_0_3m    = (cnt_0_3m    / denom * 100).fillna(0)
    pct_3_6m    = (cnt_3_6m    / denom * 100).fillna(0)
    pct_6_12m   = (cnt_6_12m   / denom * 100).fillna(0)
    pct_1_2y    = (cnt_1_2y    / denom * 100).fillna(0)
    pct_2_5y    = (cnt_2_5y    / denom * 100).fillna(0)
    pct_5y_plus = (cnt_5y_plus / denom * 100).fillna(0)

    return pd.DataFrame({
        "aantal_attributen_met_datum": with_date_cnt,
        "aantal_geldige_voor_verleden": n_valid_past,
        "aantal_missend": missing_cnt,
        "aantal_toekomst": future_cnt,
        "cnt_0_3m":      cnt_0_3m,
        "cnt_3_6m":      cnt_3_6m,
        "cnt_6_12m":     cnt_6_12m,
        "cnt_1_2y":      cnt_1_2y,
        "cnt_2_5y":      cnt_2_5y,
        "cnt_5y_plus":   cnt_5y_plus,
        "pct_0_3m":      pct_0_3m.round(2),
        "pct_3_6m":      pct_3_6m.round(2),
        "pct_6_12m":     pct_6_12m.round(2),
        "pct_1_2y":      pct_1_2y.round(2),
        "pct_2_5y":      pct_2_5y.round(2),
        "pct_5y_plus":   pct_5y_plus.round(2),
    })

# --- Samenvattingen per kolom en totaal ---

def summarize_column(col: pd.Series, now: pd.Timestamp) -> dict:
    s = pd.to_datetime(col, errors='coerce')
    total = s.notna().sum()
    valid_past = s.notna() & (s <= now)
    n_valid = valid_past.sum()
    cutoff_3m = now - pd.DateOffset(months=3)
    cutoff_6m = now - pd.DateOffset(months=6)
    cutoff_1y = now - pd.DateOffset(years=1)
    cutoff_2y = now - pd.DateOffset(years=2)
    cutoff_5y = now - pd.DateOffset(years=5)

    cnt_0_3m   = ((s >= cutoff_3m) & (s <= now)).sum()
    cnt_3_6m   = ((s >= cutoff_6m) & (s <  cutoff_3m)).sum()
    cnt_6_12m  = ((s >= cutoff_1y) & (s <  cutoff_6m)).sum()
    cnt_1_2y   = ((s >= cutoff_2y) & (s <  cutoff_1y)).sum()
    cnt_2_5y   = ((s >= cutoff_5y) & (s <  cutoff_2y)).sum()
    cnt_5y_plus= ((s <  cutoff_5y)).sum()

    def pct(c):
        return float(0 if n_valid == 0 else round(c / n_valid * 100, 2))

    return {
        'aantal_met_datum': int(total),
        'aantal_geldig_verleden': int(n_valid),
        'aantal_toekomst': int((s > now).sum()),
        'aantal_missend': int(s.isna().sum()),
        'cnt_0_3m': int(cnt_0_3m), 'pct_0_3m': pct(cnt_0_3m),
        'cnt_3_6m': int(cnt_3_6m), 'pct_3_6m': pct(cnt_3_6m),
        'cnt_6_12m': int(cnt_6_12m), 'pct_6_12m': pct(cnt_6_12m),
        'cnt_1_2y': int(cnt_1_2y), 'pct_1_2y': pct(cnt_1_2y),
        'cnt_2_5y': int(cnt_2_5y), 'pct_2_5y': pct(cnt_2_5y),
        'cnt_5y_plus': int(cnt_5y_plus), 'pct_5y_plus': pct(cnt_5y_plus),
    }


def summarize_overall(df_dates: pd.DataFrame, now: pd.Timestamp) -> pd.Series:
    s = pd.to_datetime(df_dates.stack(), errors='coerce')
    s = s.dropna()
    total = len(s)
    valid = s[s <= now]
    n_valid = len(valid)
    cutoff_3m = now - pd.DateOffset(months=3)
    cutoff_6m = now - pd.DateOffset(months=6)
    cutoff_1y = now - pd.DateOffset(years=1)
    cutoff_2y = now - pd.DateOffset(years=2)
    cutoff_5y = now - pd.DateOffset(years=5)

    def cnt(cond):
        return int(cond.sum())

    cnt_0_3m   = cnt((valid >= cutoff_3m) & (valid <= now))
    cnt_3_6m   = cnt((valid >= cutoff_6m) & (valid <  cutoff_3m))
    cnt_6_12m  = cnt((valid >= cutoff_1y) & (valid <  cutoff_6m))
    cnt_1_2y   = cnt((valid >= cutoff_2y) & (valid <  cutoff_1y))
    cnt_2_5y   = cnt((valid >= cutoff_5y) & (valid <  cutoff_2y))
    cnt_5y_plus= cnt((valid <  cutoff_5y))

    def pct(c):
        return 0.0 if n_valid == 0 else round(c / n_valid * 100, 2)

    return pd.Series({
        'aantal_met_datum': int(total),
        'aantal_geldig_verleden': int(n_valid),
        'aantal_toekomst': int((s > now).sum()),
        'cnt_0_3m': cnt_0_3m, 'pct_0_3m': pct(cnt_0_3m),
        'cnt_3_6m': cnt_3_6m, 'pct_3_6m': pct(cnt_3_6m),
        'cnt_6_12m': cnt_6_12m, 'pct_6_12m': pct(cnt_6_12m),
        'cnt_1_2y': cnt_1_2y, 'pct_1_2y': pct(cnt_1_2y),
        'cnt_2_5y': cnt_2_5y, 'pct_2_5y': pct(cnt_2_5y),
        'cnt_5y_plus': cnt_5y_plus, 'pct_5y_plus': pct(cnt_5y_plus),
    })

# -------------------------------
# RUN – zonder argparse, met Excel
# -------------------------------
def run_to_excel(input_csv: Path,
                 output_xlsx: Path = None,
                 suffix: str = "_last_changed",
                 auto_detect: bool = False,
                 date_cols: list = None,
                 sep: str = ",",
                 encoding: str = None,
                 reference_now: str = None):
    # Lees CSV met fallback voor encoding
    if encoding is None:
        try:
            df = pd.read_csv(input_csv, sep=sep)
        except Exception:
            df = pd.read_csv(input_csv, sep=sep, encoding="latin1")
    else:
        df = pd.read_csv(input_csv, sep=sep, encoding=encoding)

    # Datumkolommen bepalen
    cols = detect_date_columns(df, suffix=suffix, auto_detect=auto_detect, explicit=date_cols)
    if len(cols) == 0:
        raise ValueError("Geen datumkolommen gedetecteerd. Pas `SUFFIX`, `DATE_COLS` of `AUTO_DETECT` aan.")

    # Referentie 'nu'
    now = pd.Timestamp(reference_now) if reference_now else pd.Timestamp.now()

    # Classificatie per rij
    df_dates   = df[cols].copy()
    classified = classify_age_buckets(df_dates, now)

    # OBJECTID of index toevoegen
    if "OBJECTID" in df.columns:
        classified.insert(0, "OBJECTID", df["OBJECTID"])
    else:
        classified.insert(0, "row_index", df.index)

    # Samenvatting per kolom
    per_col = []
    for c in cols:
        d = summarize_column(df[c], now)
        d.update({'kolom': c})
        per_col.append(d)
    df_per_col = pd.DataFrame(per_col)[[
        'kolom',
        'aantal_met_datum','aantal_geldig_verleden','aantal_toekomst','aantal_missend',
        'cnt_0_3m','pct_0_3m','cnt_3_6m','pct_3_6m','cnt_6_12m','pct_6_12m',
        'cnt_1_2y','pct_1_2y','cnt_2_5y','pct_2_5y','cnt_5y_plus','pct_5y_plus'
    ]]

    # Totaalsamenvatting over het hele bestand
    total_series = summarize_overall(df[cols], now)
    df_total = total_series.to_frame().T

    # Outputpad
    if output_xlsx is None:
        output_xlsx = input_csv.with_name(f"classified_{input_csv.stem}.xlsx")

    # Schrijf Excel met 3 tabs: Per_rij, Per_kolom, Totaal
    from pandas import ExcelWriter
    with ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        classified.to_excel(writer, index=False, sheet_name='Per_rij')
        df_per_col.to_excel(writer, index=False, sheet_name='Per_kolom')
        df_total.to_excel(writer, index=False, sheet_name='Totaal')

    return output_xlsx, cols

# -------------------------------
# MAIN – pas CONFIG bovenaan aan
# -------------------------------
if __name__ == "__main__":
    out, used_cols = run_to_excel(
        input_csv     = INPUT_CSV,
        output_xlsx   = OUTPUT_XLSX,
        suffix        = SUFFIX,
        auto_detect   = AUTO_DETECT,
        date_cols     = DATE_COLS,
        sep           = SEP,
        encoding      = ENCODING,
        reference_now = REFERENCE_NOW,
    )
    print(f"Excel geschreven naar: {out}")
    print(f"Datumkolommen ({len(used_cols)}): {used_cols}")
