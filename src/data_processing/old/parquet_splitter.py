
import os
import pandas as pd

# ============= Instellingen =============
INPUT_FOLDER = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\DAMO_H_parquet"     # <-- aanpassen
OUTPUT_FOLDER = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\DAMO_H_parquet_opgesplitst"        # <-- aanpassen
OBJECTID_FIELD = "OBJECTID"                # <-- aanpassen indien nodig
N_PER_PART = 10_000                        # chunk grootte
RECURSIVE = False                           # True = submappen meenemen
# ========================================

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def walk_parquet_files(folder, recursive=False):
    for root, dirs, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".parquet"):
                yield os.path.join(root, f)
        if not recursive:
            break

def split_parquet(file_path: str):
    base_name = os.path.basename(file_path)
    name, _ = os.path.splitext(base_name)

    print(f"\nVerwerken: {base_name}")

    # Lees parquet in 1x (kan omdat Parquet gecomprimeerd en kolom-gebaseerd is)
    df = pd.read_parquet(file_path)

    if OBJECTID_FIELD not in df.columns:
        print(f" - OBJECTID kolom '{OBJECTID_FIELD}' niet gevonden. Sla over.")
        return

    # Drop rijen zonder geldige OBJECTID
    df = df.copy()
    df[OBJECTID_FIELD] = pd.to_numeric(df[OBJECTID_FIELD], errors="coerce")
    df = df.dropna(subset=[OBJECTID_FIELD])
    df[OBJECTID_FIELD] = df[OBJECTID_FIELD].astype(int)

    if df.empty:
        print(" - Geen rijen met geldige OBJECTID. Sla over.")
        return

    min_oid = df[OBJECTID_FIELD].min()

    # Bepaal chunk index per rij
    df["_part"] = ((df[OBJECTID_FIELD] - min_oid) // N_PER_PART) + 1

    # Groepeer per part en schrijf elk groepje uit
    for part_idx, subset in df.groupby("_part"):
        out_path = os.path.join(OUTPUT_FOLDER, f"{name}_part{part_idx}.parquet")
        ensure_dir(os.path.dirname(out_path))

        subset.drop(columns=["_part"]).to_parquet(out_path, index=False)
        print(f" - Geschreven: {out_path} ({len(subset)} rijen)")

def main():
    ensure_dir(OUTPUT_FOLDER)
    files = list(walk_parquet_files(INPUT_FOLDER, RECURSIVE))

    if not files:
        print("Geen parquet-bestanden gevonden.")
        return

    print(f"Gevonden parquet-bestanden: {len(files)}")

    for f in files:
        split_parquet(f)

if __name__ == "__main__":
    main()