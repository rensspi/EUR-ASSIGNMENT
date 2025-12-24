
import os
import csv
from typing import Optional

# ============= Instellingen =============
INPUT_FOLDER = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\DAMO_H_CSV"      # <-- pas aan
OUTPUT_FOLDER = r"K:\CentraalDatamanagement\PDC\01_WIP\01_Algemeen\X_000002_DatakwaliteitBaseline\DAMO_H_CSV_opgeknipt"         # <-- pas aan
N_PER_PART = 10_000                              # grootte OBJECTID-range per part
OBJECTID_FIELD = "OBJECTID"                      # pas aan als kolomnaam anders is
RECURSIVE = False                                # True als ook submappen
DEFAULT_DELIMITER = ','                          # standaard delimiter
DEFAULT_ENCODING = 'utf-8-sig'                   # standaard encoding
# ========================================

def ensure_dir(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path)

def detect_dialect(sample_text: str) -> Optional[csv.Dialect]:
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_text)
        return dialect
    except Exception:
        return None

def open_csv_reader(file_path: str):
    """
    Probeert met voorkeurs-encoding 'utf-8-sig', valt terug op 'utf-8' en 'latin-1'.
    Probeert delimiter te detecteren; valt terug op DEFAULT_DELIMITER.
    Retourneert (fp, reader, dialect, encoding).
    """
    try_encodings = [DEFAULT_ENCODING, 'utf-8', 'latin-1']
    for enc in try_encodings:
        try:
            fp = open(file_path, 'r', encoding=enc, newline='')
            sample = fp.read(8192)
            fp.seek(0)
            dialect = detect_dialect(sample)
            if dialect is None:
                # Bouw een eenvoudige dialect met default delimiter
                class FallbackDialect(csv.Dialect):
                    delimiter = DEFAULT_DELIMITER
                    doublequote = True
                    escapechar = None
                    lineterminator = '\n'
                    quotechar = '"'
                    quoting = csv.QUOTE_MINIMAL
                    skipinitialspace = False
                reader = csv.reader(fp, dialect=FallbackDialect)
                return fp, reader, FallbackDialect, enc
            else:
                # Forceer veilig quoten voor writer-compatibiliteit
                class SafeDialect(csv.Dialect):
                    delimiter = dialect.delimiter or DEFAULT_DELIMITER
                    doublequote = True if dialect.doublequote is None else dialect.doublequote
                    escapechar = None
                    lineterminator = '\n'
                    quotechar = dialect.quotechar or '"'
                    quoting = csv.QUOTE_MINIMAL  # cruciaal: voorkomt escapechar-fouten
                    skipinitialspace = bool(getattr(dialect, 'skipinitialspace', False))
                reader = csv.reader(fp, dialect=SafeDialect)
                return fp, reader, SafeDialect, enc
        except Exception:
            try:
                fp.close()
            except Exception:
                pass
            continue
    # Laatste fallback: ascii lezen met default delimiter
    fp = open(file_path, 'r', encoding='ascii', errors='ignore', newline='')
    class AsciiDialect(csv.Dialect):
        delimiter = DEFAULT_DELIMITER
        doublequote = True
        escapechar = None
        lineterminator = '\n'
        quotechar = '"'
        quoting = csv.QUOTE_MINIMAL
        skipinitialspace = False
    reader = csv.reader(fp, dialect=AsciiDialect)
    return fp, reader, AsciiDialect, 'ascii'

def csv_dict_reader(file_path: str):
    """
    Geeft (fp, reader: DictReader, fieldnames: list[str], dialect, encoding)
    """
    fp, base_reader, dialect, enc = open_csv_reader(file_path)
    try:
        header = next(base_reader)
    except StopIteration:
        fp.close()
        return None, None, None, None, None
    if not header:
        fp.close()
        return None, None, None, None, None

    reader = csv.DictReader(fp, fieldnames=header, dialect=dialect)
    return fp, reader, header, dialect, enc

def csv_dict_writer(file_path: str, fieldnames, dialect: csv.Dialect, encoding: str):
    """
    Writer met veilig quoten (QUOTE_MINIMAL) en delimiter uit dialect.
    Hiermee voorkomen we: 'need to escape, but no escapechar set'.
    """
    ensure_dir(os.path.dirname(file_path))
    fp = open(file_path, 'w', encoding=encoding or DEFAULT_ENCODING, newline='')

    class SafeDialect(csv.Dialect):
        delimiter = getattr(dialect, 'delimiter', DEFAULT_DELIMITER) or DEFAULT_DELIMITER
        doublequote = True
        escapechar = None
        lineterminator = '\n'
        quotechar = getattr(dialect, 'quotechar', '"') or '"'
        quoting = csv.QUOTE_MINIMAL
        skipinitialspace = bool(getattr(dialect, 'skipinitialspace', False))

    writer = csv.DictWriter(fp, fieldnames=fieldnames, dialect=SafeDialect)
    writer.writeheader()
    return fp, writer

def part_index_for_objectid(oid: int, start_oid: int, n_per_part: int) -> int:
    if n_per_part <= 0:
        raise ValueError("N_PER_PART moet > 0 zijn.")
    return ((oid - start_oid) // n_per_part) + 1

def process_csv_file(file_path: str):
    base_name = os.path.basename(file_path)
    name, _ = os.path.splitext(base_name)
    print(f"Verwerken: {base_name}")

    # Pass 1: bepaal minimale OBJECTID voor range-basis
    fp, reader, header, dialect, enc = csv_dict_reader(file_path)
    if reader is None:
        print(f" - Lege of onleesbare file: {base_name}")
        return

    if OBJECTID_FIELD not in header:
        print(f" - Kolom '{OBJECTID_FIELD}' niet gevonden in {base_name}. Sla over.")
        fp.close()
        return

    min_oid = None
    row_count = 0
    for row in reader:
        row_count += 1
        val = row.get(OBJECTID_FIELD)
        try:
            oid = int(val) if val not in (None, "") else None
        except ValueError:
            oid = None
        if oid is not None and (min_oid is None or oid < min_oid):
            min_oid = oid
    fp.close()

    if row_count == 0:
        print(f" - Geen rijen in {base_name}.")
        return
    if min_oid is None:
        print(f" - Geen geldige {OBJECTID_FIELD}-waarden in {base_name}. Sla over.")
        return

    # Pass 2: routeer rijen naar parts
    fp, reader, header, dialect, enc = csv_dict_reader(file_path)
    open_parts = {}   # part_index -> (fp_out, writer)
    counts_per_part = {}

    def get_writer_for_part(idx: int):
        if idx not in open_parts:
            out_path = os.path.join(OUTPUT_FOLDER, f"{name}_part{idx}.csv")
            fp_out, writer = csv_dict_writer(out_path, header, dialect, enc)
            open_parts[idx] = (fp_out, writer)
            counts_per_part[idx] = 0
        return open_parts[idx]

    routed = 0
    skipped = 0

    for row in reader:
        val = row.get(OBJECTID_FIELD)
        try:
            oid = int(val) if val not in (None, "") else None
        except ValueError:
            oid = None

        if oid is None:
            skipped += 1
            continue

        idx = part_index_for_objectid(oid, min_oid, N_PER_PART)
        fp_out, writer = get_writer_for_part(idx)
        writer.writerow(row)
        counts_per_part[idx] += 1
        routed += 1

    # Sluit outputs
    for idx, (fp_out, _) in open_parts.items():
        fp_out.close()
    fp.close()

    parts_info = ", ".join([f"part{idx}={cnt} rijen" for idx, cnt in sorted(counts_per_part.items())])
    print(f" - Gereed: {base_name} -> {parts_info}. Overgeslagen (zonder geldige {OBJECTID_FIELD}): {skipped}")

def walk_csv_files(input_folder: str, recursive: bool):
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith(".csv"):
                yield os.path.join(root, file)
        if not recursive:
            break

def main():
    ensure_dir(OUTPUT_FOLDER)
    files = list(walk_csv_files(INPUT_FOLDER, RECURSIVE))
    if not files:
        print("Geen CSV-bestanden gevonden.")
        return

    print(f"Gevonden CSV-bestanden: {len(files)}")
    for path in files:
        process_csv_file(path)

if __name__ == "__main__":
    main()
