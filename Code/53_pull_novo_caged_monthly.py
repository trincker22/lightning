from pathlib import Path
import csv
import ftplib
from collections import defaultdict
import py7zr

ROOT = Path('/Users/theresarincker/Documents/GitHub/lightning')
OUT_DIR = ROOT / 'data' / 'powerIV' / 'novo_caged'
TMP_DIR = OUT_DIR / 'tmp'
MONTH_DIR = OUT_DIR / 'municipality_month_parts'
OUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)
MONTH_DIR.mkdir(parents=True, exist_ok=True)

HOST = 'ftp.mtps.gov.br'
BASE_DIR = '/pdet/microdados/NOVO CAGED'
START_YYYYMM = 202001
END_YYYYMM = 202412
FILE_TYPES = ['MOV', 'FOR', 'EXC']
MONTH_FIELDS = [
    'competencia', 'year', 'month', 'codigo_ibge',
    'total_net', 'female_net', 'total_admissions', 'female_admissions',
    'total_separations', 'female_separations', 'total_records', 'female_records',
    'mov_records', 'for_records', 'exc_records'
]
MANIFEST_FIELDS = [
    'competencia', 'file_type', 'archive_name', 'archive_bytes',
    'txt_name', 'txt_bytes', 'n_municipios_in_file'
]


def month_sequence(start_yyyymm: int, end_yyyymm: int):
    sy, sm = divmod(start_yyyymm, 100)
    ey, em = divmod(end_yyyymm, 100)
    y, m = sy, sm
    out = []
    while (y < ey) or (y == ey and m <= em):
        out.append(f'{y:04d}{m:02d}')
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def ftp_download(remote_path: str, local_path: Path):
    ftp = ftplib.FTP(HOST, timeout=120, encoding='latin-1')
    ftp.login()
    with open(local_path, 'wb') as f:
        ftp.retrbinary('RETR ' + remote_path, f.write, blocksize=1024 * 1024)
    ftp.quit()


def ftp_list_names(remote_dir: str):
    ftp = ftplib.FTP(HOST, timeout=120, encoding='latin-1')
    ftp.login()
    lines = []
    ftp.retrlines('LIST ' + remote_dir, lines.append)
    ftp.quit()
    names = []
    for line in lines:
        parts = line.split()
        if parts:
            names.append(parts[-1])
    return names


def extract_7z(archive_path: Path, extract_dir: Path):
    extract_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode='r') as zf:
        zf.extractall(path=extract_dir)
    names = [p for p in extract_dir.iterdir() if p.is_file()]
    if len(names) != 1:
        raise RuntimeError(f'Expected exactly one extracted file from {archive_path}, got {names}')
    return names[0]


def aggregate_txt(txt_path: Path, source_type: str):
    agg = defaultdict(lambda: {
        'total_net': 0,
        'female_net': 0,
        'total_admissions': 0,
        'female_admissions': 0,
        'total_separations': 0,
        'female_separations': 0,
        'total_records': 0,
        'female_records': 0,
        'mov_records': 0,
        'for_records': 0,
        'exc_records': 0,
    })
    with open(txt_path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            municipio = row['município']
            saldo_str = row['saldomovimentação']
            saldo = int(saldo_str) if saldo_str else 0
            female = row['sexo'] == '3'
            bucket = agg[municipio]
            bucket['total_net'] += saldo
            bucket['total_records'] += 1
            if saldo > 0:
                bucket['total_admissions'] += 1
            elif saldo < 0:
                bucket['total_separations'] += 1
            if female:
                bucket['female_net'] += saldo
                bucket['female_records'] += 1
                if saldo > 0:
                    bucket['female_admissions'] += 1
                elif saldo < 0:
                    bucket['female_separations'] += 1
            bucket[f'{source_type.lower()}_records'] += 1
    return agg


def merge_aggs(target, addition):
    for municipio, vals in addition.items():
        bucket = target[municipio]
        for key, value in vals.items():
            bucket[key] += value


def read_manifest(path: Path):
    if not path.exists():
        return []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f, delimiter='\t'))


def write_manifest(path: Path, rows):
    rows = sorted(rows, key=lambda x: (x['competencia'], x['file_type']))
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)


def build_month(yyyymm: str, manifest_path: Path):
    out_file = MONTH_DIR / f'novo_caged_municipality_month_{yyyymm}.tsv'
    if out_file.exists():
        print(f'skip existing {yyyymm}')
        return

    year = yyyymm[:4]
    month = yyyymm[4:6]
    combined = defaultdict(lambda: {
        'total_net': 0,
        'female_net': 0,
        'total_admissions': 0,
        'female_admissions': 0,
        'total_separations': 0,
        'female_separations': 0,
        'total_records': 0,
        'female_records': 0,
        'mov_records': 0,
        'for_records': 0,
        'exc_records': 0,
    })

    manifest_rows = read_manifest(manifest_path)
    manifest_lookup = {(row['competencia'], row['file_type']): row for row in manifest_rows}

    available_names = set(ftp_list_names(f'{BASE_DIR}/{year}/{yyyymm}'))

    for file_type in FILE_TYPES:
        archive_name = f'CAGED{file_type}{yyyymm}.7z'
        if archive_name not in available_names:
            print(f'missing {archive_name}: not listed on ftp', flush=True)
            continue
        remote_path = f'{BASE_DIR}/{year}/{yyyymm}/{archive_name}'
        archive_path = TMP_DIR / archive_name
        extract_dir = TMP_DIR / f'{archive_name}_extract'

        if extract_dir.exists():
            for p in extract_dir.iterdir():
                if p.is_file():
                    p.unlink()
            extract_dir.rmdir()

        print(f'downloading {archive_name}', flush=True)
        ftp_download(remote_path, archive_path)
        txt_path = extract_7z(archive_path, extract_dir)
        print(f'aggregating {txt_path.name}', flush=True)
        agg = aggregate_txt(txt_path, file_type)
        merge_aggs(combined, agg)

        manifest_lookup[(yyyymm, file_type)] = {
            'competencia': yyyymm,
            'file_type': file_type,
            'archive_name': archive_name,
            'archive_bytes': str(archive_path.stat().st_size),
            'txt_name': txt_path.name,
            'txt_bytes': str(txt_path.stat().st_size),
            'n_municipios_in_file': str(len(agg)),
        }
        write_manifest(manifest_path, list(manifest_lookup.values()))

        txt_path.unlink()
        extract_dir.rmdir()
        archive_path.unlink()

    rows = []
    for municipio, vals in sorted(combined.items()):
        row = {
            'competencia': yyyymm,
            'year': year,
            'month': month,
            'codigo_ibge': municipio,
        }
        row.update({k: str(v) for k, v in vals.items()})
        rows.append(row)

    with open(out_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=MONTH_FIELDS, delimiter='\t')
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    manifest_path = OUT_DIR / 'novo_caged_file_manifest.tsv'
    for yyyymm in month_sequence(START_YYYYMM, END_YYYYMM):
        build_month(yyyymm, manifest_path)
    print('done')
