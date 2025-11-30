#!/usr/bin/env python3
"""
Einfache S3-Auswertung für den Bucket crm-berglicht-e-formulare-mit-fotos.

Zählt:
- Gesamtanzahl Objekte
- Anzahl Formular-CSV-Dateien
- Anzahl Anhänge (alles außer CSV und "Ordner"-Platzhalter)
- Verteilung der Dateiendungen bei Anhängen

Nutzung:
    pip install boto3 botocore
    # entweder .env befüllen oder die Variablen weiter unten direkt setzen
    python scripts/e_formulare_stats.py
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def load_env_from_file() -> None:
    """
    Lädt optional Variablen aus ../.env, falls sie nicht bereits im Prozess existieren.
    """
    env_path = Path(__file__).resolve().parents[1] / '.env'
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue

        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_from_file()

#
# ==== AWS-Zugangsdaten (hier eintragen, falls nicht über .env / Environment) ====
#
MANUAL_AWS_ACCESS_KEY_ID = ''      # z.B. 'AKIAXXX...'
MANUAL_AWS_SECRET_ACCESS_KEY = ''  # z.B. 'abcd1234...'
MANUAL_AWS_SESSION_TOKEN = ''


def resolve_credential(env_key: str, manual_value: str) -> str:
    env_value = os.environ.get(env_key, '').strip()
    if env_value:
        return env_value
    return manual_value.strip()


BUCKET_NAME = os.environ.get('E_FORM_BUCKET', 'crm-berglicht-e-formulare-mit-fotos')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')
AWS_ACCESS_KEY_ID = resolve_credential('AWS_ACCESS_KEY_ID', MANUAL_AWS_ACCESS_KEY_ID)
AWS_SECRET_ACCESS_KEY = resolve_credential('AWS_SECRET_ACCESS_KEY', MANUAL_AWS_SECRET_ACCESS_KEY)
AWS_SESSION_TOKEN = resolve_credential('AWS_SESSION_TOKEN', MANUAL_AWS_SESSION_TOKEN)


def iter_bucket_objects(bucket: str) -> Iterable[Dict]:
    """Liest alle Objekte aus dem Bucket via Pagination."""
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print('Fehlende AWS-Credentials: bitte oben im Skript oder in der .env setzen.', file=sys.stderr)
        sys.exit(1)

    client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN or None,
        config=Config(max_pool_connections=20, retries={'max_attempts': 5}),
    )

    continuation_token = None
    while True:
        params: Dict[str, str] = {'Bucket': bucket, 'MaxKeys': 1000}
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response = client.list_objects_v2(**params)
        for obj in response.get('Contents', []):
            yield obj

        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')


def classify_objects(objects: Iterable[Dict]) -> Tuple[int, int, int, Counter]:
    """
    Zählt CSV-Formulare vs. sonstige Anhänge.
    Ordner-Platzhalter (Key endet mit '/' und Size 0) werden ignoriert.
    """
    total = 0
    csv_count = 0
    attachment_count = 0
    attachment_extensions: Counter = Counter()

    for obj in objects:
        key = obj.get('Key', '')
        size = obj.get('Size', 0) or 0

        # Skip "Ordner"
        if key.endswith('/') and size == 0:
            continue

        total += 1
        key_lower = key.lower()

        if key_lower.endswith('.csv'):
            csv_count += 1
            continue

        attachment_count += 1
        ext = key_lower.rsplit('.', 1)[-1] if '.' in key_lower else '<ohne_endung>'
        attachment_extensions[ext] += 1

    return total, csv_count, attachment_count, attachment_extensions


def human_size(num_bytes: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f'{value:.2f} {unit}'
        value /= 1024
    return f'{value:.2f} TB'


def main() -> None:
    print(f'Auswertung für Bucket: {BUCKET_NAME}')

    try:
        objects = list(iter_bucket_objects(BUCKET_NAME))
    except (ClientError, BotoCoreError) as exc:
        print(f'Fehler beim Lesen des Buckets: {exc}', file=sys.stderr)
        sys.exit(1)

    total_size = sum(obj.get('Size', 0) or 0 for obj in objects)
    total, csv_count, attachment_count, ext_counter = classify_objects(objects)

    print('\nErgebnis:')
    print(f'- Gesamtobjekte: {total:,}')
    print(f'- Gesamtdatenmenge: {human_size(total_size)}')
    print(f'- Formulare (CSV): {csv_count:,}')
    print(f'- Anhänge (nicht CSV): {attachment_count:,}')

    if ext_counter:
        print('\nAnhänge nach Dateiendung:')
        for ext, count in ext_counter.most_common():
            print(f'  • {ext}: {count:,}')


if __name__ == '__main__':
    main()

