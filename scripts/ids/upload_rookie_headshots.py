"""Upload rookie headshot PNGs to Supabase Storage and set headshot_url.

Reads PNG files from a local folder, matches by filename to rookie players
in the dynasty_values table, uploads to Supabase Storage, and PATCHes
headshot_url on the players table.

Usage:
    python3 scripts/ids/upload_rookie_headshots.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    supabase_rest_patch,
)

HEADSHOTS_DIR = "/Users/dan/Downloads/Rookie Headshots"
BUCKET = "headshots"
key = SUPABASE_SERVICE_KEY or SUPABASE_KEY


def create_bucket():
    """Create a public storage bucket (idempotent)."""
    url = f"{SUPABASE_URL}/storage/v1/bucket"
    data = json.dumps({
        "id": BUCKET,
        "name": BUCKET,
        "public": True,
        "file_size_limit": 5242880,  # 5MB
        "allowed_mime_types": ["image/png", "image/jpeg"],
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }, method="POST")
    try:
        urllib.request.urlopen(req)
        print(f"Created bucket '{BUCKET}'")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        if "already exists" in body.lower() or e.code == 409:
            print(f"Bucket '{BUCKET}' already exists")
        else:
            print(f"ERROR creating bucket: {e.code} {body}")
            sys.exit(1)


def upload_file(file_path, storage_path):
    """Upload a file to Supabase Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{storage_path}"
    with open(file_path, "rb") as f:
        file_data = f.read()

    req = urllib.request.Request(url, data=file_data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "image/png",
        "x-upsert": "true",
    }, method="POST")
    urllib.request.urlopen(req)


def get_public_url(storage_path):
    """Get the public URL for a file in storage."""
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{storage_path}"


def fetch_rookie_players():
    """Fetch rookie players from dynasty_values who need headshots."""
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/dynasty_values"
            f"?select=player_id,players(first_name,last_name,position,headshot_url,dan_id)"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        batch = json.loads(urllib.request.urlopen(req).read().decode())
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def main():
    # ── 1. List available headshot files ────────────────────────────────────
    if not os.path.isdir(HEADSHOTS_DIR):
        print(f"ERROR: Headshots directory not found: {HEADSHOTS_DIR}")
        sys.exit(1)

    png_files = [f for f in os.listdir(HEADSHOTS_DIR) if f.lower().endswith(".png")]
    print(f"Found {len(png_files)} PNG files in {HEADSHOTS_DIR}")

    # Build lookup: normalized name -> filename
    file_by_name = {}
    for f in png_files:
        name = os.path.splitext(f)[0]
        file_by_name[normalize_name(name)] = f

    # ── 2. Fetch players who need headshots ─────────────────────────────────
    print("Fetching dynasty value players from Supabase...")
    dv_players = fetch_rookie_players()
    print(f"  {len(dv_players)} dynasty value players")

    # Find those missing headshots
    need_headshot = []
    for dv in dv_players:
        p = dv["players"]
        if p.get("headshot_url"):
            continue  # already has one
        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        if norm in file_by_name:
            need_headshot.append({
                "player_id": dv["player_id"],
                "name": full_name,
                "file": file_by_name[norm],
            })

    print(f"  {len(need_headshot)} players need headshots and have matching files")

    if not need_headshot:
        print("Nothing to upload!")
        return

    # ── 3. Create bucket ────────────────────────────────────────────────────
    create_bucket()

    # ── 4. Upload and update ────────────────────────────────────────────────
    print(f"\nUploading {len(need_headshot)} headshots...")
    uploaded = 0
    errors = 0

    for item in need_headshot:
        file_path = os.path.join(HEADSHOTS_DIR, item["file"])
        # Use player_id as storage filename for uniqueness
        storage_path = f"rookies/{item['player_id']}.png"

        try:
            upload_file(file_path, storage_path)
            public_url = get_public_url(storage_path)
            supabase_rest_patch("players", "player_id", item["player_id"], {
                "headshot_url": public_url,
            })
            uploaded += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR {item['name']}: {e.code} {body}")
            errors += 1

        if (uploaded + errors) % 20 == 0:
            print(f"  Processed {uploaded + errors}/{len(need_headshot)}...")

    # ── 5. Summary ──────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"PNG files available:    {len(png_files)}")
    print(f"Players needing shots:  {len(need_headshot)}")
    print(f"Uploaded & updated:     {uploaded}")
    print(f"Errors:                 {errors}")


if __name__ == "__main__":
    main()
