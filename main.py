import asyncio
import aiohttp
import csv
import os
import dropbox
from aiohttp import ClientTimeout
from dotenv import load_dotenv

load_dotenv()

url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'

start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))
chunk_size = 5
error_stickers = []
data_chunk = {}
headers = set()

DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")

if not DROPBOX_ACCESS_TOKEN:
    raise Exception("Missing DROPBOX_ACCESS_TOKEN in environment variables")

dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

def write_csv(filename, data_chunk, headers):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data_chunk.values():
            writer.writerow(row)

def upload_to_dropbox(local_path, dropbox_path):
    with open(local_path, "rb") as f:
        print(f"[UPLOAD] Uploading {local_path} to Dropbox at {dropbox_path}...")
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    print(f"[DONE] Uploaded to Dropbox: {dropbox_path}")

async def fetch(session, sticker_no, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with session.post(
                url,
                json={'PassportNo': '', 'StickerNo': sticker_no},
                timeout=ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                if 'd' in data and data['d']:
                    res = data['d'][0]
                    res.pop('__type', None)
                    return res
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            else:
                error_stickers.append(sticker_no)
                print(f"[ERROR] {sticker_no} failed after {max_retries} attempts: {e}")
                return None
    return None

async def save_and_upload_chunk(file_index, data_chunk, headers):
    filename = f"finalpermission{file_index if file_index > 0 else ''}.csv"
    print(f"[WRITE] Saving {len(data_chunk)} records to {filename}")
    await asyncio.to_thread(write_csv, filename, data_chunk, headers)
    print(f"[DONE] Saved: {filename}")

    dropbox_path = f"/{filename}"
    await asyncio.to_thread(upload_to_dropbox, filename, dropbox_path)

def merge_csv_files(pattern, output_file):
    import glob
    print(f"\n[MERGE] Merging all chunk files into {output_file}...")
    all_files = sorted(f for f in glob.glob(pattern) if not f.endswith('_merged.csv'))
    if not all_files:
        print("⚠️ No CSV files found to merge.")
        return 0

    seen = set()
    with open(output_file, 'w', newline='', encoding='utf-8') as fout:
        writer = None
        for filename in all_files:
            with open(filename, 'r', newline='', encoding='utf-8') as fin:
                reader = csv.DictReader(fin)
                if writer is None:
                    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
                    writer.writeheader()
                for row in reader:
                    key = row.get('StickerNo')
                    if key not in seen:
                        seen.add(key)
                        writer.writerow(row)
    print(f"[DONE] Merged into {output_file} ({len(seen)} records)")
    return len(seen)

async def main():
    file_index = 0
    counter = 0

    async with aiohttp.ClientSession() as session:
        for sticker_no in range(start_point, end_point):
            sticker_num = f"{sticker_no:09d}"
            print(f"[QUEUE] Processing {sticker_num}")

            res = await fetch(session, sticker_num)
            if res and 'StickerNo' in res:
                data_chunk[res['StickerNo']] = res
                headers.update(res.keys())
                counter += 1

            if counter > 0 and counter % chunk_size == 0:
                await save_and_upload_chunk(file_index, data_chunk.copy(), list(headers))
                data_chunk.clear()
                file_index += 1

            await asyncio.sleep(3)

    if data_chunk:
        await save_and_upload_chunk(file_index, data_chunk, list(headers))

    merged_filename = 'finalpermission_merged.csv'
    total_records = merge_csv_files('finalpermission*.csv', merged_filename)

    if total_records > 0:
        dropbox_path = f"/{merged_filename}"
        await asyncio.to_thread(upload_to_dropbox, merged_filename, dropbox_path)

    if error_stickers:
        print(f"\n❌ Failed stickers: {error_stickers}")
    else:
        print("\n✅ All stickers processed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
