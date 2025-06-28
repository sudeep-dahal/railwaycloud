import asyncio
import aiohttp
import csv
import os
from aiohttp import ClientTimeout
from dotenv import load_dotenv
import boto3

# Load .env variables
load_dotenv()

# DOFE API
url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'
start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))
chunk_size = 5
error_stickers = []
data_chunk = {}
headers = set()

# AWS S3 setup
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')

s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def upload_to_s3(file_path):
    key = os.path.basename(file_path)
    print(f"[UPLOAD] Uploading {file_path} to S3 bucket {S3_BUCKET} as {key} ...")
    s3_client.upload_file(file_path, S3_BUCKET, key)
    print("[DONE] Upload complete.")

def write_csv(filename, data_chunk, headers):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data_chunk.values():
            writer.writerow(row)

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
    await asyncio.to_thread(upload_to_s3, filename)

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

            await asyncio.sleep(1)

    if data_chunk:
        await save_and_upload_chunk(file_index, data_chunk, list(headers))

    merged_filename = 'finalpermission_merged.csv'
    total_records = merge_csv_files('finalpermission*.csv', merged_filename)

    if total_records > 0:
        await asyncio.to_thread(upload_to_s3, merged_filename)

    if error_stickers:
        print(f"\n❌ Failed stickers: {error_stickers}")
    else:
        print("\n✅ All stickers processed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
