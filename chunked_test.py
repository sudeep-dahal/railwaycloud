import asyncio
import aiohttp
import csv
import os
import glob
from aiohttp import ClientTimeout
from dotenv import load_dotenv

load_dotenv()

url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'

start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))

chunk_size = 10  # Save after every 10,000 records
error_stickers = []
data_chunk = {}
csv_save_tasks = []
headers = set()

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
                print(f"[ERROR] {sticker_no} failed after {max_retries} attempts")
                return None
    return None

def write_csv(filename, data_chunk, headers):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data_chunk.values():
            writer.writerow(row)

async def save_to_csv_async(file_index, data_chunk, headers):
    filename = f"final_permission{file_index if file_index > 0 else ''}.csv"
    print(f"[WRITE] Saving {len(data_chunk)} records to {filename}")
    await asyncio.to_thread(write_csv, filename, data_chunk, headers)
    print(f"[DONE] Saved: {filename}")

async def main():
    file_index = 0
    counter = 0

    async with aiohttp.ClientSession() as session:
        for sticker_no in range(start_point, end_point):
            sticker_num = f"{sticker_no:09d}"
            print(f"Queueing {sticker_num}")
            
            res = await fetch(session, sticker_num)
            if res and 'StickerNo' in res:
                data_chunk[res['StickerNo']] = res
                headers.update(res.keys())
                counter += 1

            # Save after every 10,000
            if counter > 0 and counter % chunk_size == 0:
                await save_to_csv_async(file_index, data_chunk.copy(), list(headers))
                data_chunk.clear()
                file_index += 1

            await asyncio.sleep(1)  # Sleep 3 seconds after every request

    # Save remaining data
    if data_chunk:
        await save_to_csv_async(file_index, data_chunk, list(headers))

    # Merge all chunk files
    merge_csv_files('final_permission*.csv', 'final_permission_merged.csv')

    if error_stickers:
        print(f"\n❌ Failed stickers: {error_stickers}")
    else:
        print("\n✅ All stickers processed successfully.")

def merge_csv_files(pattern, output_file):
    print(f"\n[MERGE] Merging all chunk files into {output_file}...")
    all_files = sorted(f for f in glob.glob(pattern) if not f.endswith('_merged.csv'))
    if not all_files:
        print("⚠️ No CSV files found to merge.")
        return

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

if __name__ == "__main__":
    asyncio.run(main())
