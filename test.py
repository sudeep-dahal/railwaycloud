import asyncio
import aiohttp
import csv
import os
from dotenv import load_dotenv
from aiohttp import ClientTimeout

load_dotenv()

url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'

start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))

csv_file = 'approval_info.csv'

error_stickers = []

async def fetch(session, sticker_no, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Set a 30-second timeout for the request
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
                print(f"Retry {attempt + 1}/{max_retries} for StickerNo {sticker_no}: {type(e).__name__} - {e}")
                await asyncio.sleep(3.5)  # Increased delay to 2 seconds
                continue
            else:
                error_stickers.append(sticker_no)
                print(f"Failed after {max_retries} attempts for StickerNo {sticker_no}: {type(e).__name__} - {e}")
                return None
    return None

def load_existing_data():
    existing = {}
    if os.path.exists(csv_file):
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                lot_no = row.get('StickerNo')
                if lot_no:
                    existing[lot_no] = row
    return existing

def save_to_csv(all_data, headers):
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in all_data.values():
            writer.writerow(row)

async def main():
    existing_data = load_existing_data()
    headers = set(existing_data[next(iter(existing_data))].keys()) if existing_data else set()
    tasks = []
    # Limit to 10 concurrent requests
    semaphore = asyncio.Semaphore(5)
    
    async def bounded_fetch(sticker_no):
        async with semaphore:
            return await fetch(session, sticker_no)

    async with aiohttp.ClientSession() as session:
        for sticker_no in range(start_point, end_point):
            sticker_num = f"{sticker_no:09d}"
            print(f"Queueing {sticker_num}")
            tasks.append(bounded_fetch(sticker_num))
            await asyncio.sleep(1)
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    new_data = {}
    for res in results:
        if isinstance(res, Exception):
            print(f"Unexpected error in results: {res}")
            continue
        if res and 'StickerNo' in res:
            new_data[res['StickerNo']] = res
            headers.update(res.keys())

    # Merge existing and new data (new data overrides old ones with same LotNo)
    all_data = {**existing_data, **new_data}

    if all_data:
        save_to_csv(all_data, list(headers))
        print(f"Saved {len(all_data)} records to {csv_file}")
    else:
        print("No data to save.")

    if error_stickers:
        print(f"Failed stickers after retries: {error_stickers}")
    else:
        print("No failed stickers.")

# Run the async program
if __name__ == "__main__":
    asyncio.run(main())