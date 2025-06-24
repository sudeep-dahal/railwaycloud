import asyncio
import aiohttp
import csv
import os

url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetPrePermissionByLotNo'

start = 100000
end = 325828
csv_file = 'pre_permissions.csv'

async def fetch(session, lot_no):
    try:
        async with session.post(url, json={"LotNo": lot_no}) as response:
            data = await response.json()
            if 'd' in data and data['d']:
                res = data['d'][0]
                res.pop('__type', None)
                return res
    except Exception as e:
        print(f"Error on LotNo {lot_no}: {e}")
    return None

def load_existing_data():
    existing = {}
    if os.path.exists(csv_file):
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                lot_no = row.get('LotNo')
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
    async with aiohttp.ClientSession() as session:
        for lot_no in range(start, end):
            tasks.append(fetch(session, lot_no))
            await asyncio.sleep(1)

        results = await asyncio.gather(*tasks)

    new_data = {}
    for res in results:
        if res and 'LotNo' in res:
            new_data[res['LotNo']] = res
            headers.update(res.keys())

    # Merge existing and new data (new data overrides old ones with same LotNo)
    all_data = {**existing_data, **new_data}

    if all_data:
        save_to_csv(all_data, list(headers))
        print(f"Saved {len(all_data)} records to {csv_file}")
    else:
        print("No data to save.")

# Run the async program
asyncio.run(main())
