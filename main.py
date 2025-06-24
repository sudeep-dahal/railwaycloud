import asyncio
import aiohttp
import csv
import os
import glob
import smtplib
from email.message import EmailMessage
from aiohttp import ClientTimeout
from dotenv import load_dotenv

load_dotenv()

url = 'http://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'

start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))

chunk_size = 10000
error_stickers = []
data_chunk = {}
headers = set()

# Email credentials
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

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

def send_email_with_attachment(subject, body, to_email, attachment_path, smtp_user, smtp_pass):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        file_data = f.read()
        file_name = os.path.basename(attachment_path)
        msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(smtp_user, smtp_pass)
        smtp.send_message(msg)

    print(f"📧 Email sent with {file_name} to {to_email}")

async def save_to_csv_async(file_index, data_chunk, headers):
    filename = f"final_permission{file_index if file_index > 0 else ''}.csv"
    print(f"[WRITE] Saving {len(data_chunk)} records to {filename}")
    await asyncio.to_thread(write_csv, filename, data_chunk, headers)
    print(f"[DONE] Saved: {filename}")

    await asyncio.to_thread(
        send_email_with_attachment,
        subject=f"CSV Chunk {file_index} Scraped",
        body=f"Attached is the CSV file with {len(data_chunk)} records.",
        to_email=EMAIL_TO,
        attachment_path=filename,
        smtp_user=EMAIL_USER,
        smtp_pass=EMAIL_PASS
    )

def merge_csv_files(pattern, output_file):
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
            print(f"Queueing {sticker_num}")
            
            res = await fetch(session, sticker_num)
            if res and 'StickerNo' in res:
                data_chunk[res['StickerNo']] = res
                headers.update(res.keys())
                counter += 1

            if counter > 0 and counter % chunk_size == 0:
                await save_to_csv_async(file_index, data_chunk.copy(), list(headers))
                data_chunk.clear()
                file_index += 1

            await asyncio.sleep(4)  # 1 request + 3 sec sleep

    # Save any remaining data
    if data_chunk:
        await save_to_csv_async(file_index, data_chunk, list(headers))

    # Merge CSV files
    merged_filename = 'final_permission_merged.csv'
    total_records = merge_csv_files('final_permission*.csv', merged_filename)

    # Email final merged file
    if total_records > 0:
        await asyncio.to_thread(
            send_email_with_attachment,
            subject="✅ All Stickers Processed - Final Merged CSV",
            body=f"Merged CSV file with {total_records} total records is attached.",
            to_email=EMAIL_TO,
            attachment_path=merged_filename,
            smtp_user=EMAIL_USER,
            smtp_pass=EMAIL_PASS
        )

    if error_stickers:
        print(f"\n❌ Failed stickers: {error_stickers}")
    else:
        print("\n✅ All stickers processed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
