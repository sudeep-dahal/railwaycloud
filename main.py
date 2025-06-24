import os
import csv
import time
import asyncio
import aiohttp
from aiohttp import ClientSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging
from tqdm import tqdm
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from dotenv import load_dotenv
import io

load_dotenv()

# Existing configuration
url = 'https://srv.dofe.gov.np/Services/DofeWebService.svc/GetFinalApprovalInfo'

# https://srv.dofe.gov.np/Services/DofeWebService.svc/GetPrePermissionByLotNo

keys = ['AgencyName', 'ApprovedDate', 'CompanyName', 'ContractPeriod', 'Country', 'Currency', 
        'FirstName', 'FullName', 'Gender', 'InsuranceName', 'LastName', 'Medical', 'MiddleName', 
        'Name', 'PassportNo', 'PolicyExpiryDate', 'PolicyNo', 'Pssid', 'Salary', 'SkillName', 
        'StickerNo', 'SubmissionNo']
error_stickers = {}
start_point = int(os.getenv('START_POINT'))
end_point = int(os.getenv('END_POINT'))
batch_size = 100
concurrent_limit = 10
timeout_seconds = 30
max_retries = 3
email_batch_size = 100
semaphore = asyncio.Semaphore(concurrent_limit)

# Email configuration
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SENDER_EMAIL = os.getenv('EMAIL_ADDRESS')
SENDER_PASSWORD = os.getenv('EMAIL_PASSWORD')
RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')
SUBJECT = 'API Data CSV'
CSV_FILENAME = 'output.csv'  # Name for the attachment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Retry configuration
@retry(
    stop=stop_after_attempt(max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((aiohttp.ClientResponseError, aiohttp.ClientConnectionError, asyncio.TimeoutError))
)
async def fetch_single_request(session: ClientSession, sticker_no: str) -> dict:
    data = {'PassportNo': '', 'StickerNo': sticker_no}
    data= {'LotNo':''}
    async with session.post(url=url, json=data, timeout=timeout_seconds) as response:
        response.raise_for_status()
        res = await response.json()
        res = res.get('d')[0]
        del res[next(iter(res))]
        return res

async def fetch_batch(session: ClientSession, sticker_numbers: list) -> list:
    tasks = []
    for sticker_num in sticker_numbers:
        sticker_no = f"{sticker_num:09d}"
        async with semaphore:
            tasks.append(fetch_single_request(session, sticker_no))
    return await asyncio.gather(*tasks, return_exceptions=True)

def create_csv_in_memory(data):
    """Generate CSV data in memory."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    for row in data:
        if isinstance(row, dict):
            writer.writerow(row)
    return output.getvalue()

async def send_email_with_csv_data(csv_data):
    """Send CSV data as an email attachment."""
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = SUBJECT

    # Add body to email
    body = 'Attached is the CSV file containing the API data.'
    msg.attach(MIMEText(body, 'plain'))

    # Attach CSV data
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(csv_data.encode('utf-8'))
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={CSV_FILENAME}')
    msg.attach(part)

    # Send email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        logger.info(f"Email sent successfully to {RECIPIENT_EMAIL}")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")

async def post_request():
    async with aiohttp.ClientSession() as session:
        sticker_numbers = list(range(start_point, end_point))
        total_requests = len(sticker_numbers)

        results = []

        for i in tqdm(range(0, total_requests, batch_size), desc="Processing batches"):
            batch = sticker_numbers[i:i + batch_size]
            batch_results = await fetch_batch(session, batch)
            for result in batch_results:
                if isinstance(result, dict):
                    results.append(result)
                
                
                    print(len(results))

                if(len(results) >= email_batch_size):
                  csv_data = create_csv_in_memory(results)
                  await send_email_with_csv_data(csv_data)
                  results = []



                await asyncio.sleep(1)

def main():
    start = time.time()
    asyncio.run(post_request())


    end = time.time()
    total_time = end - start
    logger.info(f"It took {total_time:.2f} seconds to make {end_point - start_point} API calls")
    logger.info(f"Total errors: {len(error_stickers)}")
    if error_stickers:
        logger.info(f"Errors encountered: {error_stickers}")

if __name__ == "__main__":
    main()