import pandas as pd
import time
import requests
import re
import threading
from bs4 import BeautifulSoup

# Telegram Bot Credentials
# Add your Telegram Credential
import os

TELEGRAM_BOTS = {
    "low": {"token": os.getenv("TELEGRAM_LOW_TOKEN"), "chat_id": os.getenv("TELEGRAM_CHAT_ID")},
    "medium": {"token": os.getenv("TELEGRAM_MEDIUM_TOKEN"), "chat_id": os.getenv("TELEGRAM_CHAT_ID")},
    "high": {"token": os.getenv("TELEGRAM_HIGH_TOKEN"), "chat_id": os.getenv("TELEGRAM_CHAT_ID")}
}


CSV_FILE_PATH = "B:\Amazon data\products\splits\largedata_part_10.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
THREAD_COUNT = 10

def send_telegram_message(bot, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOTS[bot]['token']}/sendMessage"
    data = {"chat_id": TELEGRAM_BOTS[bot]['chat_id'], "text": message, "parse_mode": "Markdown"}
    requests.post(url, data=data)

def extract_coupon_info(soup):
    try:
        coupon_span = soup.find("span", class_="couponLabelText")
        if coupon_span:
            return coupon_span.text.strip()

        flat_coupon = soup.find("span", string=re.compile(r"Save\s*₹?\s*\d+", re.IGNORECASE))
        if flat_coupon:
            return flat_coupon.text.strip()

        percent_coupon = soup.find("span", string=re.compile(r"Save\s*\d+\s*%", re.IGNORECASE))
        if percent_coupon:
            return percent_coupon.text.strip()

        return None
    except Exception as e:
        print(f"⚠️ Error extracting coupon info: {e}")
        return None

def is_product_unavailable(soup):
    availability = soup.select_one("#availability")
    return availability and "currently unavailable" in availability.text.lower()

def get_latest_price(product_link):
    try:
        response = requests.get(product_link, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            if is_product_unavailable(soup):
                print(f"❌ Product unavailable: {product_link}")
                return None, None

            price_div = soup.find('div', id='corePriceDisplay_desktop_feature_div')
            if price_div:
                price_whole = price_div.find("span", {"class": "a-price-whole"})
                price_fraction = price_div.find("span", {"class": "a-price-fraction"})

                if price_whole:
                    price_text = price_whole.text.strip().replace(",", "")
                    if price_fraction and price_fraction.text.strip().isdigit():
                        price_text += "." + price_fraction.text.strip()
                    else:
                        price_text += ".00"
                    price_text = re.sub(r'[^0-9.]', '', price_text)
                    if price_text.count(".") > 1:
                        price_text = price_text.replace(".", "", price_text.count(".") - 1)

                    coupon_info = extract_coupon_info(soup)
                    return float(price_text), coupon_info
    except Exception as e:
        print(f"Error fetching price: {e}")
    return None, None

def clean_price(price_str):
    if isinstance(price_str, str):
        price_str = re.sub(r'[^0-9.]', '', price_str)
    return float(price_str) if price_str else None

def process_chunk(chunk):
    threads = []
    for index, row in chunk.iterrows():
        thread = threading.Thread(target=process_product, args=(row,))
        threads.append(thread)
        thread.start()
        if len(threads) >= THREAD_COUNT:
            for t in threads:
                t.join()
            threads = []
    for t in threads:
        t.join()

def process_product(row):
    try:
        title = row["Product Name"]
        previous_price = clean_price(row["Discounted Price"])
        product_link = row["Product Link"]

        latest_price, coupon_info = get_latest_price(product_link)
        if latest_price is None:
            return

        effective_price = latest_price
        coupon_discount = 0.0
        coupon_note = ""

        if coupon_info:
            flat_match = re.search(r'₹\s?(\d+)', coupon_info.replace(",", ""))
            percent_match = re.search(r'(\d+)%', coupon_info)

            if flat_match:
                coupon_discount = float(flat_match.group(1))
                effective_price -= coupon_discount
                coupon_note = f"Coupon: ₹{coupon_discount} off"
            elif percent_match:
                percent = float(percent_match.group(1))
                coupon_discount = (percent / 100.0) * latest_price
                effective_price -= coupon_discount
                coupon_note = f"Coupon: {percent}% off"

        if previous_price is not None:
            discount_percent = ((previous_price - effective_price) / previous_price) * 100
            discount_percent = round(discount_percent, 2)

            print(f"🔍 {title} | Prev: ₹{previous_price} | New: ₹{effective_price:.2f} | Discount: {discount_percent}%")

            if 45 <= discount_percent < 80:
                bot = "low"
            elif 80 <= discount_percent <= 90:
                bot = "medium"
            elif discount_percent > 90:
                bot = "high"
            else:
                return

            message = (
                f"🔔 *Price Drop Alert!* 🔔\n\n"
                f"🛒 *{title}*\n"
                f"💰 New Price: ₹{effective_price:.2f}\n"
                f"💲 Previous Price: ₹{previous_price}\n"
                f"📉 Discount: {discount_percent}%\n"
            )

            if coupon_note:
                message += f"🏷️ {coupon_note}\n"

            message += f"🔗 [Buy Now]({product_link})"
            send_telegram_message(bot, message)

    except Exception as e:
        print(f"Error processing product: {e}")

def monitor_prices_once():
    try:
        df = pd.read_csv(CSV_FILE_PATH, usecols=["Product Name", "Discounted Price", "Product Link"])
        if not all(col in df.columns for col in ["Product Name", "Discounted Price", "Product Link"]):
            print("Error: Missing required columns in CSV file.")
            return
        process_chunk(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    monitor_prices_once()
