from textwrap import indent
import os
from datetime import datetime
import pandas as pd
import requests
import json
import time
import html
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed



# ==== CẤU HÌNH ====
HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "referer": "https://www.google.com/",
}
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
MAX_WORKERS = 50       # Số luồng tối đa
RETRY_LIMIT = 3        # Số lần thử lại khi lỗi
BATCH_SIZE = 1000      # Số sản phẩm mỗi file
INPUT_FILE = "/home/manhzz/data_tiki/products_id.csv"

CHECKPOINT_DIR = 'checkpoints'
CHECKPOINT_FILE = 'tiki_crawler_checkpoint.json'
OUTPUT_DIR = 'tiki_data'  # Thư mục lưu file JSON

# tao thu muc can thiet
def create_directories():
    for directory in [CHECKPOINT_DIR, OUTPUT_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Da tao thu muc: {directory}")

# luu checkpoint sau moi batch
def save_checkpoint(batch_index, total_success, total_failed, failed_ids, completed_batches):

    checkpoint_data = {
        'timestamp': datetime.now().isoformat(),
        'last_completed_batch': batch_index,
        'total_success': total_success,
        'total_failed': total_failed,
        'failed_ids': failed_ids,
        'completed_batches': completed_batches,
        'status': 'in_progress'
    }

    checkpoint_path = os.path.join(CHECKPOINT_DIR, CHECKPOINT_FILE)
    with open(checkpoint_path, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

    print(f" Checkpoint da luu: batch {batch_index}")

# tai checkpoint de tiep tuc
def load_checkpoint():
    checkpoint_path = os.path.join(CHECKPOINT_DIR, CHECKPOINT_FILE)
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f" Da tai checkpoint: {data['timestamp']}")
            print(f" Batch cuoi: {data['last_completed_batch']}")
            print(f" Da thanh cong: {data['total_success']}")
            print(f" Da loi : {data['total_failed']}")
            return data
        except Exception as e:
            print(f" Loi khi doc checkpoint: {e}")
            return None
    return None

# danh dau da hoan thanh checkpoint
def mark_checkpoint_completed():
    checkpoint_path = os.path.join(CHECKPOINT_DIR, CHECKPOINT_FILE)
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            data['status'] = 'completed'
            data['completed_timestamp'] = datetime.now().isoformat()

            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Checkpoint danh dau hoan thanh")
        except Exception as e:
            print(f" Loi khi cap nhat checkpoint: {e}")


#=========functions=======
# ham chuan hoa descripsion
def clean_descripsion(raw_html):
    decoded =html.unescape(raw_html)
    soup = BeautifulSoup(decoded,"html.parser")
    return soup.get_text(strip=True)
# ham parser du lieu san pham
def parser_product(json_data):
    return{
        "id": json_data.get("id"),
        "name": json_data.get("name"),
        "url_key": json_data.get("url_key"),
        "price": json_data.get("price"),
        "description": clean_descripsion(json_data.get("description", "")),
        "images": json_data["images"][0]["base_url"] if json_data.get("images") else None
    }
# ham goi api voi retry
def fetch_products(pid):
    for attempt in range(RETRY_LIMIT):
        try:
            url = API_URL.format(pid)
            response = requests.get(url,headers=HEADERS,timeout=10)
            if response.status_code==200:
                print(f'{pid}-Success')
                return parser_product(response.json())
            else:
                print(f'{pid}-{response.status_code}')
        except Exception as e:
            print(f'{pid}-Attempt:{attempt+1}-failed:{e}')
        time.sleep(1)
    return None
# ghi File json theo batch
def save_batch_to_json(data,batch_index):
    start=batch_index * BATCH_SIZE
    end = start + len(data) -1
    filename= f'products_{start}_to_{end}.json'
    with open(filename,'w',encoding='utf-8',errors="ignore") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)
        #can co errors de loai bo nhung ki tu utf-8 khong ho tro cu the trong nay la emoji


# main
def main():

    create_directories()

    # Doc sanh sach ID
    try:
        df_id = pd.read_csv(INPUT_FILE)
        p_ids = df_id.id.dropna().astype(int).tolist()
        total_ids = len(p_ids)
        total_batches = (total_ids + BATCH_SIZE - 1) // BATCH_SIZE

        print(f" Tong so product ID: {total_ids:,}")
        print(f" Tong so batch: {total_batches}")
        print(f" Batch size: {BATCH_SIZE}")
        print(f" Max workers: {MAX_WORKERS}")

    except Exception as e:
        print(f" Loi khi doc file {INPUT_FILE}: {e}")
        return

    # Kiem tra va tai checkpoint
    checkpoint_data = load_checkpoint()

    # Khoi tao tracking variables
    if checkpoint_data and checkpoint_data['status'] == 'in_progress':
        start_batch = checkpoint_data['last_completed_batch'] + 1
        total_success = checkpoint_data['total_success']
        total_failed = checkpoint_data['total_failed']
        failed_ids = checkpoint_data['failed_ids']
        completed_batches = checkpoint_data['completed_batches']
        print(f" Tiep tuc tu batch {start_batch}")
    else:
        start_batch = 0
        total_success = 0
        total_failed = 0
        failed_ids = []
        completed_batches = []


    # Xu ly tung batch voi checkpoint
    try:
        for batch_index in range(start_batch, total_batches):
            start_idx = batch_index * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_ids)
            chunk = p_ids[start_idx:end_idx]

            if not chunk:
                break

            print(f"\n Dang xu ly batch {batch_index + 1}/{total_batches}")
            print(f" ID Tu {start_idx} → {end_idx - 1} ({len(chunk)} san pham)")

            # Crawl Du lieu song song
            batch_results = []
            batch_failed = []

            try:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    futures = {ex.submit(fetch_products, pid): pid for pid in chunk}
                    for future in tqdm(as_completed(futures), total=len(futures),desc=f" Batch {batch_index + 1}"):
                        pid = futures[future]
                        try:
                            res = future.result()
                            if res:
                                batch_results.append(res)
                                total_success += 1
                            else:
                                batch_failed.append(pid)
                                total_failed += 1
                        except Exception as e:
                            print(f"Loi xu ly id {pid}: {e}")
                            batch_failed.append(pid)
                            total_failed += 1

                # Luu ket qua batch
                if batch_results:
                    save_batch_to_json(batch_results, batch_index)
                    print(f" Batch {batch_index + 1}: {len(batch_results)} san phan thanh cong")


                if batch_failed:
                    failed_ids.extend(batch_failed)
                    print(f" Batch {batch_index + 1}: {len(batch_failed)} san pham bi loi")

                # Cap nhat completed batches
                completed_batches.append(batch_index)

                # Luu checkpoint
                save_checkpoint(batch_index, total_success, total_failed, failed_ids, completed_batches)

                # Progress
                processed = total_success + total_failed
                progress_pct = processed / total_ids * 100
                print(f"Tien trinh tong : {processed:,}/{total_ids:,} ({progress_pct:.1f}%)")

                # Nghi giu cac batch de tranh rate limit
                if batch_index < total_batches - 1:  # Khong sleep o batch cuoi
                    print(" Nghi 3s...")
                    time.sleep(3)

            except KeyboardInterrupt:
                print(f"\n Dung chuong trinh tai batch {batch_index + 1}")
                print(f" Checkpoint da duoc luu, co the tiep tuc chay script")
                return
            except Exception as e:
                print(f"\n Loi nghiem trong tai batch {batch_index + 1}: {e}")
                print(f"Luu checkpoint va tiep tuc batch tiep theo...")
                save_checkpoint(batch_index, total_success, total_failed, failed_ids, completed_batches)
                continue

    except Exception as e:
        print(f"\n Loi khong mong muon: {e}")
        print(f"Checkpoint da tu dong duoc luu")
        return

    # Danh dau hoan thanh
    mark_checkpoint_completed()

    # final report
    print(f"\n{'=' * 60}")

    print(f"Tong so ID : {total_ids:,}")
    print(f"Tong so ID success: {total_success:,}")
    print(f"Tong so ID loi: {total_failed:,}")
    print(f"So batch da hoan thanh: {len(completed_batches)}")
    print(f"Ty le thanh cong: {total_success / total_ids * 100:.2f}%")
    print(f"Ty le loi :{total_failed / total_ids * 100:.2f}%")

    # Luu danh sach ID loi
    if failed_ids:
        failed_df = pd.DataFrame({'failed_id': failed_ids})
        failed_file = os.path.join(OUTPUT_DIR, 'failed_product_ids.csv')
        failed_df.to_csv(failed_file, index=False)
        print(f"Danh sach ID loi da luu: {failed_file}")


if __name__ == "__main__":
    # Uncomment để reset checkpoint
    # reset_checkpoint()

    # Uncomment để xem trạng thái checkpoint
    # show_checkpoint_status()

    main()

