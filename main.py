import requests
import pandas as pd
from loguru import logger
import time

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69",
}

new_order = [
    'stock_id',
    'bond_id',
    'stock_nm',
    'margin_flg',
    'bond_nm',
    'apply_tips',
    'progress_nm',
    'progress_dt',
    'progress_full_1',
    'progress_full_2',
    'progress_full_3',
    'progress_full_4',
    'progress_full_5',
    'amount',
    'cb_type',
    'rating_cd',
    'ration_rt',
    'convert_price',
    'price',
    'increase_rt',
    'pma_rt',
    'pb',
    'cb_amount',
    'ration',
    'apply10',
    'record_dt',
    'online_amount',
    'lucky_draw_rt',
    'single_draw',
    'valid_apply',
    'offline_limit',
    'offline_draw',
    'offline_accounts',
    'underwriter_rt',
    'rid',
    'audit_id',
    'registration',
    'progress',
    'accept_date',
    'b_shares',
    'pg_shares',
    'ma20_price',
    'naps',
    'cb_flag',
    'apply_date',
    'apply_cd',
    'ration_cd',
    'record_price',
    'list_date',
    'list_price',
    'status_cd',
    'orders',
    'cp_flag',
    'ap_flag',
    'valid_apply_raw',
    'jsl_advise_text'
]


def fetch_data(url, max_retries=10, retry_interval=30):
    retries = 0

    while retries <= max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise error in case of failure

            json_data = response.json()
            if json_data['code'] != 200:
                logger.error(f"Scrape failed! {json_data['msg']}, retrying...")
                time.sleep(retry_interval)
                continue

            json_list = split_progress_full(json_data['data'])

            df = pd.DataFrame(json_list)
            logger.info("Scrape successes!")
            return df

        except requests.RequestException as e:
            retries += 1
            logger.error(f"Scrape failed! {e}, retrying...")
            time.sleep(retry_interval)

    logger.error(f"Scrape failed! {e}, max retries exceeded!")
    return None


def split_progress_full(json_list):
    """progress_full"""
    for row in json_list:
        progress_full_stripped = row['progress_full'].strip()

        # 使用\n分割字符串
        progress_full_split = progress_full_stripped.split('\n')

        # 删除原始的'progress_full'键值
        del row['progress_full']

        # 将新键值对添加到字典中
        for i, item in enumerate(progress_full_split, start=1):
            new_key = f'progress_full_{i}'
            row[new_key] = item

    return json_list


if __name__ == "__main__":
    url = "https://www.jisilu.cn/webapi/cb/pre/?history=N"

    while 1:
        df = fetch_data(url)

        # 使用新的列顺序重新排列 DataFrame
        df = df[new_order]

        if df is not None:
            print(df)

        df.to_excel('my_dataframe.xlsx', index=False)  # Only for test

        # Scrape every 24h
        time.sleep(24 * 60 * 60)
