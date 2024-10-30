# Description: Upbit에서 호가창 데이터와 체결 데이터를 수집하여 큐로 관리하고 파일로 저장하는 스크립트

import os
import asyncio
from datetime import datetime
import aiofiles
import traceback
import jinupbit

# Upbit API 객체 초기화
jupbit = jinupbit.JinUpbit("", "")

# 큐 크기 제한 설정
ORDERBOOK_QUEUE_LIMIT = 60
TICK_QUEUE_LIMIT = 60

# Upbit 호가 데이터를 수집하는 비동기 함수
async def upbit_get_orderbook():
    before_date = datetime.today()
    date = before_date.strftime('%Y%m%d')
    file_name = "/home/ubuntu/project/data/queue/orders.txt"

    try:
        bf_timestamp = 0
        orderbook_queue = []
        while True:
            # Upbit 호가 데이터 비동기 수집
            upbit_res = await jupbit.async_get_orderbook("KRW-BTC")

            # 응답이 딕셔너리인 경우 예외 처리
            if isinstance(upbit_res, dict):
                await exception_func(str(upbit_res), "Orderbook Error", "upbit_get_orderbook")

            # 이전 timestamp보다 작거나 같으면 다음으로
            if bf_timestamp >= int(upbit_res[0]['timestamp']):
                await asyncio.sleep(0.2)
                continue

            # 현재 timestamp 업데이트
            bf_timestamp = int(upbit_res[0]['timestamp'])
            upbit_res = upbit_res[0]
            
            # 불필요한 key 제거
            upbit_res.pop('level', None)
            upbit_res.pop('market', None)
            
            # 큐에 호가 데이터 추가
            orderbook_queue.insert(0, upbit_res)
            if len(orderbook_queue) > ORDERBOOK_QUEUE_LIMIT:
                orderbook_queue.pop()
            
            # 큐 데이터를 파일로 저장
            await write_queue_to_file(orderbook_queue, file_name)

            await asyncio.sleep(0.2)

    except Exception as e:
        await exception_func(e, "Orderbook Error", "upbit_get_orderbook")

# Upbit 체결 데이터를 수집하는 비동기 함수
async def upbit_get_ticks():
    before_date = datetime.today()
    date = before_date.strftime('%Y%m%d')
    file_name = "/home/ubuntu/project/data/queue/ticks.txt"

    try:
        bf_sequential_id = 0
        ticks_queue = []
        while True:
            # Upbit 체결 데이터 비동기 수집
            upbit_res = await jupbit.async_get_ticks("KRW-BTC", "", "", "", "")

            for r in upbit_res:
                if bf_sequential_id < r['sequential_id']:
                    bf_sequential_id = r['sequential_id']
                    
                    # 불필요한 key 제거
                    r.pop('trade_date_utc', None)
                    r.pop('trade_time_utc', None)
                    r.pop('sequential_id', None)
                    r.pop('market', None)
                    
                    # 큐에 체결 데이터 추가
                    ticks_queue.insert(0, r)
                    if len(ticks_queue) > TICK_QUEUE_LIMIT:
                        ticks_queue.pop()
                    
                    # 큐 데이터를 파일로 저장
                    await write_queue_to_file(ticks_queue, file_name)

            await asyncio.sleep(0.5)

    except Exception as e:
        await exception_func(e, "Tick Error", "upbit_get_ticks")

# 큐 데이터를 파일에 쓰는 함수
async def write_queue_to_file(queue, file_name):
    async with aiofiles.open(file_name, 'w') as file:
        for item in queue:
            await file.write(str(item) + "\n")

# 예외 처리 함수
async def exception_func(e, title, info):
    traceback_info = traceback.format_exc()
    print(f"traceback: {traceback_info}")
    print(f"error: {e}")
    print(f"title: {title}")
    print(f"info: {info}")

# 메인 함수
async def main():
    try:
        tasks = [
            asyncio.create_task(upbit_get_orderbook()),
            asyncio.create_task(upbit_get_ticks())
        ]
        await asyncio.gather(*tasks)
    except Exception as e:
        await exception_func(e, "Main Procedure Error", "main")

if __name__ == "__main__":
    asyncio.run(main())
