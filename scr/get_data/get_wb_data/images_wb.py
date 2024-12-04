import aiohttp
import asyncio
import pandas as pd
from typing import Optional

async def find_working_basket(nm_id: int, session: aiohttp.ClientSession) -> Optional[int]:
    """Находит рабочий номер корзины для заданного nmID"""
    part = str(nm_id)[:6]
    vol = str(nm_id)[:4]

    async def check_basket(basket_num: int) -> tuple[int, bool]:
        url = f"https://basket-{basket_num:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/tm/1.webp"
        try:
            async with session.head(url, allow_redirects=True, timeout=2) as response:
                return basket_num, response.status == 200
        except (asyncio.TimeoutError, aiohttp.ClientError):
            return basket_num, False

    tasks = [asyncio.create_task(check_basket(num)) for num in range(10, 21)]
    try:
        done, pending = await asyncio.wait(tasks, timeout=5)
        for task in pending:
            task.cancel()
    except asyncio.TimeoutError:
        return None

    for task in done:
        try:
            basket_num, is_working = await task
            if is_working:
                return basket_num
        except Exception:
            continue
    return None

async def add_image_formulas(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет формулы изображений Wildberries в DataFrame."""
    result_df = df.copy()

    if 'nmID' not in df.columns or 'stocks' not in df.columns:
        raise ValueError("DataFrame должен содержать колонки 'nmID' и 'stocks'")

    mask = (
        pd.to_numeric(df['nmID'], errors='coerce').notna() &
        df['stocks'].notna() &
        (df['stocks'] != 0)
    )

    if 'image' in result_df.columns:
        # Проверяем отсутствие формулы IMAGE в ячейке
        mask = mask & (~result_df['image'].str.startswith('=IMAGE', na=True))
    else:
        result_df['image'] = None

    rows_to_process = df[mask]
    if len(rows_to_process) == 0:
        return result_df

    timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_read=5)
    connector = aiohttp.TCPConnector(limit=5, force_close=True)

    async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,*/*'
            }
    ) as session:
        for idx in rows_to_process.index:
            try:
                nm_id = int(df.loc[idx, 'nmID'])
                basket_num = await find_working_basket(nm_id, session)

                if basket_num:
                    part = str(nm_id)[:6]
                    vol = str(nm_id)[:4]
                    result_df.loc[idx, 'image'] = (
                        f'=IMAGE("https://basket-{basket_num:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/tm/1.webp"; 3)'
                    )
            except Exception:
                continue

            await asyncio.sleep(0.1)

    return result_df


# async def main():
#     # # Создаем тестовый датафрейм
#     # df = pd.read_csv('result_im.csv')
#     #
#     # # Создаем простой логгер
#     # # from logger import logger
#     #
#     # # Вызываем функцию
#     # result_df = await add_image_formulas(df)
#     # result_df.to_csv('result_im.csv')
#     # print("Результат:")
#     # print(result_df)
#     #
#
# if __name__ == "__main__":
#     asyncio.run(main())