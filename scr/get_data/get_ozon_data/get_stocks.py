import aiohttp
import asyncio
import pandas as pd
from typing import Dict, List, Optional


async def get_stocks_info(
        client_id: str,
        api_key: str,
        offer_ids: Optional[List[str]] = None,
        product_ids: Optional[List[int]] = None,
        limit: int = 100
) -> pd.DataFrame:
    """
    Асинхронная функция для получения информации о количестве товаров на складах Ozon

    Args:
        client_id: Идентификатор клиента
        api_key: API-ключ
        offer_ids: Список идентификаторов товаров продавца
        product_ids: Список идентификаторов товаров Ozon
        limit: Количество товаров на странице (макс. 1000)

    Returns:
        DataFrame с информацией о количестве товаров
    """

    url = "https://api-seller.ozon.ru/v3/product/info/stocks"

    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    # Формируем фильтр
    filter_data = {}
    if offer_ids:
        filter_data["offer_id"] = offer_ids
    if product_ids:
        filter_data["product_id"] = product_ids

    data = {
        "filter": filter_data,
        "limit": limit,
        "last_id": ""
    }

    all_items = []

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            try:
                async with session.post(url, json=data) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message=f"Error: {error_text}"
                        )

                    result = await response.json()
                    result = result["result"]
                    items = result["items"]

                    if not items:
                        break

                    for item in items:
                        for stock in item["stocks"]:
                            all_items.append({
                                "product_id": item["product_id"],
                                "offer_id": item["offer_id"],
                                "warehouse_type": stock["type"],
                                "present": stock["present"],
                                "reserved": stock["reserved"]
                            })

                    last_id = result.get("last_id")
                    if not last_id or last_id == "null":
                        break

                    data["last_id"] = last_id

            except aiohttp.ClientError as e:
                print(f"Ошибка при выполнении запроса: {e}")
                break

    if not all_items:
        return pd.DataFrame()

    return pd.DataFrame(all_items)


async def add_stock_columns(main_df: pd.DataFrame, stock_df: pd.DataFrame) -> pd.DataFrame:
    """
    Асинхронная функция для добавления колонок stock_fbo и stock_fbs к основному DataFrame

    Args:
        main_df: Основной DataFrame с данными о товарах
        stock_df: DataFrame с информацией о стоках

    Returns:
        DataFrame с добавленными колонками stock_fbo и stock_fbs на позициях 8 и 9
    """

    loop = asyncio.get_event_loop()

    async def process_dataframes():
        # Сохраняем первую строку с описаниями
        descriptions = main_df.iloc[0].copy()

        # Создаем копию основного DataFrame без первой строки для обработки
        work_df = main_df.iloc[1:].copy()

        # Создаем сводные таблицы для FBO и FBS
        fbo_stocks = stock_df[stock_df['warehouse_type'] == 'fbo'].groupby('offer_id')[
            'present'].first().reset_index()
        fbs_stocks = stock_df[stock_df['warehouse_type'] == 'fbs'].groupby('offer_id')[
            'present'].first().reset_index()

        # Переименовываем колонки
        fbo_stocks.columns = ['offer_id', 'stock_fbo']
        fbs_stocks.columns = ['offer_id', 'stock_fbs']

        # Преобразуем offer_id в нужный тип данных
        work_df['offer_id'] = work_df['offer_id'].astype(fbo_stocks['offer_id'].dtype)

        # Добавляем новые колонки через merge
        work_df = work_df.merge(fbo_stocks, on='offer_id', how='left')
        work_df = work_df.merge(fbs_stocks, on='offer_id', how='left')

        # Заполняем пропущенные значения нулями
        work_df['stock_fbo'] = work_df['stock_fbo'].fillna(0)
        work_df['stock_fbs'] = work_df['stock_fbs'].fillna(0)

        # Создаем новый DataFrame с описаниями
        # Добавляем строку с описаниями
        descriptions['stock_fbo'] = 'Количество товара на складе FBO'
        descriptions['stock_fbs'] = 'Количество товара на складе FBS'

        # Собираем финальный DataFrame
        result_df = pd.concat([pd.DataFrame([descriptions]), work_df], ignore_index=True)

        # Получаем список всех колонок
        cols = list(result_df.columns)

        # Удаляем stock_fbo и stock_fbs из текущих позиций
        cols.remove('stock_fbo')
        cols.remove('stock_fbs')

        # Вставляем их на позиции 8 и 9
        cols.insert(8, 'stock_fbo')
        cols.insert(9, 'stock_fbs')

        # Переупорядочиваем колонки
        result_df = result_df[cols]

        return result_df

    result = await loop.run_in_executor(None, lambda: asyncio.run(process_dataframes()))

    return result

# async def main():
#     client_id = "1921962"
#     api_key = "95cb9589-6502-4a8a-827f-9798226a279d"
#
#     df = await get_stocks_info(
#         client_id=client_id,
#         api_key=api_key,
#     )
#     print(df)
#     main_df = pd.read_csv('sell.csv')
#     df.to_csv('stocks.csv')
#     result_df = await add_stock_columns(main_df, df)
#     print(result_df)
#     result_df.to_csv('wstocks.csv')
#
# # Запуск
# asyncio.run(main())








