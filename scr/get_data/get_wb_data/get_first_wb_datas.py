import asyncio
import aiohttp
import pandas as pd
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger


async def get_goods_wb(
        api_key: str,
        limit: Optional[int] = 10,
        offset: Optional[int] = 0,
        filter_nm_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Асинхронная функция для получения информации о товарах через API Wildberries.

    Args:
        api_key (str): API ключ для авторизации
        limit (int, optional): Количество товаров на странице (макс. 1000). По умолчанию 10
        offset (int, optional): Смещение от начала списка. По умолчанию 0
        filter_nm_id (int, optional): Артикул товара Wildberries для фильтрации

    Returns:
        Dict[str, Any]: Ответ API с информацией о товарах
    """
    url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    params = {
        "limit": min(limit, 1000),
        "offset": max(offset, 0)
    }

    if filter_nm_id is not None:
        params["filterNmID"] = filter_nm_id

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        logger.info(f"Успешно получены данные о товарах. Статус: {response.status}")
                        return response_data
                    except aiohttp.client_exceptions.ContentTypeError:
                        response_text = await response.text()
                        logger.error(f"Ошибка при разборе ответа от API. Статус: {response.status}")
                        logger.error(f"Текст ответа: {response_text}")
                        return None
                else:
                    response_text = await response.text()
                    logger.error(f"Ошибка при выполнении запроса. Статус: {response.status}")
                    logger.error(f"Текст ответа: {response_text}")
                    return None

    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении запроса: {str(e)}")
        return None


def convert_wb_data_to_df(response_data: Dict) -> pd.DataFrame:
    """
    Преобразует ответ API Wildberries в pandas DataFrame.

    Args:
        response_data (Dict): Ответ API Wildberries

    Returns:
        pd.DataFrame: DataFrame с данными о товарах
    """
    if not response_data or 'data' not in response_data or 'listGoods' not in response_data['data']:
        logger.error("Некорректные данные для преобразования в DataFrame")
        return pd.DataFrame()

    processed_data = []
    # print(response_data)

    for item in response_data['data']['listGoods']:
        size_data = item['sizes'][0] if item['sizes'] else {}

        product_data = {
            'nmID': item['nmID'],
            'vendorCode': item['vendorCode'],
            'price': size_data.get('price', 0),
            'discountedPrice': size_data.get('discountedPrice', 0),
            'discount': item.get('discount', 0),
            'techSizeName': size_data.get('techSizeName', ''),
            'sizeID': size_data.get('sizeID', ''),
            'currencyIsoCode': item.get('currencyIsoCode4217', 'RUB'),
            'editableSizePrice': item.get('editableSizePrice', False)
        }
        processed_data.append(product_data)

    df = pd.DataFrame(processed_data)

    dtype_dict = {
        'nmID': 'int64',
        'vendorCode': 'str',
        'price': 'float64',
        'discountedPrice': 'float64',
        'discount': 'int64',
        'techSizeName': 'str',
        'sizeID': 'int64',
        'currencyIsoCode': 'str',
        'editableSizePrice': 'bool'
    }

    for col, dtype in dtype_dict.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.error(f"Ошибка при преобразовании колонки {col}: {str(e)}")

    return df


async def transform_wb_data_async(
        df: pd.DataFrame,
        client_id: str = "",
        username: str = "",
        marketname: str = ""
) -> pd.DataFrame:
    """
    Асинхронная функция для преобразования DataFrame.

    Args:
        df (pd.DataFrame): Исходный DataFrame с данными Wildberries
        client_id (str): Номер магазина/клиента
        username (str): Имя пользователя
        marketname (str): Название магазина

    Returns:
        pd.DataFrame: Преобразованный DataFrame
    """
    logger.info(f"Начало преобразования данных для пользователя {username} магазина {marketname}")

    with ThreadPoolExecutor() as executor:
        final_df = await asyncio.get_event_loop().run_in_executor(
            executor,
            transform_wb_data_sync,
            df,
            marketname
        )

    logger.info(f"Преобразование данных завершено успешно для {username} ({marketname})")
    return final_df


def transform_wb_data_sync(df: pd.DataFrame, client_id: str = "") -> pd.DataFrame:
    """
    Синхронная функция преобразования DataFrame.
    """
    descriptions = pd.DataFrame([{
        'Client-Id': 'Номер магазина',
        'nmID': 'Артикул товара',
        'id': 'Артикул товара (как у нас)',
        'link': 'Ссылка на товар',
        'stocks': 'Общие непроданные остатки',
        'price': 'Цена в системе',
        'disc_old': 'Скидка в системе',
        't_price': 'Цена для применения',
        'discount': 'Скидка в процентах',
        'prim': 'Примечание(заполняется программой)'
    }])

    transformed_df = pd.DataFrame({
        'Client-Id': [client_id] * len(df),
        'nmID': df['nmID'].values,
        'id': df['vendorCode'].values,
        'link': df['nmID'].apply(lambda x: f"https://www.wildberries.ru/catalog/{x}/detail.aspx").values,
        'stocks': ['Нет значения'] * len(df),
        'price': df['price'].values,
        'disc_old': df['discount'].values,
        't_price': df['price'].values,
        'discount': df['discount'].values,
        'prim': ['Нет значения'] * len(df)
    })

    final_df = pd.concat([descriptions, transformed_df], ignore_index=True)
    return final_df


async def get_wb_data(
        api_key: str,
        limit: int = 10,
        username: str = "",
        marketname: str = ""
) -> Optional[pd.DataFrame]:
    """
    Основная функция для получения и преобразования данных Wildberries.

    Args:
        api_key (str): API ключ для авторизации
        limit (int): Количество товаров для получения
        username (str): Имя пользователя
        marketname (str): Название магазина

    Returns:
        Optional[pd.DataFrame]: Преобразованный DataFrame или None в случае ошибки
    """
    logger.info(f"Начало получения данных WB для пользователя {username} магазина {marketname}")

    try:
        result = await get_goods_wb(api_key=api_key, limit=limit)
        if not result:
            logger.error(f"Не удалось получить данные WB для {username} ({marketname})")
            return None

        initial_df = convert_wb_data_to_df(result)

        final_df = await transform_wb_data_async(
            initial_df,
            'client_id',
            username,
            marketname
        )

        logger.info(f"Данные успешно получены и преобразованы для {username} ({marketname})")
        return final_df

    except Exception as e:
        logger.error(f"Ошибка при получении/преобразовании данных WB для {username} ({marketname}): {str(e)}")
        return None


# async def main():
#     """
#     Пример использования функций модуля.
#     """
#
#     final_df = await get_wb_data(
#         api_key=api_key,
#         limit=200,
#         username=username,
#         marketname=marketname
#     )
#
#     if final_df is not None:
#         final_df.to_csv('final.csv')
#         # Преобразуем DataFrame в строку перед логированием
#         logger.info("Преобразованный DataFrame:")
#         logger.info("\n" + final_df.head().to_string())
#
#         final_df.to_excel("wb_goods_transformed.xlsx", index=False)
#         logger.info(f"Данные сохранены в файл для {username} ({marketname})")
#
#
# if __name__ == "__main__":
#     asyncio.run(main())

