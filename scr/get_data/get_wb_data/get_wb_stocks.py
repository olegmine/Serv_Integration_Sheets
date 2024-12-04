import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger

async def get_stocks(api_key: str, marketname: str, username: str, date_from: Optional[str] = None) -> pd.DataFrame:
    """
    Асинхронно получает данные об остатках товаров с API Wildberries

    Args:
        api_key (str): Ключ API Wildberries
        marketname (str): Название маркетплейса
        username (str): Имя пользователя
        date_from (str, optional): Дата в формате YYYY-MM-DD

    Returns:
        pandas.DataFrame: DataFrame с данными об остатках
    """
    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

    if date_from is None:
        date_from = (datetime.now(timezone(timedelta(hours=3))) - timedelta(days=365)).strftime('%Y-%m-%d')

    headers = {
        'Authorization': api_key
    }

    params = {
        'dateFrom': date_from
    }

    logger.info(
        "Начало запроса остатков товаров WB",
        marketname=marketname,
        username=username,
        дата_от=date_from
    )

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(base_url, headers=headers, params=params) as response:
                response_data = await response.json()

                if not response.ok:
                    logger.error(
                        "Ошибка запроса к API WB",
                        marketname=marketname,
                        username=username,
                        код_ответа=response.status,
                        данные_ошибки=response_data
                    )

                    if response.status == 401:
                        error_msg = format_error_message(response_data)
                        raise Exception(f"Ошибка авторизации: {error_msg}")
                    elif response.status == 429:
                        raise Exception("Превышен лимит запросов (максимум 1 запрос в минуту).")
                    else:
                        error_msg = format_error_message(response_data)
                        raise Exception(f"Ошибка API: {error_msg}")

                # Преобразуем успешный ответ в DataFrame
                df = pd.DataFrame(response_data)

                # Преобразуем даты в datetime
                if 'lastChangeDate' in df.columns:
                    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])

                logger.info(
                    "Успешно получены данные об остатках WB",
                    marketname=marketname,
                    username=username,
                    количество_строк=len(df),
                    дата_от=date_from
                )

                return df

        except aiohttp.ClientError as e:
            logger.error(
                "Сетевая ошибка при запросе к API WB",
                marketname=marketname,
                username=username,
                ошибка=str(e),
                тип_ошибки=type(e).__name__
            )
            raise Exception(f"Ошибка при выполнении запроса: {str(e)}")
        except Exception as e:
            logger.error(
                "Непредвиденная ошибка при запросе к API WB",
                marketname=marketname,
                username=username,
                ошибка=str(e),
                тип_ошибки=type(e).__name__
            )
            raise

def format_error_message(error_data: dict) -> str:
    """Форматирует сообщение об ошибке из ответа API"""
    return f"""  
Ошибка API Wildberries:  
- Заголовок: {error_data.get('title', 'Неизвестная ошибка')}  
- Детали: {error_data.get('detail', '')}  
- Код ошибки: {error_data.get('code', '')}  
- ID запроса: {error_data.get('requestId', '')}  
- Источник: {error_data.get('origin', '')}  
- HTTP статус: {error_data.get('status', '')} ({error_data.get('statusText', '')})  
- Время запроса: {error_data.get('timestamp', '')}  
"""

async def main():
    API_KEY = "your_api_key"  # Замените на ваш API ключ
    MARKET_NAME = "wildberries"
    USERNAME = "your_username"

    try:
        df = await get_stocks(
            api_key=API_KEY,
            marketname=MARKET_NAME,
            username=USERNAME
        )
        df.to_excel('market.xlsx')
        return df

    except Exception as e:
        logger.error(
            "Ошибка при получении данных",
            marketname=MARKET_NAME,
            username=USERNAME,
            ошибка=str(e),
            тип_ошибки=type(e).__name__
        )
        raise

if __name__ == "__main__":
    df = asyncio.run(main())