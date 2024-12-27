import requests
import pandas as pd
from datetime import datetime
import json
import asyncio
import sys,os



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger


async def get_stock_info(campaigns_df, offer_ids, api_token, username, marketname, chunk_size=50):
    """
    Получение информации об остатках товаров для всех магазинов из DataFrame

    Параметры:
    ----------
    campaigns_df : DataFrame
        DataFrame с данными о магазинах (должен содержать campaign_id)
    offer_ids : list
        Список SKU для проверки остатков
    api_token : str
        API токен для авторизации
    username : str
        Имя пользователя для логирования
    marketname : str
        Название магазина для логирования
    chunk_size : int
        Размер пакета SKU для одного запроса
    """

    all_stocks = {}  # Используем словарь для хранения уникальных комбинаций

    try:
        if not offer_ids:
            logger.error(f"[{username}][{marketname}] Не предоставлен список SKU")
            return None

        logger.info(f"[{username}][{marketname}] Начало обработки {len(offer_ids)} SKU")

        for _, campaign in campaigns_df.iterrows():
            campaign_id = campaign['campaign_id']

            url = f'https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks'

            headers = {
                "Api-Key": api_token,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            for i in range(0, len(offer_ids), chunk_size):
                chunk = offer_ids[i:i + chunk_size]

                body = {
                    'offerIds': chunk
                }

                try:
                    response = requests.post(url, headers=headers, json=body)

                    if response.status_code != 200:
                        logger.debug(
                            f"[{username}][{marketname}] Ошибка получения данных для магазина {campaign_id}: {response.status_code}")
                        continue

                    data = response.json()

                    if data['status'] != 'OK':
                        logger.error(
                            f"[{username}][{marketname}] API вернул ошибку для магазина {campaign_id}: {data['status']}")
                        continue

                        # Обработка данных с использованием словаря для уникальности
                    for warehouse in data['result']['warehouses']:
                        for offer in warehouse['offers']:
                            offer_id = offer['offerId']
                            key = (campaign_id, offer_id)

                            # Считаем общее количество остатков
                            total_stock = sum(stock['count'] for stock in offer['stocks'])

                            # Пропускаем нулевые остатки
                            if total_stock > 0:
                                # Обновляем существующие остатки или создаем новую запись
                                if key in all_stocks:
                                    all_stocks[key] = max(all_stocks[key], total_stock)
                                else:
                                    all_stocks[key] = total_stock

                except requests.exceptions.RequestException as e:
                    logger.error(f"[{username}][{marketname}] Ошибка запроса для магазина {campaign_id}: {str(e)}")
                    continue

        if not all_stocks:
            logger.warning(f"[{username}][{marketname}] Не получено данных об остатках")
            return None

            # Преобразуем словарь в DataFrame
        stocks_data = [
            {
                'campaign_id': campaign_id,
                'offer_id': offer_id,
                'stock_count': count
            }
            for (campaign_id, offer_id), count in all_stocks.items()
        ]

        df = pd.DataFrame(stocks_data)

        # Сохраняем результат
        # output_file = f'stocks_{marketname}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        # df.to_csv(output_file, index=False)
        # logger.info(f"[{username}][{marketname}] Данные об остатках сохранены в файл: {output_file}")

        return df

    except Exception as e:
        logger.error(f"[{username}][{marketname}] Непредвиденная ошибка при получении остатков: {str(e)}")
        return None


async def main():
    """
    Основная функция для получения данных о магазинах и их остатках
    """
    API_TOKEN = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    USERNAME = "test_user"
    MARKETNAME = "test_shop"

    # Получаем список магазинов
    campaigns_df = pd.read_csv('campaings.csv')

    if campaigns_df is not None:
        # Подготавливаем список SKU
        sku_df = pd.read_csv('data.csv')
        offer_ids = sku_df['offer_id'].tolist()  # Здесь должен быть ваш список SKU

        # Получаем остатки для всех магазинов
        stocks_df = await get_stock_info(
            campaigns_df=campaigns_df,
            offer_ids=offer_ids,
            api_token=API_TOKEN,
            username=USERNAME,
            marketname=MARKETNAME
        )

        if stocks_df is not None:
            logger.info(f"[{USERNAME}][{MARKETNAME}] Обработка данных успешно завершена")



if __name__ == "__main__":
    asyncio.run(main())