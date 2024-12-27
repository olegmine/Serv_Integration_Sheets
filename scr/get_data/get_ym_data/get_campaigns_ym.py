import aiohttp
import asyncio
import json
from datetime import datetime
import pandas as pd
import sys,os



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger


async def get_campaigns(api_token, page_size=50):
    """
    Асинхронное получение списка всех доступных магазинов Яндекс.Маркет

    Параметры:
    ----------
    api_token : str
        API токен для авторизации
    page_size : int
        Количество магазинов на одной странице

    Возвращает:
    -----------
    DataFrame с данными о магазинах или None в случае ошибки
    """
    url = "https://api.partner.market.yandex.ru/campaigns"
    headers = {
        'Api-Key': api_token,
        'Accept': 'application/json'
    }

    # Список для хранения данных всех магазинов
    all_campaigns = []
    current_page = 1

    async with aiohttp.ClientSession() as session:
        try:
            while True:
                params = {
                    'page': current_page,
                    'pageSize': page_size
                }

                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Ошибка получения данных от API: {response.status}")
                        return None

                    data = await response.json()
                    campaigns = data.get('campaigns', [])
                    all_campaigns.extend(campaigns)

                    # Проверка наличия следующей страницы
                    pager = data.get('pager', {})
                    total_pages = pager.get('pagesCount', 0)

                    if current_page >= total_pages:
                        break

                    current_page += 1

            if all_campaigns:
                # Формируем DataFrame из полученных данных
                campaigns_data = [
                    {
                        'campaign_id': campaign.get('id'),
                        'domain': campaign.get('domain'),
                        'client_id': campaign.get('clientId'),
                        'business_id': campaign.get('business', {}).get('id'),
                        'business_name': campaign.get('business', {}).get('name'),
                        'placement_type': campaign.get('placementType')
                    }
                    for campaign in all_campaigns
                ]

                df = pd.DataFrame(campaigns_data)

                # Сохраняем результаты
                # output_file = f'campaigns_list_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                # df.to_csv(output_file, index=False, encoding='utf-8')
                # logger.info(f"Данные успешно сохранены в файл: {output_file}")
                return df

            logger.warning("Не найдено доступных магазинов")
            return None

        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return None




async def main():
    """
    Основная функция для запуска процесса получения и анализа данных о магазинах
    """
    API_TOKEN = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    campaigns_df = await get_campaigns(API_TOKEN)
    campaigns_df.to_csv('campaings.csv')



if __name__ == "__main__":
    asyncio.run(main())


