import aiohttp
import pandas as pd
import json
import logging
import sys,os


# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger  # Импортируем настроенный логгер

logging = logger


async def update_prices_mm(df, token, offer_id_col, price_col,  debug=False):
    async with aiohttp.ClientSession() as session:
        url = "https://api.megamarket.tech/api/merchantIntegration/v1/offerService/manualPrice/save"

        prices = []
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            isDeleted = False
            prices.append({
                "offerId": str(offer_id),
                "price": int(row[price_col]),
                "isDeleted": bool(isDeleted)
            })

        data = {
            "meta": {},
            "data": {
                "token": token,
                "prices": prices
            }
        }

        if debug == True:
            logging.info("Отладочный режим  для MM включен. Запрос не будет отправлен.")
            logging.info("Отправляемые данные:")
            logging.info(json.dumps(data, indent=2))
            return False
        else:
            async with session.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(data)) as response:
                if response.status == 200:
                    try:
                        response_data = await response.json()
                        logging.info(f"Цены для товара с артикулом {offer_id} успешно обновлены!")
                        logging.info(f"Ответ сервера: {response_data}")
                        return True
                    except aiohttp.client_exceptions.ContentTypeError:
                        response_text = await response.text()
                        logging.error(f"Ошибка при обновлении цен для товара с артикулом {offer_id}: {response_text}")
                        logging.info(f"Статус ответа: {response.status}")
                        logging.info(f"Заголовки ответа: {response.headers}")
                        return False
                else:
                    response_text = await response.text()
                    logging.error(f"Ошибка при отправке в МегаМаркет цен для товара с артикулом {offer_id}: {response_text}")
                    logging.info(f"Статус ответа: {response.status}")
                    logging.info(f"Заголовки ответа: {response.headers}")
                    return  False

# Пример использования
df = pd.DataFrame({
    "offer_id": ["103616"],
    "price": [2790],
    "is_deleted": [False]
})

# asyncio.run(update_prices_mm(df, token, "offer_id", "price", debug=False))