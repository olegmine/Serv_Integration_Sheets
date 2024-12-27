import aiohttp
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import asyncio
import time


class YandexMarketAPI:
    def __init__(self, oauth_token: str, business_id: str):
        self.base_url = "https://api.partner.market.yandex.ru"
        self.headers = {
            "Content-Type": "application/json",
            "Api-Key": oauth_token
        }
        self.business_id = business_id

    async def get_offers(self, session: aiohttp.ClientSession, page_token: Optional[str] = None,
                         limit: int = 200) -> Dict:
        endpoint = f"/businesses/{self.business_id}/offer-mappings"
        url = f"{self.base_url}{endpoint}"

        params = {}
        if page_token:
            params['page_token'] = page_token
        if limit:
            params['limit'] = min(limit, 200)

        async with session.post(url, headers=self.headers, params=params, json={}) as response:
            response.raise_for_status()
            return await response.json()

    async def get_offers_by_ids(self, session: aiohttp.ClientSession, offer_ids: List[str]) -> Dict:
        endpoint = f"/businesses/{self.business_id}/offer-mappings"
        url = f"{self.base_url}{endpoint}"

        chunk_size = 200
        all_results = []

        for i in range(0, len(offer_ids), chunk_size):
            chunk = offer_ids[i:i + chunk_size]
            payload = {"offerIds": chunk}

            async with session.post(url, headers=self.headers, json=payload) as response:
                response.raise_for_status()
                data = await response.json()

                if 'result' in data and 'offerMappings' in data['result']:
                    all_results.extend(data['result']['offerMappings'])

            await asyncio.sleep(0.1)

        return {"status": "OK", "result": {"offerMappings": all_results}}

    async def fetch_all_offers(self, session: aiohttp.ClientSession) -> List[Dict]:
        all_offers = []
        next_page_token = None

        while True:
            try:
                response_data = await self.get_offers(session, next_page_token)

                if 'result' in response_data and 'offerMappings' in response_data['result']:
                    offers = response_data['result']['offerMappings']
                    all_offers.extend(offers)

                if ('result' in response_data and
                        'paging' in response_data['result'] and
                        'nextPageToken' in response_data['result']['paging']):
                    next_page_token = response_data['result']['paging']['nextPageToken']
                else:
                    break

                await asyncio.sleep(0.1)

            except aiohttp.ClientError:
                break

        return all_offers

    def create_dataframe(self, offers: List[Dict]) -> pd.DataFrame:
        processed_offers = []

        for offer_mapping in offers:
            offer = offer_mapping.get('offer', {})
            mapping = offer_mapping.get('mapping', {})

            processed_offer = {
                'offer_id': offer.get('offerId'),
                'name': offer.get('name'),
                'vendor': offer.get('vendor'),
                'vendor_code': offer.get('vendorCode'),
                'category_name': offer.get('category'),
                'market_category_id': mapping.get('marketCategoryId'),
                'market_category_name': mapping.get('marketCategoryName'),
                'market_sku': mapping.get('marketSku'),
                'market_model_id': mapping.get('marketModelId'),
                'market_model_name': mapping.get('marketModelName'),
                'basic_price': offer.get('basicPrice', {}).get('value'),
                'basic_sale': offer.get('basicPrice', {}).get('discountBase'),
                'basic_price_currency': offer.get('basicPrice', {}).get('currencyId'),
                'description': offer.get('description'),
                'pictures': ','.join(offer.get('pictures', [])),
                'videos': ','.join(offer.get('videos', [])),
                'weight': offer.get('weightDimensions', {}).get('weight'),
                'length': offer.get('weightDimensions', {}).get('length'),
                'width': offer.get('weightDimensions', {}).get('width'),
                'height': offer.get('weightDimensions', {}).get('height'),
                'barcodes': ','.join(map(str, offer.get('barcodes', []))),
                'manufacturer_countries': ','.join(offer.get('manufacturerCountries', [])),
                'card_status': offer.get('cardStatus'),
                'archived': offer.get('archived', False),
                'updated_at': datetime.now().isoformat()
            }
            processed_offers.append(processed_offer)

        return pd.DataFrame(processed_offers)


async def fetch_yandex_market_data(oauth_token: str, business_id: str, output_file: str = None) -> pd.DataFrame:
    """
    Получает данные из API Яндекс.Маркета и возвращает их в виде DataFrame

    Аргументы:
        oauth_token (str): OAuth токен для аутентификации
        business_id (str): ID бизнеса для запросов к API

    Returns:
        pd.DataFrame: DataFrame containing the fetched data
    """
    api = YandexMarketAPI(oauth_token, business_id)

    async with aiohttp.ClientSession() as session:
        offers = await api.fetch_all_offers(session)

    df = api.create_dataframe(offers)

    if output_file:
        df.to_csv(output_file, index=False, encoding='utf-8')

    return df