import asyncio
import pandas as pd
from .get_ozon_data import get_products_report
from .get_stocks import get_stocks_info,add_stock_columns
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scr.logger import logger



async def from_ozon(client_id,api_key,market_name,user_id):
    try:
        logger.info(f'Запрос данных из Ozon для пользователя {user_id}')
        report_df = await get_products_report(
            client_id=client_id,
            api_key=api_key,
            marketname=market_name,
            username=user_id,
        )
    except Exception as e:
        report_df = None
        logger.error(
            "Критическая ошибка при запросе данных Ozon",
            extra={
                'user_id': user_id,
                'market_name': market_name,
                'error': str(e)
            }
        )
    if report_df is not None:
        try:
            stocks_df = await get_stocks_info(
                client_id=client_id,
                api_key=api_key,
            )
        except Exception as e:
            stocks_df = None
            logger.error(
                "Критическая ошибка при запросе данных об остатках Ozon",
                extra={
                    'user_id': user_id,
                    'market_name': market_name,
                    'error': str(e)
                }
            )
    else:
        stocks_df = None
    if report_df is not None and stocks_df is not None:
        try:
            result_df = await add_stock_columns(report_df, stocks_df)
        except Exception as e:
            result_df = None
            logger.error(
                "Критическая ошибка при обьеденении данных об остатках Ozon",
                extra={
                    'user_id': user_id,
                    'market_name': market_name,
                    'error': str(e)
                }
            )
    else:
        result_df = None

    return result_df

