import pandas as pd
import asyncio
import structlog
from datetime import datetime
from typing import Optional, Tuple
from .ym_get_data import fetch_yandex_market_data
from .merger import transform_wb_data_sync, update_stocks_data
from .get_stocks import get_stock_info
from .get_campaigns_ym import get_campaigns
import sys,os



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger

async def update_dataframe_ym(df1: pd.DataFrame, df2: pd.DataFrame, user_name: str, market_name: str) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame на основе offer_id,
    пропуская первую описательную строку.
    """

    def update_df():
        # Сохраняем описательную строку из первого DataFrame
        header_row = df1.iloc[0:1].copy()

        # Копируем DataFrame'ы без первой строки
        df1_updated = df1.iloc[1:].copy()
        df2_updated = df2.iloc[1:].copy()

        # Определяем колонки
        required_cols = ['price', 'price_old', 'id', 'link','stocks','image']
        optional_cols = ['Client-Id','t_price', 'discount_base']

        # Проверяем наличие обязательных колонок
        missing_required_cols = [col for col in required_cols if col not in df2_updated.columns]
        if missing_required_cols:
            raise ValueError(f"Отсутствуют обязательные колонки в df2: {', '.join(missing_required_cols)}")

        # Получаем списки product_id
        df1_products = set(df1_updated['offer_id'].astype(str))
        df2_products = set(df2_updated['offer_id'].astype(str))
        new_products = df2_products - df1_products

        # Добавляем метку времени для новых записей
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df2_updated['prim'] = df2_updated.apply(
            lambda row: f"Добавлен автоматически {current_time}"
            if str(row['offer_id']) in new_products
            else row.get('prim', ''),
            axis=1
        )

        # Разделяем df2 на новые и существующие записи
        df2_new = df2_updated[df2_updated['offer_id'].astype(str).isin(new_products)]
        df2_existing = df2_updated[~df2_updated['offer_id'].astype(str).isin(new_products)]

        # Обновляем существующие записи
        merged_existing = df1_updated.merge(
            df2_existing,
            on='offer_id',
            how='left',
            suffixes=('', '_new')
        )

        # Обновляем значения в существующих записях
        for col in required_cols:
            # Обязательные колонки обновляются всегда (кроме product_id)
            if col != 'offer_id' and f'{col}_new' in merged_existing.columns:
                merged_existing[col] = merged_existing[f'{col}_new'].fillna(merged_existing[col])

        for col in optional_cols:
            # Опциональные колонки обновляются только если текущее значение None или "Нет значения"
            if f'{col}_new' in merged_existing.columns:
                merged_existing[col] = merged_existing.apply(
                    lambda row: row[f'{col}_new']
                    if pd.notna(row[f'{col}_new']) and (
                        pd.isna(row[col])
                        or row[col] == ''
                        or row[col] == 'Нет значения'
                    )
                    else row[col],
                    axis=1
                )

        # Удаляем временные колонки
        merged_existing = merged_existing.drop(columns=[col for col in merged_existing.columns if col.endswith('_new')])

        # Добавляем новые записи
        if not df2_new.empty:
            # Убеждаемся, что все необходимые колонки присутствуют
            for col in required_cols + optional_cols:
                if col not in df2_new.columns:
                    df2_new[col] = ''

            # Объединяем существующие и новые записи
            final_df = pd.concat([merged_existing, df2_new], ignore_index=True)
        else:
            final_df = merged_existing

        # Заполняем пустые значения
        final_df = final_df.fillna('')

        # Добавляем описательную строку обратно в начало DataFrame
        final_df = pd.concat([header_row, final_df], ignore_index=True)

        logger.info(
            f"Обновление завершено. Исходные строки: {len(df1_updated)}, "  
            f"Новые строки: {len(df2_new)}, "  
            f"Итого строк: {len(final_df)}"
        )

        return final_df

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, update_df)
async def process_data_async(
        input_df: pd.DataFrame,
        stocks_df: pd.DataFrame,
        client_id: str,
) -> pd.DataFrame:
    """
    Асинхронная функция для обработки данных: трансформация и обновление остатков.
    """

    try:
        logger.info("Начало обработки данных", client_id=client_id)

        # Создаем событийный цикл для асинхронных операций
        loop = asyncio.get_event_loop()

        # Выполняем первичную трансформацию данных в отдельном потоке
        transformed_df = await loop.run_in_executor(
            None,
            transform_wb_data_sync,
            input_df,
            client_id
        )

        logger.info("Трансформация данных завершена", строк_преобразовано=len(transformed_df))

        # Обновляем остатки
        final_df = await update_stocks_data(
            old_df=transformed_df,
            new_df=stocks_df,
        )

        logger.info("Обработка данных успешно завершена", итоговых_строк=len(final_df))
        return final_df

    except Exception as e:
        logger.error("Ошибка при обработке данных", ошибка=str(e), тип_ошибки=type(e).__name__)
        raise


async def get_df_ym(
        api_key: str,
        business_id: int,
        client_id: str = 'TestMarket',
        username: str = 'testuser',
        marketname: str = 'testmarket',
        chunk_size: int = 50,
        page_size: int = 50,
        save_to_file: bool = True,
        output_file: str = 'processed_data.csv',
) -> Tuple[pd.DataFrame, bool]:
    """
    Единая точка входа для получения и обработки данных Яндекс.Маркета.

    Args:
        api_key: API ключ Яндекс.Маркета
        business_id: ID бизнеса
        client_id: ID клиента для трансформации
        username: Имя пользователя для получения остатков
        marketname: Название магазина для получения остатков
        chunk_size: Размер чанка для запроса остатков
        page_size: Размер страницы для запроса кампаний
        save_to_file: Сохранять ли результат в файл
        output_file: Путь к файлу для сохранения результата
        logger: Logger для записи информации

    Returns:
        Tuple[pd.DataFrame, bool]: (обработанный DataFrame, успех операции)
    """


    try:
        # Получаем основные данные
        df_data = await fetch_yandex_market_data(
            oauth_token=api_key,
            business_id=business_id
        )
        logger.info("Получены основные данные", строк=len(df_data))

        # Получаем данные о кампаниях
        campaigns_data = await get_campaigns(
            api_token=api_key,
            page_size=page_size
        )
        logger.info("Получены данные о кампаниях", кампаний=len(campaigns_data))

        # Получаем данные об остатках
        stocks_data = await get_stock_info(
            campaigns_df=campaigns_data,
            offer_ids=df_data['offer_id'].tolist(),
            api_token=api_key,
            username=username,
            marketname=marketname,
            chunk_size=chunk_size
        )
        logger.info("Получены данные об остатках", строк=len(stocks_data))

        # Обрабатываем данные
        result_df = await process_data_async(
            input_df=df_data,
            stocks_df=stocks_data,
            client_id=client_id,
        )

        # Сохраняем результат, если требуется
        if save_to_file:
            result_df.to_csv(output_file, index=False)
            logger.info("Результат сохранен в файл", файл=output_file)

        return result_df, True

    except Exception as e:
        logger.error(
            "Ошибка при получении и обработке данных",
            ошибка=str(e),
            тип_ошибки=type(e).__name__
        )
        return pd.DataFrame(), False




async def main():
    API_KEY = "test"
    BUSINESS_ID = 76443469

    result_df, success = await get_df_ym(
        api_key=API_KEY,
        business_id=BUSINESS_ID,
        save_to_file=False
    )
    result_df.to_csv('result.csv')

    if success:
        print("Обработка успешно завершена")
    else:
        print("Произошла ошибка при обработке")


if __name__ == "__main__":
    asyncio.run(main())







