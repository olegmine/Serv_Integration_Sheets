import aiohttp
import asyncio
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd
import tempfile
from pathlib import Path
import os
from urllib.parse import urlparse
import sys
import io
from contextlib import asynccontextmanager
import numpy as np
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scr.logger import logger


def process_csv(file_path, market_name=None, username=None):
    """
    Читает CSV файл и очищает данные от апострофов.

    Args:
        file_path: Путь к CSV файлу.
        market_name: Название магазина (опционально).
        username: Имя пользователя (опционально).

    Returns:
        pandas.DataFrame: Очищенный DataFrame.
        None: Если произошла ошибка при обработке.
    """
    try:
        logger.info("Начало обработки файла",
                    extra={
                        "market_name": market_name or "Не указан",
                        "username": username or "Не указан",
                        "file_path": file_path
                    })

        # Читаем CSV файл
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')

        # Очищаем все строковые колонки от апострофов
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].astype(str).str.replace("'", "")

        logger.info("Файл успешно обработан",
                    extra={
                        "market_name": market_name or "Не указан",
                        "username": username or "Не указан",
                        "rows_count": len(df)
                    })

        return df

    except Exception as e:
        logger.error("Ошибка при обработке файла",
                     extra={
                         "market_name": market_name or "Не указан",
                         "username": username or "Не указан",
                         "error": str(e)
                     })
        return None



def process_dataframe(df, market_name=None, username=None):
    """
    Обрабатывает DataFrame согласно заданным правилам.

    Args:
        df (pandas.DataFrame): DataFrame для обработки.
        market_name (str, optional): Название магазина.
        username (str, optional): Имя пользователя.

    Returns:
        pandas.DataFrame: Обработанный DataFrame.
        None: Если произошла ошибка или отсутствуют необходимые колонки.
    """
    # Константы
    REQUIRED_COLUMNS = {
        'Артикул': 'offer_id',
        'FBO OZON SKU ID': 'product_id',
        'Статус товара' : 'status',
        'Текущая цена с учетом скидки, ₽': 'price',
        'Цена до скидки (перечеркнутая цена), ₽': 'price_old',
        'Наименование товара': 'id',
        'Доступно к продаже по схеме FBS, шт.': 'stock',
        'Рыночная цена, ₽': 'market_price',
        'Рейтинг': 'rating'
    }
    df.to_csv('from.csv')

    def log_operation(message, level='info', extra_data=None):
        """Вспомогательная функция для логирования"""
        base_extra = {
            "market_name": market_name or "Не указан",
            "username": username or "Не указан"
        }
        if extra_data:
            base_extra.update(extra_data)

        getattr(logger, level)(message, extra=base_extra)

    log_operation("Начало обработки DataFrame", extra_data={"input_df_shape": df.shape})

    # Проверка наличия необходимых колонок
    missing_columns = [col for col in REQUIRED_COLUMNS.keys() if col not in df.columns]
    if missing_columns:
        log_operation("Отсутствуют необходимые колонки",
                      level='error',
                      extra_data={"missing_columns": missing_columns})
        return None

    try:
        def safe_float_convert(series, default_value=0.0):
            """Безопасное преобразование в числовой формат"""
            try:
                if pd.api.types.is_numeric_dtype(series):
                    return series.astype(float)

                series = series.astype(str)
                series = series.replace(['', 'nan', 'None'], np.nan)
                series = series.str.replace(',', '.')
                return pd.to_numeric(series, errors='coerce').fillna(default_value)

            except Exception as e:
                log_operation(f"Ошибка при конвертации значений: {str(e)}", level='warning')
                return pd.to_numeric(series, errors='coerce').fillna(default_value)

                # Создание нового DataFrame с улучшенной структурой

        new_df = pd.DataFrame()

        # Базовые поля
        new_df['Client-Id'] = [market_name] * len(df)
        new_df['offer_id'] = df['Артикул'].fillna('').astype(str)
        new_df['product_id'] = df['FBO OZON SKU ID']
        new_df['id'] = df['Наименование товара']
        new_df['link'] = 'https://www.ozon.ru/product/' + df['FBO OZON SKU ID'].fillna('').astype(str)
        new_df['status'] = df['Статус товара']
        new_df['stock'] = safe_float_convert(df['Доступно к продаже по схеме FBS, шт.'], default_value=0)
        new_df['rating'] = safe_float_convert(df['Рейтинг'], default_value=0)

        # Ценовые поля
        new_df['market_price'] = safe_float_convert(df['Рыночная цена, ₽'])
        new_df['price'] = safe_float_convert(df['Текущая цена с учетом скидки, ₽'])
        new_df['price_old'] = safe_float_convert(df['Цена до скидки (перечеркнутая цена), ₽'])
        new_df['min_price_old'] = new_df['price']
        new_df['t_price'] = new_df['price']
        new_df['old_price'] = new_df['price_old']
        new_df['min_price'] = new_df['price']

        # Информационные поля


        new_df['prim'] = 'Нет значения'

        # Словарь с описаниями колонок
        column_descriptions = {
            'Client-Id': 'Номер магазина',
            'offer_id': 'Артикул товара',
            'product_id': 'SKU товара',
            'status': 'Статус товара',
            'price': 'Цена в системе',
            'price_old': 'Перечеркнутая цена в системе',
            'market_price': 'Рыночная цена на товар(если Озон предоставляет такую)',
            'min_price_old': 'Базовая минимальная цена участия в акциях',
            'id': 'Название Товара',
            'link': 'Ссылка на товар',
            't_price': 'Цена для применения',
            'old_price': 'Перечеркнутая цена для применения',
            'min_price': 'Минимальная цена для участия в акциях',
            'stock': 'Доступно к продаже (FBS)',
            'rating': 'Рейтинг товара',
            'prim': 'Примечание'
        }

        # Добавление строки с описаниями
        description_row = {col: column_descriptions.get(col, '') for col in new_df.columns}
        new_df_with_descriptions = pd.concat([
            pd.DataFrame([description_row]),
            new_df
        ], ignore_index=True)

        log_operation("DataFrame успешно обработан",
                      extra_data={"output_df_shape": new_df_with_descriptions.shape})

        return new_df_with_descriptions

    except Exception as e:
        log_operation(f"Ошибка при обработке DataFrame: {str(e)}", level='error')
        return None

async def get_products_report(
        client_id: str,
        api_key: str,
        marketname: str,
        username: str,
        language: str = "DEFAULT",
        offer_ids: Optional[List[str]] = None,
        search: Optional[str] = None,
        skus: Optional[List[int]] = None,
        visibility: str = "ALL",
        timeout_seconds: int = 300,
        check_interval_seconds: int = 10
) -> pd.DataFrame:
    """
    Получение отчёта о товарах Ozon в виде pandas DataFrame.

    Args:
        client_id: Идентификатор клиента Ozon
        api_key: API-ключ Ozon
        marketname: Название маркетплейса
        username: Имя пользователя
        language: Язык ответа ('DEFAULT', 'RU', 'EN')
        offer_ids: Список идентификаторов товаров в системе продавца
        search: Поисковый запрос
        skus: Список идентификаторов товаров в системе Ozon
        visibility: Фильтр видимости товаров
        timeout_seconds: Таймаут ожидания готовности отчёта в секундах
        check_interval_seconds: Интервал проверки готовности отчёта в секундах

    Returns:
        pd.DataFrame: DataFrame с данными отчёта

    Raises:
        Exception: При ошибке получения или обработки отчёта
    """


    logger.info("Начало получения отчета о товарах",
                marketname=marketname,
                username=username,
                operation="get_products_report",
                client_id=client_id,
                language=language)

    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }

    # 1. Создание отчёта
    try:
        create_url = "https://api-seller.ozon.ru/v1/report/products/create"
        create_payload = {
            "language": language,
            "offer_id": offer_ids or [],
            "search": search or "",
            "sku": skus or [],
            "visibility": visibility
        }

        logger.debug("Отправка запроса на создание отчета",
                     marketname=marketname,
                     username=username,
                     operation="get_products_report",
                     payload=create_payload)

        async with aiohttp.ClientSession() as session:
            async with session.post(create_url, headers=headers, json=create_payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error("Ошибка при создании отчета",
                                 marketname=marketname,
                                 username=username,
                                 operation="get_products_report",
                                 status_code=response.status,
                                 error=error_text)
                    raise Exception(f"Ошибка создания отчёта: {error_text}")

                data = await response.json()
                if "result" not in data or "code" not in data["result"]:
                    logger.error("Неожиданный формат ответа API",
                                 marketname=marketname,
                                 username=username,
                                 operation="get_products_report",
                                 response_data=data)
                    raise Exception(f"Неожиданный формат ответа API: {data}")

                report_code = data["result"]["code"]
                logger.info("Отчет успешно создан",
                            marketname=marketname,
                            username=username,
                            operation="get_products_report",
                            report_code=report_code)

    except Exception as e:
        logger.error("Критическая ошибка при создании отчета",
                     marketname=marketname,
                     username=username,
                     operation="get_products_report",
                     error=str(e),
                     exc_info=True)
        raise

    # 2. Ожидание готовности отчёта
    start_time = datetime.now()
    report_url = None

    try:
        info_url = "https://api-seller.ozon.ru/v1/report/info"
        while True:
            elapsed_time = (datetime.now() - start_time).total_seconds()
            if elapsed_time > timeout_seconds:
                logger.error("Превышено время ожидания отчета",
                             marketname=marketname,
                             username=username,
                             operation="get_products_report",
                             timeout_seconds=timeout_seconds,
                             elapsed_seconds=elapsed_time)
                raise TimeoutError(f"Превышено время ожидания отчёта ({timeout_seconds}с)")

            async with aiohttp.ClientSession() as session:
                async with session.post(info_url, headers=headers, json={"code": report_code}) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error("Ошибка при проверке статуса отчета",
                                     marketname=marketname,
                                     username=username,
                                     operation="get_products_report",
                                     status_code=response.status,
                                     error=error_text)
                        raise Exception(f"Ошибка получения информации об отчёте: {error_text}")

                    info = await response.json()
                    status = info.get("result", {}).get("status")

                    if status == "success":
                        report_url = info.get("result", {}).get("file")
                        if not report_url:
                            logger.error("Отсутствует URL файла в готовом отчете",
                                         marketname=marketname,
                                         username=username,
                                         operation="get_products_report",
                                         response_data=info)
                            raise Exception("Отсутствует URL файла в готовом отчёте")
                        logger.info("Отчет готов к скачиванию",
                                    marketname=marketname,
                                    username=username,
                                    operation="get_products_report",
                                    report_url=report_url)
                        break
                    elif status == "failed":
                        error = info.get("result", {}).get("error", "Неизвестная ошибка")
                        logger.error("Ошибка формирования отчета на стороне API",
                                     marketname=marketname,
                                     username=username,
                                     operation="get_products_report",
                                     error=error)
                        raise Exception(f"Ошибка формирования отчёта: {error}")
                    elif status in ["waiting", "processing"]:
                        logger.debug("Ожидание готовности отчета",
                                     marketname=marketname,
                                     username=username,
                                     operation="get_products_report",
                                     status=status,
                                     elapsed_seconds=elapsed_time)
                        await asyncio.sleep(check_interval_seconds)
                    else:
                        logger.error("Неизвестный статус отчета",
                                     marketname=marketname,
                                     username=username,
                                     operation="get_products_report",
                                     status=status)
                        raise Exception(f"Неизвестный статус отчёта: {status}")

    except Exception as e:
        logger.error("Ошибка при ожидании готовности отчета",
                     marketname=marketname,
                     username=username,
                     operation="get_products_report",
                     error=str(e),
                     exc_info=True)
        raise

    # 3. Скачивание и обработка отчёта
    try:
        # Создаём временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            temp_path = tmp_file.name

        logger.info("Начало скачивания файла отчета",
                    marketname=marketname,
                    username=username,
                    operation="get_products_report",
                    temp_path=temp_path)

        async with aiohttp.ClientSession() as session:
            async with session.get(report_url) as response:
                if response.status != 200:
                    logger.error("Ошибка при скачивании файла",
                                 marketname=marketname,
                                 username=username,
                                 operation="get_products_report",
                                 status_code=response.status)
                    raise Exception(f"Ошибка при скачивании: HTTP {response.status}")

                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0

                with open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            progress = (downloaded / total_size) * 100
                            logger.debug("Прогресс скачивания",
                                         marketname=marketname,
                                         username=username,
                                         operation="get_products_report",
                                         progress=f"{progress:.1f}%",
                                         downloaded_bytes=downloaded,
                                         total_bytes=total_size)
                # # Добавляем отладочное чтение и сохранение файла
                # try:
                #     with open(temp_path, 'r', encoding='utf-8') as debug_file:
                #         content = debug_file.read()
                #         logger.debug("Содержимое скачанного файла",
                #                      marketname=marketname,
                #                      username=username,
                #                      operation="get_products_report",
                #                      content=content[:1000])  # Первые 1000 символов для примера
                #
                #         # Сохраняем копию файла для отладки
                #         debug_path = f"debug_{marketname}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                #         with open(debug_path, 'w', encoding='utf-8') as debug_copy:
                #             debug_copy.write(content)
                #         logger.info(f"Отладочная копия сохранена в {debug_path}",
                #                     marketname=marketname,
                #                     username=username,
                #                     operation="get_products_report")
                # except Exception as e:
                #     logger.error(f"Ошибка при отладочном чтении файла: {str(e)}",
                #                  marketname=marketname,
                #                  username=username,
                #                  operation="get_products_report")

        logger.info("Файл успешно скачан, начинаем чтение в DataFrame",
                    marketname=marketname,
                    username=username,
                    operation="get_products_report")

        try:
            df = await asyncio.to_thread(process_csv, temp_path,marketname,username)
            try:
                df = await asyncio.to_thread(process_dataframe, df, marketname, username)
                logger.info('Начало окончательного преобразования датафрейма',
                            marketname=marketname,
                            username=username
                            )
            except:
                logger.error("Не удалось преобразовать файл",
                             marketname=marketname,
                             username=username,
                             operation="get_products_report",
                             )

            if df is None:
                logger.error("Не удалось прочитать файл",
                             marketname=marketname,
                             username=username,
                             operation="get_products_report",
                             )
                raise Exception("Не удалось прочитать файл")

            return df

        finally:
            # Удаляем временный файл
            try:
                os.unlink(temp_path)
                logger.debug("Временный файл удален",
                             marketname=marketname,
                             username=username,
                             operation="get_products_report",
                             temp_path=temp_path)
            except Exception as e:
                logger.warning("Не удалось удалить временный файл",
                               marketname=marketname,
                               username=username,
                               operation="get_products_report",
                               temp_path=temp_path,
                               error=str(e))

    except Exception as e:
        logger.error("Критическая ошибка при обработке отчета",
                     marketname=marketname,
                     username=username,
                     operation="get_products_report",
                     error=str(e),
                     exc_info=True)
        raise


async def sort_by_status_async(df):
    """
    Асинхронно сортирует DataFrame по статусу, гарантированно сохраняя описательную строку первой.
    """

    def sort_df():
        try:
            # Создаем полную копию DataFrame
            working_df = df.copy()

            # Явно отделяем описательную строку и данные
            header = working_df.iloc[[0]]  # Используем двойные скобки для сохранения DataFrame структуры
            data = working_df.iloc[1:].copy()

            # Определяем приоритеты статусов
            status_priority = {
                'Продается': 0,
                'Готов к продаже': 1,
                'Не продается': 2
            }

            # Создаем и сортируем только данные
            if 'status' in data.columns:
                # Создаем временный столбец для сортировки
                data = data.assign(
                    status_sort=data['status'].map(lambda x: status_priority.get(x, 3))
                )

                # Сортируем данные
                data = data.sort_values('status_sort').drop('status_sort', axis=1)

                # Собираем финальный DataFrame, принудительно размещая header первым
            result = pd.DataFrame()
            result = pd.concat([header, data], axis=0)

            # Сбрасываем индексы
            result.reset_index(drop=True, inplace=True)

            logger.info("DataFrame успешно отсортирован по статусу",
                        extra={
                            "total_rows": len(result),
                            "data_rows": len(data)
                        })

            # Финальная проверка
            if not result.iloc[0:1].equals(header):
                logger.error("Нарушен порядок строк после сортировки")
                # Принудительно восстанавливаем порядок
                result = pd.concat([header, result.iloc[1:]], axis=0).reset_index(drop=True)

            return result

        except Exception as e:
            logger.error(f"Ошибка при сортировке DataFrame: {str(e)}")
            return df

            # Выполняем сортировку асинхронно

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, sort_df)

async def update_dataframe_ozon(df1: pd.DataFrame, df2: pd.DataFrame, user_name: str, market_name: str) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame на основе product_id,
    пропуская первую описательную строку.
    """

    def update_df():
        # Сохраняем описательную строку из первого DataFrame
        header_row = df1.iloc[0:1].copy()

        # Копируем DataFrame'ы без первой строки
        df1_updated = df1.iloc[1:].copy()
        df2_updated = df2.iloc[1:].copy()

        # Определяем колонки
        required_cols = ['price', 'price_old', 'id', 'link', 'status','market_price', 'stock', 'rating']
        optional_cols = ['Client-Id', 'offer_id', 'product_id', 't_price', 'old_price', 'min_price_old', 'min_price']

        # Проверяем наличие обязательных колонок
        missing_required_cols = [col for col in required_cols if col not in df2_updated.columns]
        if missing_required_cols:
            raise ValueError(f"Отсутствуют обязательные колонки в df2: {', '.join(missing_required_cols)}")

        # Получаем списки product_id
        df1_products = set(df1_updated['product_id'].astype(str))
        df2_products = set(df2_updated['product_id'].astype(str))
        new_products = df2_products - df1_products

        # Добавляем метку времени для новых записей
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df2_updated['prim'] = df2_updated.apply(
            lambda row: f"Добавлен автоматически {current_time}"
            if str(row['product_id']) in new_products
            else row.get('prim', ''),
            axis=1
        )

        # Разделяем df2 на новые и существующие записи
        df2_new = df2_updated[df2_updated['product_id'].astype(str).isin(new_products)]
        df2_existing = df2_updated[~df2_updated['product_id'].astype(str).isin(new_products)]

        # Обновляем существующие записи
        merged_existing = df1_updated.merge(
            df2_existing,
            on='product_id',
            how='left',
            suffixes=('', '_new')
        )

        # Обновляем значения в существующих записях
        for col in required_cols:
            # Обязательные колонки обновляются всегда (кроме product_id)
            if col != 'product_id' and f'{col}_new' in merged_existing.columns:
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

# Пример использования
if __name__ == "__main__":
    async def example():
        try:
            df = await get_products_report(
                client_id="1336645",
                api_key="e6640a3f-d177-4b08-9487-59be840f8a8c",
                marketname="ozon",
                username="test_user",
                # language="RU"
            )
            df_fin = await sort_by_status_async(df)
            df_fin.to_csv('sell.csv')
            print(f"Получен DataFrame размером: {df.shape}")
            print("\nПервые несколько строк:")
            print(df.head())
            print("\nСтолбцы DataFrame:")
            print(df.columns.tolist())
        except Exception as e:
            print(f"Ошибка: {e}")


    asyncio.run(example())