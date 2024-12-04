import aiohttp
import asyncio
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd
import sys
import os
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger

async def update_dataframe_wb(df1: pd.DataFrame, df2: pd.DataFrame, user_name: str, market_name: str) -> pd.DataFrame:
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
        required_cols = ['price', 'disc_old', 'id', 'link']
        optional_cols = ['Client-Id','t_price', 'discount']

        # Проверяем наличие обязательных колонок
        missing_required_cols = [col for col in required_cols if col not in df2_updated.columns]
        if missing_required_cols:
            raise ValueError(f"Отсутствуют обязательные колонки в df2: {', '.join(missing_required_cols)}")

        # Получаем списки product_id
        df1_products = set(df1_updated['nmID'].astype(str))
        df2_products = set(df2_updated['nmID'].astype(str))
        new_products = df2_products - df1_products

        # Добавляем метку времени для новых записей
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df2_updated['prim'] = df2_updated.apply(
            lambda row: f"Добавлен автоматически {current_time}"
            if str(row['nmID']) in new_products
            else row.get('prim', ''),
            axis=1
        )

        # Разделяем df2 на новые и существующие записи
        df2_new = df2_updated[df2_updated['nmID'].astype(str).isin(new_products)]
        df2_existing = df2_updated[~df2_updated['nmID'].astype(str).isin(new_products)]

        # Обновляем существующие записи
        merged_existing = df1_updated.merge(
            df2_existing,
            on='nmID',
            how='left',
            suffixes=('', '_new')
        )

        # Обновляем значения в существующих записях
        for col in required_cols:
            # Обязательные колонки обновляются всегда (кроме product_id)
            if col != 'nmID' and f'{col}_new' in merged_existing.columns:
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


async def read_csv_async(filename: str) -> pd.DataFrame:
    """
    Асинхронно читает CSV файл
    """
    return await asyncio.to_thread(pd.read_csv, filename)


async def write_csv_async(df: pd.DataFrame, filename: str) -> None:
    """
    Асинхронно записывает датафрейм в CSV файл
    """
    return await asyncio.to_thread(lambda: df.to_csv(filename, index=False))


async def update_stocks_data(old_df: pd.DataFrame, new_df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Асинхронно обновляет данные о стоках из колонки quantityFull
    """
    try:
        logger.info(
            "Начало обновления данных о стоках",
            старых_строк=int(len(old_df)),
            новых_строк=int(len(new_df))
        )

        # Проверяем наличие нужной колонки
        if 'quantityFull' not in new_df.columns:
            logger.error("Колонка quantityFull не найдена во втором датафрейме")
            raise ValueError("Колонка quantityFull не найдена")

            # Сохраняем первую строку с описаниями
        descriptions = old_df.iloc[0:1].copy()

        # Удаляем первую строку из старого датафрейма для обработки
        old_df_without_desc = old_df.iloc[1:].copy()

        # Преобразуем nmID и nmId в числовой тип для корректного сравнения
        old_df_without_desc['nmID'] = pd.to_numeric(old_df_without_desc['nmID'], errors='coerce')
        new_df['nmId'] = pd.to_numeric(new_df['nmId'], errors='coerce')

        # Создаем маппинг nmId -> quantityFull из нового датафрейма
        stocks_mapping = dict(zip(new_df['nmId'], new_df['quantityFull']))

        # Выводим пример маппинга для проверки
        logger.info(
            "Пример данных для обновления",
            пример_маппинга=dict(list(stocks_mapping.items())[:3])
        )

        # Обновляем значения в колонке stocks, используя nmID из старого датафрейма
        old_df_without_desc['stocks'] = old_df_without_desc['nmID'].map(stocks_mapping)

        # Заполняем NaN значения нулями
        old_df_without_desc['stocks'] = old_df_without_desc['stocks'].fillna(0)

        # Преобразуем stocks в целые числа
        old_df_without_desc['stocks'] = old_df_without_desc['stocks'].astype(int)

        # Сортируем по убыванию stocks
        sorted_df = old_df_without_desc.sort_values(by='stocks', ascending=False)

        # Возвращаем строку с описаниями обратно
        final_df = pd.concat([descriptions, sorted_df], ignore_index=True)

        # Подсчитываем статистику обновления
        updated_count = int(old_df_without_desc['nmID'].isin(new_df['nmId']).sum())
        not_updated_count = int(len(old_df_without_desc) - updated_count)

        logger.info(
            "Успешно обновлены данные о стоках",
            всего_строк=int(len(final_df) - 1),
            обновлено_строк=updated_count,
            не_обновлено_строк=not_updated_count
        )
        if 'image' not in final_df.columns:
            # Создаем колонку image и заполняем первую строку
            final_df['image'] = ''
            final_df.at[0, 'image'] = 'Изображение товара'
        return final_df

    except Exception as e:
        logger.error(
            "Ошибка при обновлении данных о стоках",
            ошибка=str(e),
            тип_ошибки=type(e).__name__
        )
        raise


async def clean_numeric_column(old_df: pd.DataFrame,
                               column_name: str,
                               username: str,
                               marketname: str,
                               logger) -> pd.DataFrame:
    """
    Асинхронно удаляет строки с нечисловыми значениями в указанной колонке.
    Принимает только целые числа.

    Args:
        old_df (pd.DataFrame): Исходный DataFrame
        column_name (str): Имя колонки для проверки
        username (str): Имя пользователя для логирования
        marketname (str): Название маркетплейса для логирования
        logger: Объект логгера

    Returns:
        pd.DataFrame: Очищенный DataFrame
    """
    try:
        bound_logger = logger.bind(
            username=username,
            marketname=marketname
        )

        bound_logger.info(
            "Начало очистки нечисловых значений",
            всего_строк=int(len(old_df)),
            колонка=column_name
        )

        # Проверяем наличие колонки
        if column_name not in old_df.columns:
            bound_logger.error(
                "Указанная колонка не найдена в датафрейме",
                колонка=column_name
            )
            raise ValueError(f"Колонка {column_name} не найдена")

        # Сохраняем первую строку с описаниями
        descriptions = old_df.iloc[0:1].copy()

        # Удаляем первую строку из датафрейма для обработки
        df_without_desc = old_df.iloc[1:].copy()

        # Функция проверки на целое число
        def is_integer(x):
            if pd.isna(x) or x == '':
                return False
            try:
                # Проверяем, что значение можно преобразовать в целое число
                value = str(x).replace(',', '').replace('.', '')
                int(value)
                return True
            except (ValueError, TypeError):
                return False

        # Создаем маску для целых чисел
        numeric_mask = df_without_desc[column_name].apply(is_integer)

        # Сохраняем проблемные значения для логирования
        problematic_rows = df_without_desc[~numeric_mask]

        if not problematic_rows.empty:
            bound_logger.warning(
                "Найдены нецелые или нечисловые значения",
                количество=int(len(problematic_rows)),
                примеры_значений=problematic_rows[column_name].head().tolist()
            )

        # Фильтруем датафрейм
        cleaned_df = df_without_desc[numeric_mask].copy()

        # Преобразуем значения в целые числа и затем в строки
        cleaned_df[column_name] = cleaned_df[column_name].apply(
            lambda x: str(int(str(x).replace(',', '').replace('.', '')))
        )

        # Сортируем по указанной колонке
        sorted_df = cleaned_df.sort_values(
            by=column_name,
            ascending=False,
            key=lambda x: x.astype(int)
        )

        # Возвращаем строку с описаниями обратно
        final_df = pd.concat([descriptions, sorted_df], ignore_index=True)

        # Подсчитываем статистику
        removed_count = int(len(df_without_desc) - len(cleaned_df))

        bound_logger.info(
            "Успешно выполнена очистка нечисловых значений",
            всего_строк=int(len(final_df) - 1),
            удалено_строк=removed_count,
            осталось_строк=int(len(cleaned_df))
        )

        return final_df

    except Exception as e:
        bound_logger.error(
            "Ошибка при очистке нечисловых значений",
            ошибка=str(e),
            тип_ошибки=type(e).__name__,
            колонка=column_name
        )
        raise


async def add_image_formulas(old_df: pd.DataFrame,
                             username: str,
                             marketname: str,
                             logger) -> pd.DataFrame:
    """
    Асинхронно добавляет формулы изображений для значений nmID.

    Args:
        old_df (pd.DataFrame): Исходный DataFrame
        username (str): Имя пользователя для логирования
        marketname (str): Название маркетплейса для логирования
        logger: Объект логгера

    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой image
    """
    try:
        bound_logger = logger.bind(
            username=username,
            marketname=marketname
        )

        bound_logger.info(
            "Начало добавления формул изображений",
            всего_строк=int(len(old_df)),
            колонка='nmID'
        )

        # Проверяем наличие колонки nmID
        if 'nmID' not in old_df.columns:
            bound_logger.error(
                "Колонка nmID не найдена в датафрейме"
            )
            raise ValueError("Колонка nmID не найдена")

            # Создаем копию датафрейма и сохраняем исходный индекс
        df = old_df.copy()
        original_index = df.index.copy()

        def create_image_formula(nm_id: int) -> str:
            try:
                # Получаем первые 6 цифр для part и 4 для vol
                part = str(nm_id)[:6]
                vol = str(nm_id)[:4]
                return f'=IMAGE("https://basket-18.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/tm/1.webp"; 3)'
            except Exception as e:
                bound_logger.warning(
                    "Ошибка при создании формулы",
                    nmID=nm_id,
                    ошибка=str(e)
                )
                return ""

                # Добавляем колонку image

        df['image'] = None

        # Сохраняем первую строку для описания
        first_row_index = original_index[0]
        df.loc[first_row_index, 'image'] = 'Изображения товаров'

        # Создаем формулы для остальных строк
        with ThreadPoolExecutor() as executor:
            remaining_indices = original_index[1:]
            nm_ids = df.loc[remaining_indices, 'nmID']

            # Проверяем на некорректные значения
            invalid_ids = nm_ids[~nm_ids.astype(str).str.match(r'^\d+$')]
            if not invalid_ids.empty:
                bound_logger.warning(
                    "Обнаружены некорректные значения nmID",
                    количество=len(invalid_ids),
                    примеры=invalid_ids.head().tolist()
                )

                # Асинхронно применяем формулу
            formulas = list(executor.map(create_image_formula, nm_ids))
            df.loc[remaining_indices, 'image'] = formulas

            # Проверяем пустые формулы
        empty_formulas = df[df['image'] == ''].index[1:]
        if len(empty_formulas) > 0:
            bound_logger.warning(
                "Обнаружены пустые формулы",
                количество=len(empty_formulas),
                строки=empty_formulas.tolist()
            )

            # Убеждаемся, что порядок строк соответствует исходному
        final_df = df.loc[original_index]

        bound_logger.info(
            "Успешно добавлены формулы изображений",
            всего_строк=int(len(final_df) - 1),
            добавлено_формул=int(len(remaining_indices)),
            пустых_формул=int(len(empty_formulas))
        )

        return final_df

    except Exception as e:
        bound_logger.error(
            "Ошибка при добавлении формул изображений",
            ошибка=str(e),
            тип_ошибки=type(e).__name__
        )
        raise

async def main():
    """
    Основная функция для асинхронного выполнения всего процесса
    """
    try:
        # Асинхронно читаем CSV файлы
        logger.info("Начало чтения CSV файлов")
        df1, df2 = await asyncio.gather(
            read_csv_async('final.csv'),
            read_csv_async('wb_stocks.csv')
        )

        # Обновляем данные
        updated_df = await update_stocks_data(df1, df2, logger)
        print(updated_df.head())
        print(updated_df['stocks'])

        # Асинхронно записываем результат
        logger.info("Сохранение результатов в CSV файл")
        await write_csv_async(updated_df, 'result.csv')

        logger.info("Процесс успешно завершен")

    except Exception as e:
        logger.error(
            "Ошибка в процессе обработки",
            ошибка=str(e),
            тип_ошибки=type(e).__name__
        )
        raise



if __name__ == "__main__":
    asyncio.run(main())


