import asyncio
import pandas as pd
import structlog
from pathlib import Path
import sqlite3
from datetime import datetime
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union
from .logger import logger

pd.set_option('future.no_silent_downcasting', True)


async def update_prices(
        df: pd.DataFrame,
        columns_dict: Dict[str, str],
        sqlite_db_name: str = 'SQLITE_DB_NAME',
        price_change_log_table: str = 'price_change_log',
        marketplace: Optional[str] = None,
        username: Optional[str] = None
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Обновляет цены, скидки и минимальные цены в датафрейме, логирует изменения в БД

    Args:
        df: Исходный датафрейм с данными
        columns_dict: Словарь соответствия названий колонок
        sqlite_db_name: Имя файла базы данных SQLite
        price_change_log_table: Имя таблицы для логирования изменений
        marketplace: Название торговой площадки
        username: Имя пользователя

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
            - Обновленный датафрейм
            - Датафрейм с информацией об изменениях (None если изменений не было)
    """

    def validate_price(value: Any) -> Optional[float]:
        """Проверяет и преобразует значение цены"""
        try:
            return float(value) if value != 'Нет Значения' else None
        except (ValueError, TypeError):
            return None

    def check_price_change(old_price: Optional[float], new_price: Optional[float]) -> Tuple[bool, str]:
        """Проверяет допустимость изменения цены"""
        if old_price is None or new_price is None:
            return False, "Отсутствует старая или новая цена"
        if old_price == 0 and new_price != 0:
            return True, f"Старая цена была 0, обновлено на {new_price}"
        if new_price == 0:
            return False, "Новая цена стала 0, требуется проверка"
        if abs(new_price - old_price) / old_price > 0.5:
            return False, f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"
        return True, ""

    def validate_discounts_and_prices(row: pd.Series) -> List[str]:
        """Проверяет корректность скидок и минимальных цен"""
        errors = []

        # Проверка скидок
        if all(key in columns_dict for key in ['old_disc_in_base_col', 'old_disc_manual_col']):
            old_disc_in_base = validate_price(row[columns_dict['old_disc_in_base_col']])
            old_disc_manual = validate_price(row[columns_dict['old_disc_manual_col']])

            if old_disc_in_base is None:
                errors.append("Некорректное значение базовой скидки")
            if old_disc_manual is None:
                errors.append("Некорректное значение ручной скидки")

                # Проверка минимальных цен
        if all(key in columns_dict for key in ['min_price_base', 'min_price']):
            min_price_base = validate_price(row[columns_dict['min_price_base']])
            min_price_new = validate_price(row[columns_dict['min_price']])

            if min_price_base is None:
                errors.append("Некорректное значение базовой минимальной цены")
            if min_price_new is None:
                errors.append("Некорректное значение новой минимальной цены")

        return errors

    def create_log_entry(
            timestamp: str,
            row: pd.Series,
            old_price: Optional[float] = None,
            new_price: Optional[float] = None,
            old_discount: Optional[float] = None,
            new_discount: Optional[float] = None,
            old_min_price: Optional[float] = None,
            new_min_price: Optional[float] = None,
            prim: str = "",
            change_applied: int = 0
    ) -> Tuple:
        """Создает запись для лога изменений"""
        return (
            timestamp,
            row[columns_dict['id_col']],
            row[columns_dict['product_id_col']],
            old_price, new_price,
            old_discount, new_discount,
            old_min_price, new_min_price,
            prim, change_applied
        )

        # Подготовка датафрейма

    df = df.replace('', np.nan).fillna(value='Нет Значения')
    df = df.iloc[1:]  # Пропускаем первую строку
    changes = []  # Список для хранения информации об изменениях
    updated_df = df.copy()

    try:
        with sqlite3.connect(sqlite_db_name) as conn:
            c = conn.cursor()

            # Создание таблицы для логирования если не существует
            c.execute(f'''CREATE TABLE IF NOT EXISTS '{price_change_log_table}'  
                         (timestamp TEXT, id TEXT, product_id TEXT, old_price REAL, new_price REAL,  
                         old_discount REAL, new_discount REAL, old_min_price REAL, new_min_price REAL,  
                         prim TEXT, change_applied INTEGER)''')

            for idx, row in df.iterrows():
                try:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

                    # Проверка флага обработки
                    process_row = str(row.get('Flag', '')).upper() in ['TRUE', '+']

                    if not process_row:
                        # Если строка не должна обрабатываться
                        updated_prim = f"Пропущено {timestamp}"
                        updated_df.at[idx, columns_dict['prim_col']] = updated_prim

                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            create_log_entry(timestamp, row, prim=updated_prim)
                        )
                        continue

                        # Валидация цен
                    old_price = validate_price(row[columns_dict['old_price_col']])
                    new_price = validate_price(row[columns_dict['price_col']])

                    # Проверка ошибок валидации
                    validation_errors = validate_discounts_and_prices(row)
                    if validation_errors:
                        error_message = f"{'; '.join(validation_errors)} ({timestamp})"
                        updated_df.at[idx, columns_dict['prim_col']] = error_message

                        logger.info("validation_failed",
                                    message=error_message,
                                    importance="high",
                                    id=row[columns_dict['id_col']],
                                    product_id=row[columns_dict['product_id_col']],
                                    marketplace=marketplace,
                                    username=username)

                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            create_log_entry(timestamp, row, prim=error_message)
                        )
                        continue

                        # Проверка изменения цены
                    is_valid_change, message = check_price_change(old_price, new_price)
                    if not is_valid_change:
                        updated_message = f"{message} ({timestamp})"
                        updated_df.at[idx, columns_dict['prim_col']] = updated_message

                        logger.info("price_validation_failed",
                                    message=message,
                                    importance="high",
                                    id=row[columns_dict['id_col']],
                                    product_id=row[columns_dict['product_id_col']],
                                    marketplace=marketplace,
                                    username=username)

                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            create_log_entry(timestamp, row, old_price, new_price, prim=updated_message)
                        )
                        continue

                        # Обработка изменений
                    change_info = None
                    price_changed = discount_changed = min_price_changed = False

                    # Проверка изменения цены
                    if new_price != old_price:
                        updated_df.at[idx, columns_dict['old_price_col']] = new_price
                        price_changed = True
                        change_info = row.to_dict()
                        change_info[columns_dict['price_col']] = new_price

                        # Проверка изменения скидок
                    if all(key in columns_dict for key in ['old_disc_in_base_col', 'old_disc_manual_col']):
                        old_disc_in_base = validate_price(row[columns_dict['old_disc_in_base_col']])
                        old_disc_manual = validate_price(row[columns_dict['old_disc_manual_col']])

                        if old_disc_in_base != old_disc_manual:
                            updated_df.at[idx, columns_dict['old_disc_in_base_col']] = old_disc_manual
                            discount_changed = True
                            if not change_info:
                                change_info = row.to_dict()
                            change_info.update({
                                columns_dict['old_disc_in_base_col']: old_disc_manual,
                                'old_discount': old_disc_in_base,
                                'new_discount': old_disc_manual
                            })

                            # Проверка изменения минимальных цен
                    if all(key in columns_dict for key in ['min_price_base', 'min_price']):
                        min_price_base = validate_price(row[columns_dict['min_price_base']])
                        min_price_new = validate_price(row[columns_dict['min_price']])

                        if min_price_base != min_price_new:
                            updated_df.at[idx, columns_dict['min_price_base']] = min_price_new
                            min_price_changed = True
                            if not change_info:
                                change_info = row.to_dict()
                            change_info.update({
                                columns_dict['min_price_base']: min_price_new,
                                'old_min_price': min_price_base,
                                'new_min_price': min_price_new
                            })

                            # Формирование итогового примечания при наличии изменений
                    if any([price_changed, discount_changed, min_price_changed]):
                        prim_parts = []
                        if price_changed:
                            prim_parts.append(f"цена с {old_price} на {new_price}")
                        if discount_changed:
                            prim_parts.append(f"скидка с {old_disc_in_base} на {old_disc_manual}")
                        if min_price_changed:
                            prim_parts.append(f"минимальная цена с {min_price_base} на {min_price_new}")

                        prim = f"Изменены: {' и '.join(prim_parts)} ({timestamp})"
                        updated_df.at[idx, columns_dict['prim_col']] = prim

                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            create_log_entry(
                                timestamp, row,
                                old_price if price_changed else None,
                                new_price if price_changed else None,
                                old_disc_in_base if discount_changed else None,
                                old_disc_manual if discount_changed else None,
                                min_price_base if min_price_changed else None,
                                min_price_new if min_price_changed else None,
                                prim, 1
                            )
                        )

                        change_info['change_applied'] = True
                        change_info[columns_dict['prim_col']] = prim
                        changes.append(change_info)

                        # Изменение Flag на False после успешной обработки
                        updated_df.at[idx, 'Flag'] = False

                except ValueError as e:
                    error_message = f"Некорректный формат данных ({timestamp})"
                    updated_df.at[idx, columns_dict['prim_col']] = error_message

                    logger.info("invalid_value",
                                message=error_message,
                                importance="high",
                                id=row[columns_dict['id_col']],
                                product_id=row[columns_dict['product_id_col']],
                                error=str(e),
                                marketplace=marketplace,
                                username=username)

                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        create_log_entry(timestamp, row, prim=error_message)
                    )

            conn.commit()

    except sqlite3.Error as e:
        logger.error("database_error",
                     message="Ошибка базы данных",
                     importance="high",
                     error=str(e),
                     marketplace=marketplace,
                     username=username)
        return None, None

    price_changed_df = pd.DataFrame(changes) if changes else None
    return updated_df, price_changed_df


# Пример словаря колонок (полный вариант)
COLUMNS_FULL = {
    'id': 'ID',
    'product_id': 'PRODUCT_ID',
    'price': 'NEW_PRICE',
    'old_price': 'OLD_PRICE',
    'old_discount': 'OLD_DISCOUNT',
    'new_discount': 'NEW_DISCOUNT',
    'min_price_base': 'OLD_MIN_PRICE',
    'min_price': 'NEW_MIN_PRICE',
    'prim': 'ПРИМЕЧАНИЕ'
}

# Пример словаря колонок (минимальный вариант)
COLUMNS_MINIMAL = {
    'id': 'ID',
    'product_id': 'PRODUCT_ID',
    'price': 'NEW_PRICE',
    'old_price': 'OLD_PRICE',
    'prim': 'ПРИМЕЧАНИЕ'
}


async def update_and_merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, key_column: str) -> pd.DataFrame:
    """
    Асинхронная версия функции обновления и объединения датафреймов.
    Обрабатывает текст в колонке 'prim' и извлекает цену для колонки 'price'.
    """

    async def process_row(text: str) -> tuple[str, float | None]:
        """
        Асинхронная обработка строки: обработка текста и извлечение цены.

        Returns:
            tuple: (обработанный_текст, цена или None если цену извлечь не удалось)
        """
        if not text or text.isspace():
            return ("Ошибка проведения операции со стороны маркетплейса (пустая строка). "  
                   "Параметры не применены, но изменены в таблице во избежание повторной обработки данной строки."), None

        words = text.split()
        first_word = words[0] if words else ""

        if first_word == "Изменена":
            # Обработка текста для случая "Изменена"
            remaining_text = ' '.join(words[2:]) if len(words) > 2 else ''
            prefix_text = "Ошибка от маркетплейса при попытке изменения цены"
            processed_text = f"{prefix_text} {remaining_text}" if remaining_text else prefix_text
        else:
            # Для всех остальных случаев
            processed_text = (f"Ошибка проведения следующей операции со стороны маркетплейса ({text}). "  
                            "Параметры не применены, но изменены в таблице во избежание повторной обработки данной строки.")

        # Извлечение цены (4-е слово в исходном тексте)
        price = None
        if len(words) >= 4:
            try:
                # Пытаемся преобразовать 4-е слово в число
                price_str = words[3].replace(',', '.')  # Заменяем запятую на точку, если она есть
                price = float(price_str)
            except (ValueError, IndexError):
                price = None

        return processed_text, price

    # Создаем копии датафреймов
    df2_modified = df2.copy()
    result_df = df1.copy()

    # Обрабатываем каждую строку в df2
    tasks = []
    for idx, row in df2_modified.iterrows():
        task = process_row(row['prim'])
        tasks.append(task)

    # Получаем обработанные данные
    processed_data = await asyncio.gather(*tasks)

    # Разделяем обработанные тексты и цены
    processed_texts, prices = zip(*processed_data)

    # Создаем словари для обновления значений
    update_dict_prim = dict(zip(df2_modified[key_column], processed_texts))
    update_dict_price = dict(zip(df2_modified[key_column], prices))

    # Обновляем значения в result_df
    for key in update_dict_prim:
        mask = result_df[key_column] == key
        # Обновляем prim
        result_df.loc[mask, 'prim'] = update_dict_prim[key]
        # Обновляем price только если удалось извлечь цену
        if update_dict_price[key] is not None:
            result_df.loc[mask, 'price'] = update_dict_price[key]

    # Заполняем пустые значения
    result_df = result_df.fillna('').infer_objects(copy=False)

    return result_df