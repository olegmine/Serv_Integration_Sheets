import asyncio
import pandas as pd
import structlog
from pathlib import Path
import sqlite3
from datetime import datetime
import numpy as np
from .logger import logger


async def update_prices(df, columns_dict, sqlite_db_name='SQLITE_DB_NAME',
                        price_change_log_table='price_change_log', marketplace=None, username=None):
    """
    Определяет необходимость обновления цен, скидок и минимальных цен, записывает информацию
    об изменениях в таблицу price_change_log и обновляет значения в DataFrame

    Parameters:
    -----------
    df : pandas.DataFrame
        Исходный DataFrame с данными
    columns_dict : dict
        Словарь с названиями колонок:
        {
            'id_col': 'id',
            'product_id_col': 'product_id',
            'price_col': 'price',
            'old_price_col': 'old_price',
            'prim_col': 'prim',
            'old_disc_in_base_col': 'old_disc_in_base',
            'old_disc_manual_col': 'old_disc_manual',
            'min_price_base': 'min_price_base',
            'min_price': 'min_price'
        }
    """

    df = df.replace('', np.nan).fillna(value='Нет Значения')
    df = df.iloc[1:]  # Пропускаем первую строку (заголовки)
    changes = []
    updated_df = df.copy()

    try:
        conn = sqlite3.connect(sqlite_db_name)
        c = conn.cursor()

        c.execute(f'''CREATE TABLE IF NOT EXISTS '{price_change_log_table}'  
                     (timestamp TEXT, id TEXT, product_id TEXT, old_price REAL, new_price REAL,   
                     old_discount REAL, new_discount REAL, old_min_price REAL, new_min_price REAL,   
                     prim TEXT, change_applied INTEGER)''')

        for _, row in df.iterrows():
            change_info = None
            price_changed = False
            discount_changed = False
            min_price_changed = False

            try:
                old_price = float(row[columns_dict['old_price_col']]) if row[columns_dict['old_price_col']] != 'Нет Значения' else None
                new_price = float(row[columns_dict['price_col']]) if row[columns_dict['price_col']] != 'Нет Значения' else None

                # Проверка наличия цен
                if old_price is None or new_price is None:
                    prim = "Отсутствует старая или новая цена"
                    updated_df.at[_, columns_dict['prim_col']] = prim
                    logger.info("missing_price",
                              message=prim,
                              importance="high",
                              id=row[columns_dict['id_col']],
                              product_id=row[columns_dict['product_id_col']],
                              marketplace=marketplace,
                              username=username)
                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         row[columns_dict['id_col']],
                         row[columns_dict['product_id_col']],
                         old_price, new_price, None, None, None, None, prim, 0))
                    continue

                # Проверка нулевой старой цены
                if old_price == 0:
                    if new_price != 0:
                        updated_df.at[_, columns_dict['old_price_col']] = new_price
                        prim = f"Старая цена была 0, обновлено на {new_price}"
                        updated_df.at[_, columns_dict['prim_col']] = prim
                        price_changed = True
                        change_info = row.to_dict()
                        change_info[columns_dict['old_price_col']] = new_price
                        change_info[columns_dict['prim_col']] = prim
                        logger.info("price_updated_from_zero",
                                  message="Цена обновлена с нуля",
                                  id=row[columns_dict['id_col']],
                                  product_id=row[columns_dict['product_id_col']],
                                  new_price=new_price,
                                  marketplace=marketplace,
                                  username=username)
                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                             row[columns_dict['id_col']],
                             row[columns_dict['product_id_col']],
                             old_price, new_price, None, None, None, None, prim, 1))
                    continue

                # Проверка нулевой новой цены
                if new_price == 0:
                    prim = "Новая цена стала 0, требуется проверка"
                    updated_df.at[_, columns_dict['prim_col']] = prim
                    logger.warning("new_price_zero",
                                 message=prim,
                                 importance="high",
                                 id=row[columns_dict['id_col']],
                                 product_id=row[columns_dict['product_id_col']],
                                 marketplace=marketplace,
                                 username=username)
                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         row[columns_dict['id_col']],
                         row[columns_dict['product_id_col']],
                         old_price, new_price, None, None, None, None, prim, 0))
                    continue

                # Проверка изменения цены более чем на 50%
                if abs(new_price - old_price) / old_price > 0.5:
                    prim = f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"
                    updated_df.at[_, columns_dict['prim_col']] = prim
                    logger.info("price_change_exceeds_limit",
                              message="Изменение цены превышает допустимый предел",
                              importance="high",
                              id=row[columns_dict['id_col']],
                              product_id=row[columns_dict['product_id_col']],
                              old_price=old_price,
                              new_price=new_price,
                              marketplace=marketplace,
                              username=username)
                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         row[columns_dict['id_col']],
                         row[columns_dict['product_id_col']],
                         old_price, new_price, None, None, None, None, prim, 0))
                    continue

                # Обновление цены при прохождении всех проверок
                if new_price != old_price:
                    updated_df.at[_, columns_dict['old_price_col']] = new_price
                    price_changed = True
                    if not change_info:
                        change_info = row.to_dict()
                    change_info[columns_dict['price_col']] = new_price

                # Обработка скидок
                if 'old_disc_in_base_col' in columns_dict and 'old_disc_manual_col' in columns_dict:
                    old_disc_in_base = float(row[columns_dict['old_disc_in_base_col']]) if row[columns_dict['old_disc_in_base_col']] != 'Нет Значения' else 0
                    old_disc_manual = float(row[columns_dict['old_disc_manual_col']]) if row[columns_dict['old_disc_manual_col']] != 'Нет Значения' else 0

                    if old_disc_in_base != old_disc_manual:
                        updated_df.at[_, columns_dict['old_disc_in_base_col']] = old_disc_manual
                        discount_changed = True
                        if not change_info:
                            change_info = row.to_dict()
                        change_info[columns_dict['old_disc_in_base_col']] = old_disc_manual
                        change_info['old_discount'] = old_disc_in_base
                        change_info['new_discount'] = old_disc_manual

                # Обработка минимальных цен
                if 'min_price_base' in columns_dict and 'min_price' in columns_dict:
                    min_price_base = float(row[columns_dict['min_price_base']]) if row[columns_dict['min_price_base']] != 'Нет Значения' else 0
                    min_price_new = float(row[columns_dict['min_price']]) if row[columns_dict['min_price']] != 'Нет Значения' else 0

                    if min_price_base != min_price_new:
                        updated_df.at[_, columns_dict['min_price_base']] = min_price_new
                        min_price_changed = True
                        if not change_info:
                            change_info = row.to_dict()
                        change_info[columns_dict['min_price_base']] = min_price_new
                        change_info['old_min_price'] = min_price_base
                        change_info['new_min_price'] = min_price_new

                # Формирование итогового примечания
                if price_changed or discount_changed or min_price_changed:
                    prim_parts = []
                    if price_changed:
                        prim_parts.append(f"цена с {old_price} на {new_price}")
                    if discount_changed:
                        prim_parts.append(f"скидка с {old_disc_in_base} на {old_disc_manual}")
                    if min_price_changed:
                        prim_parts.append(f"минимальная цена с {min_price_base} на {min_price_new}")

                    prim = "Изменены: " + " и ".join(prim_parts)
                    updated_df.at[_, columns_dict['prim_col']] = prim
                    change_info[columns_dict['prim_col']] = prim

                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         row[columns_dict['id_col']],
                         row[columns_dict['product_id_col']],
                         old_price if price_changed else None,
                         new_price if price_changed else None,
                         old_disc_in_base if discount_changed else None,
                         old_disc_manual if discount_changed else None,
                         min_price_base if min_price_changed else None,
                         min_price_new if min_price_changed else None,
                         prim, 1))

                    change_info['change_applied'] = True
                    changes.append(change_info)

            except ValueError:
                logger.warning("invalid_value",
                             message="Некорректный формат данных",
                             importance="high",
                             id=row[columns_dict['id_col']],
                             product_id=row[columns_dict['product_id_col']],
                             marketplace=marketplace,
                             username=username)
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, old_min_price, new_min_price, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     row[columns_dict['id_col']],
                     row[columns_dict['product_id_col']],
                     None, None, None, None, None, None,
                     "Ошибка формата данных", 0))

        conn.commit()
    except sqlite3.Error as e:
        logger.error("database_error",
                    message="Ошибка базы данных",
                    importance="high",
                    error=str(e),
                    marketplace=marketplace,
                    username=username)
        return None, None

    conn.close()

    price_changed_df = pd.DataFrame(changes)
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