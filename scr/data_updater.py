
import asyncio
import pandas as pd
import structlog
from pathlib import Path
import sqlite3
from datetime import datetime
import numpy as np
from .logger import logger

ID_COL = 'id'
PRODUCT_ID_COL = 'product_id'
PRICE_COL = 't_price'
OLD_PRICE_COL = 'price'
PRIM_COL = 'prim'


async def update_price(df, id_col=ID_COL, product_id_col=PRODUCT_ID_COL, price_col=PRICE_COL,
                       old_price_col=OLD_PRICE_COL, prim_col=PRIM_COL, sqlite_db_name='SQLITE_DB_NAME',
                       price_change_log_table='price_change_log', old_disc_in_base_col=None, old_disc_manual_col=None):
    """Определяет необходимость обновления цен и скидок, записывает информацию об изменениях в
    таблицу price_change_log и обновляет цены в DataFrame"""

    df = df.replace('', np.nan).fillna(value='Нет Значения')
    df = df.iloc[1:]  # Пропускаем первую строку (заголовки)
    changes = []
    updated_df = df.copy()

    try:
        conn = sqlite3.connect(sqlite_db_name)
        c = conn.cursor()

        c.execute(f'''CREATE TABLE IF NOT EXISTS '{price_change_log_table}'  
                     (timestamp TEXT, id TEXT, product_id TEXT, old_price REAL, new_price REAL, old_discount REAL, new_discount REAL, prim TEXT, change_applied INTEGER)''')

        for _, row in df.iterrows():
            change_info = None
            price_changed = False
            discount_changed = False

            try:
                old_price = float(row[old_price_col]) if row[old_price_col] != 'Нет Значения' else None
                new_price = float(row[price_col]) if row[price_col] != 'Нет Значения' else None

                # Обработка скидок, если соответствующие колонки переданы
                if old_disc_in_base_col and old_disc_manual_col:
                    old_disc_in_base = float(row[old_disc_in_base_col]) if row[old_disc_in_base_col] != 'Нет Значения' else 0
                    old_disc_manual = float(row[old_disc_manual_col]) if row[old_disc_manual_col] != 'Нет Значения' else 0

                    if old_disc_in_base != old_disc_manual:
                        updated_df.at[_, old_disc_in_base_col] = old_disc_manual
                        discount_changed = True
                        change_info = row.to_dict()
                        change_info[old_disc_in_base_col] = old_disc_manual
                        change_info['old_discount'] = old_disc_in_base
                        change_info['new_discount'] = old_disc_manual
                        discount_prim = f"Обновлена скидка с {old_disc_in_base} на {old_disc_manual}"
                        updated_df.at[_, prim_col] = discount_prim
                        change_info['prim'] = discount_prim
                        logger.info("discount_updated", message=f"Обновлена скидка для товара", id=row[id_col], product_id=row[product_id_col], old_discount=old_disc_in_base, new_discount=old_disc_manual)

                        c.execute(
                            f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col],
                             row[old_price_col], row[old_price_col], old_disc_in_base, old_disc_manual, discount_prim, 1))

            except ValueError:
                logger.warning("invalid_price_or_discount", message="Некорректный формат цены или скидки", importance="high", id=row[id_col], product_id=row[product_id_col])
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], None, None, None,
                     None, "Ошибка формата данных", 0))
                continue

            if old_price is None or new_price is None:
                updated_df.at[_, prim_col] = "Отсутствует старая или новая цена"
                logger.info("missing_price", message="Отсутствует старая или новая цена", importance="high", id=row[id_col], product_id=row[product_id_col])
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                     new_price, None, None, "Отсутствует старая или новая цена", 0))
            elif old_price == 0:
                if new_price != 0:
                    updated_df.at[_, old_price_col] = new_price
                    updated_df.at[_, prim_col] = f"Старая цена была 0, обновлено на {new_price}"
                    price_changed = True
                    change_info = row.to_dict()
                    change_info[old_price_col] = new_price
                    change_info[prim_col] = f"Старая цена была 0, обновлено на {new_price}"
                    logger.info("price_updated_from_zero", message="Цена обновлена с нуля", id=row[id_col], product_id=row[product_id_col], new_price=new_price)
                    c.execute(
                        f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                         new_price, None, None, f"Старая цена была 0, обновлено на {new_price}", 1))
            elif new_price == 0:
                updated_df.at[_, prim_col] = "Новая цена стала 0, требуется проверка"
                logger.warning("new_price_zero", message="Новая цена стала нулевой, требуется проверка", importance="high", id=row[id_col], product_id=row[product_id_col])
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                     new_price, None, None, "Новая цена стала 0, требуется проверка", 0))
            elif abs(new_price - old_price) / old_price > 0.5:
                updated_df.at[_, prim_col] = f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена"
                logger.info("price_change_exceeds_limit", message="Изменение цены превышает допустимый предел", importance="high", id=row[id_col], product_id=row[product_id_col], old_price=old_price, new_price=new_price)
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                     new_price, None, None, f"Изменение цены с {old_price} на {new_price} превышает 50%, цена не изменена", 0))
            elif new_price != old_price:
                updated_df.at[_, old_price_col] = new_price  # Обновляем цену в DataFrame
                updated_df.at[_, prim_col] = f"Изменена цена с {old_price} на {new_price}"
                price_changed = True
                change_info = row.to_dict()
                change_info[price_col] = new_price
                change_info[prim_col] = f"Изменена цена с {old_price} на {new_price}"
                logger.info("price_updated", message="Цена обновлена", id=row[id_col], product_id=row[product_id_col], old_price=old_price, new_price=new_price)
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                     new_price, None, None, f"Изменена цена с {old_price} на {new_price}", 1))

            # Обновляем примечание, если изменились и цена, и скидка
            if price_changed and discount_changed:
                prim = f"Изменена цена с {old_price} на {new_price} и скидка с {old_disc_in_base} на {old_disc_manual}"
                updated_df.at[_, prim_col] = prim
                change_info[prim_col] = prim
                logger.info("price_and_discount_updated", message="Обновлены цена и скидка", id=row[id_col], product_id=row[product_id_col], old_price=old_price, new_price=new_price, old_discount=old_disc_in_base, new_discount=old_disc_manual)
                c.execute(
                    f"INSERT INTO '{price_change_log_table}' (timestamp, id, product_id, old_price, new_price, old_discount, new_discount, prim, change_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), row[id_col], row[product_id_col], old_price,
                     new_price, old_disc_in_base, old_disc_manual, prim, 1))

            if change_info and (price_changed or discount_changed):
                change_info['change_applied'] = True
                changes.append(change_info)

        conn.commit()
    except sqlite3.Error as e:
        logger.error("database_error", message="Ошибка базы данных", importance="high", error=str(e))
        return None, None

    conn.close()

    # Создаем новый DataFrame, содержащий только строки с измененными ценами или скидками
    price_changed_df = pd.DataFrame(changes)

    return updated_df, price_changed_df


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
        words = text.split()
        if not words:
            return "Ошибка применения цены, со стороны маркетплейса", None

            # Проверяем первое слово
        first_word = words[0]

        if first_word == "Изменена":
            # Обработка текста для случая "Изменена"
            remaining_text = ' '.join(words[2:]) if len(words) > 2 else ''
            prefix_text = "Ошибка от маркетплейса при попытке изменения цены"
            processed_text = f"{prefix_text} {remaining_text}" if remaining_text else prefix_text
        else:
            # Для всех остальных случаев
            processed_text = "Ошибка применения цены, со стороны маркетплейса"

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
    result_df = result_df.fillna('')

    return result_df

