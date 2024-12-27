import pandas as pd
import asyncio
import sys,os



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger
def transform_wb_data_sync(df: pd.DataFrame, client_id: str = "") -> pd.DataFrame:
    """
    Синхронная функция преобразования DataFrame.
    """
    descriptions = pd.DataFrame([{
        'Client-Id': 'Название магазина',
        'offer_id': 'Ваш SKU',
        'id': 'Название товара',
        'link': 'Ссылка на товар',
        'stocks': 'Общие непроданные остатки',
        'price': 'Цена в системе',
        'price_old': 'Базовая перечеркнутая цена в системе',
        't_price': 'Цена для применения',
        'discount_base': 'Цена до скидки.',
        'Flag': 'Тумблер применения цены Указать (True/TRUE/+) для обработки строки',
        'prim': 'Примечание(заполняется программой)',
        'image': 'Изображение товара'
    }])

    def try_convert_to_int(value):
        """
        Пытается преобразовать значение в целое число.
        Если значение пустое или None, возвращает 0.
        Если преобразование не удается, возвращает исходное значение.
        """
        # Проверяем пустые значения
        if pd.isna(value) or value == '' or value == 'Нет значения':
            return 0

        try:
            # Пробуем преобразовать в float (на случай если строка содержит точку),
            # а затем в int
            return int(float(str(value).replace(',', '.')))
        except (ValueError, TypeError):
            return value

    transformed_df = pd.DataFrame({
        'Client-Id': [client_id] * len(df),
        'offer_id': df['offer_id'].values,
        'id': df['name'].values,
        'link': ['Ваша ссылка'] * len(df),
        'stocks': ['Нет значения'] * len(df),
        'price': pd.Series(df['basic_price'].values).apply(try_convert_to_int),
        'price_old': pd.Series(df['basic_sale'].values).apply(try_convert_to_int),
        't_price': pd.Series(df['basic_price'].values).apply(try_convert_to_int),
        'discount_base': pd.Series(df['basic_sale'].values).apply(try_convert_to_int),
        'Flag': False,
        'prim': ['Нет значения'] * len(df),
        'image': df['pictures'].fillna('').str.split(',').str[0].apply(
            lambda x: f'=IMAGE("{x}"; 2)' if x else 'Нет значения')
    })
    final_df = pd.concat([descriptions, transformed_df], ignore_index=True)
    return final_df


# res_df = transform_wb_data_sync(pd.read_csv('data.csv'),'TestMarket')
# res_df.to_csv('first_stage.csv')

async def update_stocks_data(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Асинхронно обновляет данные о стоках из колонки stock_count для Яндекс.Маркета
    """
    try:
        logger.info(
            "Начало обновления данных о стоках",
            старых_строк=int(len(old_df)),
            новых_строк=int(len(new_df))
        )

        # Проверяем наличие нужной колонки
        if 'stock_count' not in new_df.columns:
            logger.error("Колонка stock_count не найдена во втором датафрейме")
            raise ValueError("Колонка stock_count не найдена")

        # Сохраняем первую строку с описаниями
        descriptions = old_df.iloc[0:1].copy()

        # Удаляем первую строку из старого датафрейма для обработки
        old_df_without_desc = old_df.iloc[1:].copy()

        # Создаем маппинг offer_id -> stock_count из нового датафрейма
        stocks_mapping = dict(zip(
            new_df['offer_id'],
            (new_df['stock_count'] / 2).round().astype(int)
        ))

        # Выводим пример маппинга для проверки
        logger.info(
            "Пример данных для обновления",
            пример_маппинга=dict(list(stocks_mapping.items())[:3])
        )

        # Обновляем значения в колонке stocks, используя offer_id из старого датафрейма
        old_df_without_desc['stocks'] = old_df_without_desc['offer_id'].map(stocks_mapping)

        # Заполняем NaN значения нулями
        old_df_without_desc['stocks'] = old_df_without_desc['stocks'].fillna(0)

        # Преобразуем stocks в целые числа
        old_df_without_desc['stocks'] = old_df_without_desc['stocks'].astype(int)

        # Сортируем по убыванию stocks
        sorted_df = old_df_without_desc.sort_values(by='stocks', ascending=False)

        # Возвращаем строку с описаниями обратно
        final_df = pd.concat([descriptions, sorted_df], ignore_index=True)

        # Подсчитываем статистику обновления
        updated_count = int(old_df_without_desc['offer_id'].isin(new_df['offer_id']).sum())
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


async def main():

    try:
        updated_df = await update_stocks_data(
            old_df=pd.read_csv('first_stage.csv'),
            new_df=pd.read_csv('stocks_test_shop_20241205_092839.csv'),
        )

        # Выводим результат
        print("\nОбновленный датафрейм:")
        print(updated_df)

        # Можно также сохранить результат в файл
        updated_df.to_csv('updated_stocks.csv', index=False)

    except Exception as e:
        print(f"Произошла ошибка: {e}")

    # Запускаем асинхронную функцию


if __name__ == "__main__":
    asyncio.run(main())
