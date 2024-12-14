import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина Ozon.

    Args:
        last_id (str): Идентификатор последнего товара для постраничного запроса.
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список товаров в магазине Ozon.

    Examples:
        >>> get_product_list("", "your_client_id", "your_seller_token")
        [{'offer_id': '12345', 'name': 'Товар 1'}, {'offer_id': '67890', 'name': 'Товар 2'}]

        >>> get_product_list("", "invalid_client_id", "invalid_seller_token")
        # Вызывает ошибку 401 Unauthorized
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает артикулы товаров магазина Ozon.

    Args:
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список артикулов товаров в магазине Ozon.

    Examples:
        >>> get_offer_ids("your_client_id", "your_seller_token")
        ['12345', '67890']

        >>> get_offer_ids("invalid_client_id", "invalid_seller_token")
        # Вызывает ошибку 401 Unauthorized
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров в магазине Ozon.

    Args:
        prices (list): Список новых цен для товаров.
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Ответ от API Ozon после обновления цен.

    Examples:
        >>> update_price([{"offer_id": "12345", "price": 5990}], "your_client_id", "your_seller_token")
        {'result': True}

        >>> update_price([], "your_client_id", "your_seller_token")
        # Вызывает ошибку 400 Bad Request из-за пустого списка цен
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров в магазине Ozon.

    Args:
        stocks (list): Список остатков товаров.
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Ответ от API Ozon после обновления остатков.

    Examples:
        >>> update_stocks([{"offer_id": "12345", "stock": 10}], "your_client_id", "your_seller_token")
        {'result': True}

        >>> update_stocks([], "your_client_id", "your_seller_token")
        # Вызывает ошибку 400 Bad Request из-за пустого списка остатков
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Загружает файл остатков с сайта Casio и обрабатывает его.

    Returns:
        list: Список остатков часов из загруженного файла.

    Examples:
        >>> download_stock()
        [{'Код': '12345', 'Количество': '>10'}, {'Код': '67890', 'Количество': '5'}]

        >>> download_stock()  # Если сайт недоступен или файл отсутствует
        # Вызывает ошибку 404 Not Found или другие ошибки сети
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список остатков на основе загруженных данных и артикулов.

    Args:
        watch_remnants (list): Список остатков часов из загруженного файла.
        offer_ids (list): Список артикулов товаров в магазине Ozon.

    Returns:
        list: Список остатков с артикулом и количеством для обновления в Ozon.

    Examples:
        >>> create_stocks([{"Код": "12345", "Количество": ">10"}], ["12345"])
        [{'offer_id': '12345', 'stock': 100}]

        >>> create_stocks([], ["12345"])  # Пустой список остатков
        []
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список новых цен на основе загруженных данных и артикулов.

    Args:
        watch_remnants (list): Список остатков часов из загруженного файла.
        offer_ids (list): Список артикулов товаров в магазине Ozon.

    Returns:
        list: Список объектов с новыми ценами для обновления в Ozon.

    Examples:
        >>> create_prices([{"Код": "12345", "Цена": "5'990.00 руб."}], ["12345"])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        'offer_id': '12345', 'old_price': '0', 'price': '5990'}]

        >>> create_prices([], ["12345"])  # Пустой список остатков
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует цену из строкового формата в числовой.

    Args:
        price (str): Цена в строковом формате (например, "5'990.00 руб.").
    Returns:
        str: Цена в числовом формате без символов и пробелов.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("")  # Пустая строка
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части по n элементов.

    Args:
        lst (list): Исходный список для разделения.
        n (int): Количество элементов в каждой части.

    Yields:
        list: Части списка по n элементов.

    Examples:
        >>> list(divide([1, 2, 3, 4], 2))
        [[1, 2], [3, 4]]

        >>> list(divide([], 2))  # Пустой список
        []
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает новые цены на товары в магазин Ozon.

    Args:
        watch_remnants (list): Список остатков часов из загруженного файла.
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список отправленных цен на товары.

    Examples:
        >>> await upload_prices([{"Код": "12345", "Цена": "5'990.00 руб."}],
        'your_client_id', 'your_seller_token')
        [{'result': True}]

        >>> await upload_prices([], 'your_client_id', 'your_seller_token')
        # Пустой список остатков
        # Вызывает ошибку из-за пустого списка цен
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает остатки товаров в магазин Ozon.

    Args:
        watch_remnants (list): Список остатков часов из загруженного файла.
        client_id (str): Идентификатор клиента для доступа к API Ozon.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        tuple: Кортеж из двух списков - непустых остатков и всех остатков.

    Examples:
        >>> await upload_stocks([{"Код": "12345", "Количество": ">10"}],
        'your_client_id', 'your_seller_token')
        ([{'offer_id': '12345', 'stock': 100}], [{'offer_id': '12345', 'stock': 100}])

        >>> await upload_stocks([], 'your_client_id', 'your_seller_token')
        # Пустой список остатков
        # Вызывает ошибку из-за пустого списка остатков
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция запуска скрипта.

    Загружает токены доступа, процесс обновления остатков и цен на товары в магазине Ozon.
    Обрабатывает ошибки при выполнении запросов к API.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
