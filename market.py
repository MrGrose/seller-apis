import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из Яндекс.Маркета для заданной кампании.

    Args:
        page (str): Токен страницы для получения следующей страницы результатов.
        campaign_id (str): Идентификатор кампании для получения товаров.
        access_token (str): Токен доступа для авторизации.

    Returns:
        list: Список товарных предложений для указанной кампании.

    Examples:
        >>> get_product_list("", "12345", "your_access_token")
        [{'offer': {'shopSku': 'SKU1', ...}}, {'offer': {'shopSku': 'SKU2', ...}}]

        >>> get_product_list("invalid_page_token", "12345", "your_access_token")
        # Вызывает ошибку из-за неверного токена страницы.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет информацию о запасах товаров в заданной кампании.

    Args:
        stocks (list): Список идентификаторов товаров для обновления запасов.
        campaign_id (str): Идентификатор кампании для обновления запасов.
        access_token (str): Токен доступа для авторизации.

    Returns:
        dict: Ответ от API после обновления запасов.

    Examples:
        >>> update_stocks(["SKU1", "SKU2"], "12345", "your_access_token")
        {'result': True}

        >>> update_stocks([], "12345", "your_access_token")
        # Вызывает ошибку из-за пустого списка запасов.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены товаров в заданной кампании.

    Args:
        prices (list): Список ценовых предложений для обновления.
        campaign_id (str): Идентификатор кампании для обновления цен.
        access_token (str): Токен доступа для авторизации.

    Returns:
        dict: Ответ от API после обновления цен.

    Examples:
        >>> update_price([{"id": "SKU1", "price": {"value": 1000}}], "12345", "your_access_token")
        {'result': True}

        >>> update_price([], "12345", "your_access_token")
        # Вызывает ошибку из-за пустого списка цен.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает идентификаторы (артикулы) товаров в заданной кампании.

    Args:
        campaign_id (str): Идентификатор кампании для получения артикулов.
        market_token (str): Токен доступа для авторизации.

    Returns:
        list: Список уникальных идентификаторов товаров в указанной кампании.

    Examples:
        >>> get_offer_ids("12345", "your_market_token")
        ['SKU1', 'SKU2']

        >>> get_offer_ids("invalid_campaign_id", "your_market_token")
        # Вызывает ошибку из-за неверного идентификатора компании.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список запасов на основе доступных остатков и артикулов.

    Args:
        watch_remnants (list): Список доступных остатков с их количествами.
        offer_ids (list): Список идентификаторов товаров в Яндекс.Маркете.
        warehouse_id (str): Идентификатор склада, где находятся запасы.

    Returns:
        list: Список запасов в формате, подходящем для API Яндекс.Маркета.

    Examples:
        >>> create_stocks([{"Код": "12345", "Количество": ">10"}], ["12345"], "warehouse_1")
        [{'sku': '12345', 'warehouseId': 'warehouse_1', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2023-12-14T12:00:00Z'}]}]

        >>> create_stocks([], ["12345"], "warehouse_1")
        []
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список цен на основе остатков и артикулов.

    Args:
        watch_remnants (list): Список товаров с их ценами.
        offer_ids (list): Список идентификаторов товаров в Яндекс.Маркете.

    Returns:
        list: Список ценовых предложений в формате, подходящем для API Яндекс.Маркета.

    Examples:
        >>> create_prices([{"Код": "12345", "Цена": "5'990.00 руб."}], ["12345"])
        [{'id': '12345', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

        >>> create_prices([], ["12345"])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает новые цены на товары в Яндекс.Маркет для указанной компании.

    Args:
        watch_remnants (list): Список доступных товаров с их ценами.
        campaign_id (str): Идентификатор кампании для обновления цен.
        market_token (str): Токен доступа для авторизации.

    Returns:
        list: Список успешно загруженных ценовых предложений.

    Examples:
        >>> await upload_prices([{"Код": '12345', 'Цена': "5'990.00 руб."}], '12345', 'your_market_token')
        [{'id': '12345', 'price': {'value': 5990}}]

        >>> await upload_prices([], '12345', 'your_market_token')
        # Вызывает ошибку из-за пустого списка цен.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загружает информацию о запасах в Яндекс.Маркет для указанной компании.

    Args:
        watch_remnants (list): Список доступных товаров с их количествами.
        campaign_id (str): Идентификатор кампании для обновления запасов.
        market_token (str): Токен доступа для авторизации.
        warehouse_id (str): Идентификатор склада с запасами.

    Returns:
        tuple: Кортеж из двух списков - непустых запасов и всех запасов.

    Examples:
        >>> await upload_stocks([{"Код": '12345', 'Количество': '>10'}], '12345', 'your_market_token', 'warehouse_1')
        ([{'sku': '12345', ...}], [{'sku': '12345', ...}])

        >>> await upload_stocks([], '12345', 'your_market_token', 'warehouse_1')
        # Вызывает ошибку из-за пустого списка запасов.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция запуска скрипта.

    Инициализирует переменные окружения и управляет процессом обновления запасов и цен
    как для FBS так и DBS компаний на Яндекс.Маркете на основе загруженных данных.

    Обрабатывает потенциальные ошибки во время выполнения и предоставляет обратную связь
    о проблемах с соединением или тайм-аутами.

    При запуске с неверными токенами, вызовет соответствующие исключения и выведет сообщения об ошибках.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
