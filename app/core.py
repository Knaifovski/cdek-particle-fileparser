import time
import pandas as pd
import json
import requests as requests

from Databases import Database
from .config import Settings

settings = Settings()
Shiptor = Database.Shiptor_Database


def run():
    data = openfile()
    print(data)
    print("=" * 10)
    for package in data:
        print(package)
        print("Шаг 1")
        print(setsold(int(package['package'][2:]), package['products']))
        print("Шаг 2")
        print(partial_return(previous_id=int(package['package'][2:]), place_count=package['not_sold_count']))
        print("Шаг 3")
        print("childs: "+set_delivered(package_id=int(package['package'][2:]), date_time = package['delivered_time']))
        print(package['package'])
        time.sleep(15)


def openfile():
    df = pd.read_excel("D:\\cdek.xlsx", sheet_name=0)
    columns = df.columns.values
    # packages_info = {}
    packages_info = []
    try:
        previus_rp = None
        not_sold_count = 0
        products = []
        delivered_time = ""
        for line in df.loc:
            if previus_rp == None:
                previus_rp = line['Номер отправления ИМ']
            if line['Номер отправления ИМ'] != previus_rp:
                packages_info.append(package_model(rp=previus_rp,
                                                   products_list=products,
                                                   not_sold_count=not_sold_count,
                                                   delivered_time=delivered_time))
                products = []
                not_sold_count = 0

            print(f"previus_rp : {previus_rp} name: {line['Наименование товара']}")

            previus_rp = line['Номер отправления ИМ']
            products.append(product_model(name=line['Наименование товара'],
                                          status=line['Количество выкупленных единиц товара'],
                                          article=int(line['Артикул товара']),
                                          count=1))
            delivered_time = line['Дата доставки']
            if line['Количество выкупленных единиц товара'] == 'не выкуплено':
                not_sold_count += 1

    except KeyError:
        print('finish')
    return packages_info


def setsold(package_id: int, products: list):
    sold_products_list = []
    for product in products:
        sold_products_list.append(sold_product_model(product['article'], product['status']))
    url = f"https://api.shiptor.ru/pvz/v1?key={settings.shiptor_api_key}"
    json_ = f"""
        {{
            "id": "manualrequest",
            "jsonrpc": "2.0",
            "method": "pvz.package.setSoldProducts",
            "params": {{
                "package_id": {package_id},
                "sold_products": {sold_products_list}
            }}
        }}
    """
    push_api(url=url, json_=json_)
    return json_


def partial_return(previous_id: int, place_count: int):
    url = f"https://api.shiptor.ru/pvz/v1?key={settings.shiptor_api_key}"
    json_ = f"""
            {{"jsonrpc": "2.0", 
            "method": "pvz.package.createPartialReturn", 
            "params": {{
            "previous_id": {previous_id}, 
            "place_count": {place_count}
            }},	
            "id": "manualrequest"
            }}"""
    push_api(url=url, json_=json_)
    return json_


def set_delivered(package_id: int, date_time: str):
    childs = Shiptor.get(f"select id from package where parent_id={package_id}")
    for child in childs:
        url = f"https://api.shiptor.ru/pvz/v1?key={settings.shiptor_api_key}"
        json_= f"""
        {{"jsonrpc": "2.0", 
        "method": "pvz.package.setDelivered", 
        "params": {{
        "id": {child['id']}, 
        "datetime": "{date_time} 00:00:00"
        }},	
        "id": "manualrequest"
        }}
        """
        print(json_)
        push_api(url=url, json_=json_)
    return "200"


def push_api(url:str, json_: str):
    json_=json_.replace("\'","\"")
    response = requests.post(
        url,
        json=json.loads(json_)
    )
    response.raise_for_status()
    response_data = response.json()
    print("response: {}".format(response_data))
    return response_data


def sold_product_model(article, status) -> dict:
    if status == 'выкуплено':
        status = 1
    elif status == 'не выкуплено':
        status = 0
    return {"barcode": str(article), "mark_code": "", "count": status}


def package_model(rp: str, products_list: list, not_sold_count: int, delivered_time: str):
    return {"package": rp, "products": products_list, 'not_sold_count': not_sold_count,
            'delivered_time': delivered_time}


def product_model(name: str, status: str, article: int, count: int):
    return {"name": name, "status": status, "article": article, "count": count}
