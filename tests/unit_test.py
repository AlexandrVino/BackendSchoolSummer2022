# encoding=utf8
import datetime
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

# API_BASEURL = "https://have-1826.usr.yandex-academy.ru"
API_BASEURL = "http://localhost:80"

ROOT_ID = "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1"

IMPORT_BATCHES = [

    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Товары",
                "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
                "parentId": None,
                "children": []
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z"
    },
    {
        "items": [
            {
                "type": "category",
                "name": "Смартфоны",
                "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            },
            {
                "type": "OFFER",
                "name": "jPhone 13",
                "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "price": 79999
            },
            {
                "type": "OFFER",
                "name": "Xomiа Readme 10",
                "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                "price": 59999
            }
        ],
        "updateDate": "2022-02-02T12:00:00.000Z"
    },

    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Телевизоры",
                "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            },
            {
                "type": "OFFER",
                "name": "Samson 70\" LED UHD Smart",
                "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 32999
            },
            {
                "type": "OFFER",
                "name": "Phyllis 50\" LED UHD Smarter",
                "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 49999
            }
        ],
        "updateDate": "2022-02-03T12:00:00.000Z"
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": "Goldstar 65\" LED UHD LOL Very Smart",
                "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                "price": 69999
            }
        ],
        "updateDate": "2022-02-03T15:00:00.000Z"
    }
]

INCORRECT_IMPORT_BATCHES = [

    {
        "items": [
            {
                "type": "CATEGORY",
                "name": "Товары",
                "id": "1231312-3-123-1-23-1",
                "parentId": '863e1a7a-1304-42ae-943b-179184c077e3',
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z"
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": "Товары",
                "id": "313123123wadad123123",
                "parentId": '069cb8d7-bbdd-47d3-ad8f-82ef4c269df',
                "price": 1000
            }
        ],
        "updateDate": "2adfasfsdgvbsd"
    },
    {
        "items": [
            {
                "type": "OFFER",
                "name": "Товары",
                "id": "313123123wdawdawdadad123123",
                "parentId": '069cb8d7-bbdd-47d3-ad8f-82ef4c269df',
                "price": -1007
            }
        ],
        "updateDate": "2022-02-01T12:00:00.000Z"
    },
    {
        "items": [],
        "updateDate": "2022-02-01T12:00:00.000Z"
    },
]

EXPECTED_TREE = {
    "type": "CATEGORY",
    "name": "Товары",
    "id": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
    "price": 58599,
    "parentId": None,
    "date": "2022-02-03T15:00:00.000Z",
    "children": [
        {
            "type": "CATEGORY",
            "name": "Телевизоры",
            "id": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "price": 50999,
            "date": "2022-02-03T15:00:00.000Z",
            "children": [
                {
                    "type": "OFFER",
                    "name": "Samson 70\" LED UHD Smart",
                    "id": "98883e8f-0507-482f-bce2-2fb306cf6483",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 32999,
                    "date": "2022-02-03T12:00:00.000Z",
                    "children": None,
                },
                {
                    "type": "OFFER",
                    "name": "Phyllis 50\" LED UHD Smarter",
                    "id": "74b81fda-9cdc-4b63-8927-c978afed5cf4",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 49999,
                    "date": "2022-02-03T12:00:00.000Z",
                    "children": None
                },
                {
                    "type": "OFFER",
                    "name": "Goldstar 65\" LED UHD LOL Very Smart",
                    "id": "73bc3b36-02d1-4245-ab35-3106c9ee1c65",
                    "parentId": "1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2",
                    "price": 69999,
                    "date": "2022-02-03T15:00:00.000Z",
                    "children": None
                }
            ]
        },
        {
            "type": "CATEGORY",
            "name": "Смартфоны",
            "id": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
            "parentId": "069cb8d7-bbdd-47d3-ad8f-82ef4c269df1",
            "price": 69999,
            "date": "2022-02-02T12:00:00.000Z",
            "children": [
                {
                    "type": "OFFER",
                    "name": "jPhone 13",
                    "id": "863e1a7a-1304-42ae-943b-179184c077e3",
                    "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                    "price": 79999,
                    "date": "2022-02-02T12:00:00.000Z",
                    "children": None
                },
                {
                    "type": "OFFER",
                    "name": "Xomiа Readme 10",
                    "id": "b1d8fd7d-2ae3-47d5-b2f9-0f094af800d4",
                    "parentId": "d515e43f-f3f6-4471-bb77-6b455017a2d2",
                    "price": 59999,
                    "date": "2022-02-02T12:00:00.000Z",
                    "children": None
                }
            ]
        },
    ]
}

SALES_EXAMPLE = [
    {
        'id': '98883e8f-0507-482f-bce2-2fb306cf6483',
        'name': 'Samson 70" LED UHD Smart',
        'date': '2022-02-03T12:00:00.000Z',
        'parentId': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
        'type': 'OFFER',
        'price': 32999
    },
    {
        'id': '74b81fda-9cdc-4b63-8927-c978afed5cf4',
        'name': 'Phyllis 50" LED UHD Smarter',
        'date': '2022-02-03T12:00:00.000Z',
        'parentId': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
        'type': 'OFFER',
        'price': 49999
    },
    {
        'id': '73bc3b36-02d1-4245-ab35-3106c9ee1c65',
        'name': 'Goldstar 65" LED UHD LOL Very Smart',
        'date': '2022-02-03T15:00:00.000Z',
        'parentId': '1cc0129a-2bfe-474c-9ee6-d435bf5fc8f2',
        'type': 'OFFER',
        'price': 69999
    }
]

STATS_EXAMPLE = {
    'id': '069cb8d7-bbdd-47d3-ad8f-82ef4c269df1',
    'name': 'Товары',
    'parentId': None,
    'type': 'CATEGORY',
    'price': 58599,
    'stats': [
        {'update_date': '2022-02-02T12:00:00.000Z', 'price': 69999},
        {'update_date': '2022-02-03T12:00:00.000Z', 'price': 55749},
        {'update_date': '2022-02-03T15:00:00.000Z', 'price': 58599}
    ]
}


def request(path, method="GET", data=None, json_response=False):
    try:
        params = {
            "url": f"{API_BASEURL}{path}",
            "method": method,
            "headers": {},
        }

        if data:
            params["data"] = json.dumps(
                data, ensure_ascii=False).encode("utf-8")
            params["headers"]["Content-Length"] = len(params["data"])
            params["headers"]["Content-Type"] = "application/json"

        req = urllib.request.Request(**params)

        with urllib.request.urlopen(req) as res:
            res_data = res.read().decode("utf-8")
            if json_response:
                res_data = json.loads(res_data)
            return (res.getcode(), res_data)
    except urllib.error.HTTPError as e:
        return (e.getcode(), None)


def deep_sort_children(node):
    if node.get("children"):
        node["children"].sort(key=lambda x: x["id"])

        for child in node["children"]:
            deep_sort_children(child)


def sort_stats(node):
    if node.get("stats"):
        node["stats"].sort(key=lambda x: x["update_date"])


def sort_sales(node):
    node.sort(key=lambda x: x["id"])


def print_diff(expected, response):
    with open("expected.json", "w") as f:
        json.dump(expected, f, indent=4, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    with open("response.json", "w") as f:
        json.dump(response, f, indent=4, ensure_ascii=False, sort_keys=True)
        f.write("\n")

    subprocess.run(["git", "--no-pager", "diff", "--no-index",
                    "expected.json", "response.json"])


def test_import():
    for index, batch in enumerate(IMPORT_BATCHES):
        start = datetime.datetime.now()
        print(f"Importing batch {index}")
        status, _ = request("/imports", method="POST", data=batch)

        assert status == 200, f"Expected HTTP status code 200, got {status}"
        print(f"Import {index} request time: %s" % (datetime.datetime.now() - start))
        print()

    for index, batch in enumerate(INCORRECT_IMPORT_BATCHES):
        start = datetime.datetime.now()
        print(f"Incorrect importing batch {index}")
        status, _ = request("/imports", method="POST", data=batch)

        assert status == 400, f"Expected HTTP status code 400, got {status}"
        print(f"Import {index} request time: %s" % (datetime.datetime.now() - start))
        print()

    print("Test import passed.")


def test_nodes():
    start = datetime.datetime.now()
    status, response = request(f"/nodes/{ROOT_ID}", json_response=True)
    # print(json.dumps(response, indent=2, ensure_ascii=False))

    assert status == 200, f"Expected HTTP status code 200, got {status}"

    deep_sort_children(response)
    deep_sort_children(EXPECTED_TREE)
    if response != EXPECTED_TREE:
        print_diff(EXPECTED_TREE, response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Nodes sucsess request time: %s" % (datetime.datetime.now() - start))

    start = datetime.datetime.now()
    # мой тест на заведомо неверный id
    status, response = request("/nodes/bla_bla_bla", json_response=True)
    assert status == 404, f"Expected HTTP status code 404, got {status}"
    print("Nodes sucsess request time: %s" % (datetime.datetime.now() - start))
    print()

    print("Test nodes passed.")


def test_sales():
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({"date": "2022-02-04T00:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"

    sort_sales(response)
    sort_sales(SALES_EXAMPLE)

    if response != SALES_EXAMPLE:
        print_diff(SALES_EXAMPLE, response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Sales request time: %s" % (datetime.datetime.now() - start))
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({"date": "2022-01-04T00:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    if response:
        print_diff([], response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Sales request time: %s" % (datetime.datetime.now() - start))
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({"date": "2022-01-0wmsnfnT00:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Sales request time: %s" % (datetime.datetime.now() - start))
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({"example": "2022-01-0wmsnfnT00:00:00.000Z"})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Sales request time: %s" % (datetime.datetime.now() - start))
    print("Test sales passed.")


def test_stats():
    start = datetime.datetime.now()
    params = urllib.parse.urlencode({
        "dateStart": "2022-02-01T00:00:00.000Z", "dateEnd": "2022-02-04T00:00:00.000Z"
    })
    status, response = request(f"/node/{ROOT_ID}/statistic?{params}", json_response=True)
    assert status == 200, f"Expected HTTP status code 200, got {status}"

    sort_stats(response)
    sort_stats(STATS_EXAMPLE)

    if response != STATS_EXAMPLE:
        print_diff(STATS_EXAMPLE, response)
        print("Response tree doesn't match expected tree.")
        sys.exit(1)

    print("Stats request time: %s" % (datetime.datetime.now() - start))
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({
        "dateStart": "2022-02-daada:00:00.000Z", "dateEnd": "2022-02-04T00:00:00.000Z"
    })
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Stats request time: %s" % (datetime.datetime.now() - start))
    start = datetime.datetime.now()

    params = urllib.parse.urlencode({})
    status, response = request(f"/sales?{params}", json_response=True)
    assert status == 400, f"Expected HTTP status code 400, got {status}"

    print("Stats request time: %s" % (datetime.datetime.now() - start))
    print("Test stats passed.")


def test_delete():
    start = datetime.datetime.now()
    status, _ = request(f"/delete/{ROOT_ID}", method="DELETE")
    assert status == 200, f"Expected HTTP status code 200, got {status}"
    print("Delete request time: %s" % (datetime.datetime.now() - start))

    start = datetime.datetime.now()
    status, _ = request(f"/nodes/{ROOT_ID}", json_response=True)
    assert status == 404, f"Expected HTTP status code 404, got {status}"
    print("Delete request time: %s" % (datetime.datetime.now() - start))

    print("Test delete passed.")


def test_all():
    test_import()
    print()
    print()

    test_nodes()
    print()
    print()

    test_sales()
    print()
    print()

    test_stats()
    print()
    print()

    # test_delete()


def main():
    global API_BASEURL
    test_name = None

    for arg in sys.argv[1:]:
        if re.match(r"^https?://", arg):
            API_BASEURL = arg
        elif test_name is None:
            test_name = arg

    if API_BASEURL.endswith('/'):
        API_BASEURL = API_BASEURL[:-1]

    if test_name is None:
        test_all()
    else:
        test_func = globals().get(f"test_{test_name}")
        if not test_func:
            print(f"Unknown test: {test_name}")
            sys.exit(1)
        test_func()


if __name__ == "__main__":
    main()
