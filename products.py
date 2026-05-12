REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/134.0.0.0 YaBrowser/25.4.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "ru,en;q=0.9",
    "Referer": "https://m.cngold.org//",
}

REQUEST_TIMEOUT_SECONDS = 10

PRODUCTS = [
    {
        "key": "raps_oil",
        "title": "Рапсовое масло",
        "source_name": "jijinhao",
        "api_url": "https://api.jijinhao.com/quoteCenter/realTime.htm",
        "codes": "JO_166042",
        "parser_type": "json",
        "response_prefix": "var quote_json = ",
        "price_path": ["JO_166042", "q5"],
    },
    {
        "key": "soevoe_oil",
        "title": "Соевое масло",
        "source_name": "jijinhao",
        "api_url": "https://api.jijinhao.com/sQuoteCenter/realTime.htm",
        "codes": "JO_165951",
        "parser_type": "csv",
        "response_prefix": "var hq_str_JO_165951 = ",
        "price_index": 3,
    },
    {
        "key": "raps_shrot",
        "title": "Рапсовый шрот",
        "source_name": "jijinhao",
        "api_url": "https://api.jijinhao.com/sQuoteCenter/realTime.htm",
        "codes": "JO_166106",
        "parser_type": "csv",
        "response_prefix": "var hq_str_JO_166106 = ",
        "price_index": 3,
    },
]
