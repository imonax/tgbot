# config.py

BOT_TOKEN = "свой токен"

OLLAMA_MODEL = "mistral:7b"
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_EMBED_URL = "http://localhost:11434/api/embeddings"

MAX_NEWS_PER_TOPIC = 80
ARCHIVE_DAYS = 7
MAX_NEWS_AGE_HOURS = 24

SIMILARITY_THRESHOLD = 0.75
HIGH_CONFIDENCE_THRESHOLD = 4

REQUEST_TIMEOUT = 300
MAX_PARALLEL_REQUESTS = 1
MAX_GOOGLE_NEWS_REQUESTS = 44

STRICT_BELARUS_ONLY = True
BELARUS_DOMAIN = ".by"

# Ключевые слова Беларуси
BELARUS_WORDS = [
    "беларусь", "беларуси", "рб", "belarus",
    "лукашенко", "совмин", "правительство беларуси", "нацбанк", "минфин"
]

BELARUS_CITIES = [
    "минск", "гомель", "гродно", "брест", "витебск", "могилев", "могилёв",
    "бобруйск", "пинск", "лида", "мозырь", "новополоцк", "солигорск",
    "жлобин", "слуцк"
]

BELARUS_REGIONS = [
    "минская область", "гродненская область", "брестская область",
    "гомельская область", "витебская область", "могилевская область"
]

# Плохие домены (полный блок)
BLACKLIST_DOMAINS = [
    "nexta", "zerkalo", "charter97", "belsat", "svaboda", "euroradio",
    "radiofreeeurope", "currenttime", "motolkohelp", "nashaniva",
    "casino", "bet", "betera"
]

# Ключевые слова для блокировки (любые новости, содержащие их)
BLACKLIST_KEYWORDS = [
    "гороскоп", "астролог", "знак зодиака",
    "шоу-бизнес", "селебрити", "развод", "роман",
    "скандал", "сенсация", "тайна",
    "звезда", "певец", "актриса",
    "инстаграм", "тикток", "блогер",
    "реалити шоу", "стрим", "мем", "прикол",
    "криминал", "убийство", "ограбление", "пожар", "дтп",  # общие стоп-слова для всех тем
    "спорт", "футбол", "хоккей", "теннис", "олимпиада",
    "культура", "театр", "кино", "фестиваль", "выставка",
    "здоровье", "медицина", "болезнь", "вакцина",
    "образование", "школа", "вуз", "экзамен",
    "рецепт", "кулинария", "еда"
]

# Негативные слова для каждой темы (будут вычитаться при подсчёте)
TOPIC_NEGATIVE = {
    "macro_inflation": [
        "фестиваль", "концерт", "кино", "театр", "спорт",
        "авария", "дтп", "погиб", "криминал"
    ],
    "fiscal_monetary": [
        "урожай", "фермер", "посевная", "дтп", "авария", "спорт"
    ],
    "investments_trade": [
        "урожай", "погода", "циклон", "осадки", "дтп", "криминал"
    ],
    "industry": [
        "кино", "театр", "фестиваль", "спорт", "криминал", "авария"
    ],
    "agriculture": [
        "кино", "театр", "фестиваль", "концерт", "it", "программирование",
        "стартап", "банк", "ставка", "инфляция", "дтп", "криминал"
    ],
    "construction": [
        "дтп", "авария", "погиб", "криминал", "спорт"
    ],
    "ict": [
        "оружие", "взрыв", "убийство", "криминал", "дтп", "погода",
        "рецепт", "еда", "спорт", "шоу-бизнес"
    ],
    "transport": [
        "кино", "театр", "фестиваль", "криминал", "спорт", "погода"
    ],
    "politics": [
        "компания", "бизнес", "жилье", "квартира", "недвижимость",
        "фермер", "логистика", "грузоперевозки", "температура", "осадки",
        "циклон", "фестиваль", "концерт", "кино", "режиссер", "оскар"
    ],
    "weather": [
        "политика", "выборы", "президент", "экономика", "бизнес", "спорт"
    ]
}

# Связь сущностей с темами (для усиления скоринга)
ENTITY_TOPIC = {
    # Макро + инфляция
    "ввп": "macro_inflation",
    "мвф": "macro_inflation",
    "всемирный банк": "macro_inflation",
    "инфляция": "macro_inflation",
    "индекс потребительских цен": "macro_inflation",

    # Фискально-монетарная
    "нацбанк": "fiscal_monetary",
    "национальный банк": "fiscal_monetary",
    "ставка рефинансирования": "fiscal_monetary",
    "ключевая ставка": "fiscal_monetary",
    "минфин": "fiscal_monetary",
    "министерство финансов": "fiscal_monetary",
    "госбюджет": "fiscal_monetary",
    "бюджет": "fiscal_monetary",
    "дефицит бюджета": "fiscal_monetary",

    # Инвестиции и торговля
    "инвестиции": "investments_trade",
    "инвестор": "investments_trade",
    "экспорт": "investments_trade",
    "импорт": "investments_trade",
    "торговый баланс": "investments_trade",
    "розничная торговля": "investments_trade",
    "магазины": "investments_trade",

    # Промышленность
    "белаз": "industry",
    "маз": "industry",
    "мтз": "industry",
    "беларуськалий": "industry",
    "нафтан": "industry",
    "промышленность": "industry",
    "производство": "industry",
    "завод": "industry",

    # Сельское хозяйство
    "урожай": "agriculture",
    "посевная": "agriculture",
    "уборочная": "agriculture",
    "фермер": "agriculture",
    "сельское хозяйство": "agriculture",

    # Строительство
    "жилье": "construction",
    "новостройка": "construction",
    "строительство": "construction",
    "застройщик": "construction",

    # IT
    "парк высоких технологий": "ict",
    "пвт": "ict",
    "айти": "ict",
    "it": "ict",
    "технологии": "ict",
    "стартап": "ict",

    # Транспорт
    "логистика": "transport",
    "грузоперевозки": "transport",
    "транспорт": "transport",

    # Политика
    "лукашенко": "politics",
    "совмин": "politics",
    "правительство": "politics",
    "администрация президента": "politics",
    "палата представителей": "politics",
    "совет республики": "politics",
    "мид": "politics",
    "мвд": "politics",
    "президент": "politics",
    "министр": "politics",
    "указ": "politics",

    # Погода
    "циклон": "weather",
    "осадки": "weather",
    "температура": "weather",
    "погода": "weather"
}

# Описание тем (ключ – идентификатор, значение – отображаемое название и ключевые слова)
TOPICS = {
    "macro_inflation": {
        "title": "📈 Макроэкономика и инфляция",
        "keywords": [
            "экономика", "ввп", "экономический рост", "рецессия",
            "инфляция", "рост цен", "индекс цен", "потребительские цены",
            "дефляция", "стагфляция", "макроэкономика"
        ],
        "exclude": TOPIC_NEGATIVE.get("macro_inflation", []) + BLACKLIST_KEYWORDS
    },
    "fiscal_monetary": {
        "title": "💰 Бюджет и монетарная политика",
        "keywords": [
            "госбюджет", "бюджет", "дефицит бюджета", "профицит",
            "ставка рефинансирования", "ключевая ставка", "монетарная политика",
            "денежно-кредитная политика", "нацбанк", "минфин",
            "госдолг", "внешний долг", "налоги", "льготы"
        ],
        "exclude": TOPIC_NEGATIVE.get("fiscal_monetary", []) + BLACKLIST_KEYWORDS
    },
    "investments_trade": {
        "title": "📊 Инвестиции и торговля",
        "keywords": [
            "инвестиции", "инвестор", "инвестпроект", "прямые инвестиции",
            "экспорт", "импорт", "торговый баланс", "внешняя торговля",
            "внутренняя торговля", "розничная торговля", "магазины", "товарооборот",
            "санкции", "эмбарго", "пошлины"
        ],
        "exclude": TOPIC_NEGATIVE.get("investments_trade", []) + BLACKLIST_KEYWORDS
    },
    "industry": {
        "title": "🏭 Промышленность",
        "keywords": [
            "промышленность", "производство", "завод", "фабрика",
            "белаз", "маз", "мтз", "беларуськалий", "нафтан", "нефтепереработка",
            "машиностроение", "станкостроение", "металлургия", "химическая промышленность",
            "легкая промышленность", "пищевая промышленность"
        ],
        "exclude": TOPIC_NEGATIVE.get("industry", []) + BLACKLIST_KEYWORDS
    },
    "agriculture": {
        "title": "🌾 Сельское хозяйство",
        "keywords": [
            "сельское хозяйство", "урожай", "фермер", "посевная", "уборочная",
            "агропромышленный", "агро", "зерно", "овощи", "фрукты", "мясо", "молоко",
            "животноводство", "растениеводство", "теплицы", "агроусадьба"
        ],
        "exclude": TOPIC_NEGATIVE.get("agriculture", []) + BLACKLIST_KEYWORDS
    },
    "construction": {
        "title": "🏗 Строительство",
        "keywords": [
            "строительство", "застройщик", "жилье", "новостройка", "квартира",
            "недвижимость", "стройка", "стройматериалы", "ипотека", "жилищное строительство",
            "объекты строительства", "подрядчик"
        ],
        "exclude": TOPIC_NEGATIVE.get("construction", []) + BLACKLIST_KEYWORDS
    },
    "ict": {
        "title": "💻 IT и технологии",
        "keywords": [
            "it", "айти", "технологии", "информационные технологии",
            "парк высоких технологий", "пвт", "стартап", "программное обеспечение",
            "искусственный интеллект", "инновации", "цифровизация", "компьютеры"
        ],
        "exclude": TOPIC_NEGATIVE.get("ict", []) + BLACKLIST_KEYWORDS
    },
    "transport": {
        "title": "🚚 Транспорт и логистика",
        "keywords": [
            "логистика", "грузоперевозки", "транспорт", "перевозки",
            "железная дорога", "автомобильные дороги", "авиаперевозки",
            "транспортная инфраструктура", "таможня", "транзит"
        ],
        "exclude": TOPIC_NEGATIVE.get("transport", []) + BLACKLIST_KEYWORDS
    },
    "politics": {
        "title": "🏛 Политика",
        "keywords": [
            "президент", "правительство", "министр", "указ", "закон",
            "совмин", "администрация президента", "депутат", "парламент",
            "выборы", "референдум", "конституция", "политика"
        ],
        "exclude": TOPIC_NEGATIVE.get("politics", []) + BLACKLIST_KEYWORDS
    },
    "weather": {
        "title": "🌦 Погода",
        "keywords": [
            "погода", "температура", "осадки", "циклон", "антициклон",
            "дождь", "снег", "ветер", "потепление", "похолодание",
            "прогноз погоды", "метеорологи", "климат"
        ],
        "exclude": TOPIC_NEGATIVE.get("weather", []) + BLACKLIST_KEYWORDS
    },
    "other": {
        "title": "📰 Другое",
        "keywords": []
    }
}

# RSS-ленты (без изменений)
BASE_RSS = [
    "https://belta.by/rss",
    #"https://sputnik.by/export/rss2/index.xml",
    #"https://president.gov.by/ru.xml",
    #"https://smartpress.by/rss/news",
    #"https://people.onliner.by/feed",
    #"https://lenta.ru/rss/google-newsstand/main/",
    #"https://npr.by/rss",
    #"https://ru.euronews.com/rss",
    #"https://neg.by/rss",
    #"https://www.mk.ru/rss/index.xml",
    #"https://www.sb.by/news-rss/google-xml/",
    #"https://belarus-news.by/rss.xml",
    #"https://ria.ru/export/rss2/archive/index.xml",
    #"https://blizko.by/feed.rss",
    #"https://money.onliner.by/feed",
    #"https://telegraf.news/feed/",
    #"https://officelife.media/news/rss/",
    #"https://pressball.by/feed",
    #"https://tech.onliner.by/feed",
    #"https://www.tio.by/rss",
    #"https://pogoda.by/rss/news",
    #"https://myfin.by/rss",
    #"https://udf.name/rss.xml",
    #"https://rsshub.app/telegram/channel/econ_gov_by",
    #"https://rsshub.app/telegram/channel/pul_1"
]
