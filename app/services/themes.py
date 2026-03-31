from __future__ import annotations

THEME_DEFINITIONS = [
    {
        "id": "economy",
        "title": "Экономика и инвестиции",
        "description": "ВРП, инвестиции, розничная торговля и устойчивость региональной экономики.",
        "indicators": [
            {
                "code": "Y477110006",
                "title": "ВРП на душу населения",
                "section": "Валовой региональный продукт",
            },
            {
                "code": "Y477110108",
                "title": "Инвестиции в основной капитал на душу населения",
                "section": "Инвестиции",
            },
            {
                "code": "Y477110224",
                "title": "Оборот розничной торговли на душу населения",
                "section": "Торговля и услуги населению",
            },
        ],
    },
    {
        "id": "population",
        "title": "Демография и качество жизни",
        "description": "Население, продолжительность жизни, доходы и бедность.",
        "indicators": [
            {
                "code": "Y477110461",
                "title": "Численность населения",
                "section": "Население",
            },
            {
                "code": "Y477110256",
                "title": "Ожидаемая продолжительность жизни",
                "section": "Население",
            },
            {
                "code": "Y477110374",
                "title": "Среднедушевые денежные доходы населения",
                "section": "Уровень жизни населения",
            },
        ],
    },
    {
        "id": "labor",
        "title": "Рынок труда и доходы",
        "description": "Безработица, заработная плата и вовлечение населения в экономику.",
        "indicators": [
            {
                "code": "Y477100008",
                "title": "Уровень безработицы населения",
                "section": "Безработные",
            },
            {
                "code": "Y477130013",
                "subsection": "Среднемесячная номинальная начисленная заработная плата, руб.",
                "title": "Среднемесячная номинальная заработная плата",
                "section": "Денежные доходы населения и их использование",
            },
            {
                "code": "Y477110470",
                "title": "Численность пенсионеров на 1000 человек населения",
                "section": "Уровень жизни населения",
            },
        ],
    },
    {
        "id": "housing",
        "title": "Жильё и строительство",
        "description": "Обеспеченность жильём, ввод жилья и цены на рынке.",
        "indicators": [
            {
                "code": "Y477110226",
                "title": "Площадь жилых помещений на одного жителя",
                "section": "Уровень жизни населения",
            },
            {
                "code": "Y477110017",
                "title": "Ввод в действие жилых домов на 1000 человек населения",
                "section": "Строительство",
            },
            {
                "code": "Y477190015",
                "title": "Средняя цена на вторичном рынке жилья",
                "section": "Уровень и динамика цен на потребительском рынке",
            },
        ],
    },
]

PROFILE_HERO_INDICATORS = [
    {"code": "Y477110461", "title": "Население"},
    {"code": "Y477110256", "title": "Ожидаемая продолжительность жизни"},
    {"code": "Y477110374", "title": "Доходы на душу населения"},
    {
        "code": "Y477130013",
        "subsection": "Среднемесячная номинальная начисленная заработная плата, руб.",
        "title": "Номинальная заработная плата",
    },
    {"code": "Y477100008", "title": "Уровень безработицы"},
    {"code": "Y477110108", "title": "Инвестиции на душу населения"},
]

MODELING_PRESETS = [
    {
        "id": "income_growth",
        "title": "Доходы населения",
        "description": "Доходы как функция зарплат, инвестиций и демографической базы.",
        "dependent_indicator": {"code": "Y477110374", "transform": "log"},
        "predictor_indicators": [
            {"code": "Y477130013", "transform": "log"},
            {"code": "Y477110108", "transform": "log", "lag_years": 1},
            {"code": "Y477110461", "transform": "log"},
        ],
        "include_year_fixed_effects": True,
        "include_object_fixed_effects": True,
        "year_from": 2015,
        "year_to": 2024,
    },
    {
        "id": "labor_market",
        "title": "Рынок труда",
        "description": "Безработица как функция доходов, зарплат и структуры населения.",
        "dependent_indicator": {"code": "Y477100008", "transform": "raw"},
        "predictor_indicators": [
            {"code": "Y477110374", "transform": "log"},
            {"code": "Y477130013", "transform": "log"},
            {"code": "Y477110470", "transform": "raw"},
        ],
        "include_year_fixed_effects": True,
        "include_object_fixed_effects": True,
        "year_from": 2012,
        "year_to": 2024,
    },
    {
        "id": "housing_prices",
        "title": "Цены на жильё",
        "description": "Цены на вторичке как функция доходов, ввода жилья и населения.",
        "dependent_indicator": {"code": "Y477190015", "transform": "log"},
        "predictor_indicators": [
            {"code": "Y477110374", "transform": "log"},
            {"code": "Y477110017", "transform": "log", "lag_years": 1},
            {"code": "Y477110461", "transform": "log"},
        ],
        "include_year_fixed_effects": True,
        "include_object_fixed_effects": False,
        "year_from": 2011,
        "year_to": 2024,
    },
]


def theme_codes(theme_id: str) -> list[str]:
    for theme in THEME_DEFINITIONS:
        if theme["id"] == theme_id:
            return [indicator["code"] for indicator in theme["indicators"]]
    return []
