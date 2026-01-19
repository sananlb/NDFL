"""
Custom template filters для отображения информации об инструментах
"""
from django import template
from decimal import Decimal

register = template.Library()


@register.filter
def instrument_type_plural(instr_kind):
    """
    Преобразует instr_kind (класс актива) в множественное число на русском языке

    Args:
        instr_kind: Класс актива (может быть на русском или английском)

    Returns:
        Название типа инструмента во множественном числе
    """
    if not instr_kind:
        return "Инструменты"

    # Словарь соответствий (и русский, и английский варианты)
    type_mapping = {
        'акции': 'Акции',
        'stocks': 'Акции',
        'акция': 'Акции',
        'опционы на акции и индексы': 'Опционы',
        'stock options': 'Опционы',
        'опцион': 'Опционы',
        'варранты': 'Варранты',
        'warrants': 'Варранты',
        'варрант': 'Варранты',
    }

    # Приводим к нижнему регистру для поиска
    instr_kind_lower = instr_kind.lower()

    # Ищем соответствие в словаре
    return type_mapping.get(instr_kind_lower, instr_kind.title())


@register.filter
def format_currency_breakdown(currencies_dict):
    """
    Форматирует словарь валют в строку вида "(−8047.34 USD, 340 CAD)"

    Args:
        currencies_dict: Словарь {currency: amount}

    Returns:
        Отформатированная строка с разбивкой по валютам
    """
    if not currencies_dict:
        return ""

    # Сортируем валюты для стабильного порядка отображения
    sorted_currencies = sorted(currencies_dict.items())

    # Форматируем каждую валюту
    formatted_parts = []
    for currency, amount in sorted_currencies:
        # Преобразуем в Decimal если нужно
        if not isinstance(amount, Decimal):
            try:
                amount = Decimal(str(amount))
            except:
                amount = Decimal(0)

        # Форматируем с 2 знаками после запятой
        formatted_amount = f"{amount:.2f}"
        formatted_parts.append(f"{formatted_amount} {currency}")

    # Объединяем в строку
    if formatted_parts:
        return f"({', '.join(formatted_parts)})"
    return ""
