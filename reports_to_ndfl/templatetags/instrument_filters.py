"""
Custom template filters для отображения информации об инструментах
"""
from django import template

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
