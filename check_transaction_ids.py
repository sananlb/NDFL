import xml.etree.ElementTree as ET
from decimal import Decimal
import json
from datetime import datetime

# =====================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# =====================================================================
XML_FILE = 'media/xml_files/broker_nalbantovfml1gmail.com_2023-12-31_23_59_59_2024-12-31_23_59_59.xml'
TICKERS_TO_ANALYZE = ['NFE.US', 'PBR.US']

# =====================================================================
# ЗАГРУЗКА ДАННЫХ
# =====================================================================
tree = ET.parse(XML_FILE)
root = tree.getroot()
cash_in_outs = root.find('.//cash_in_outs')

# =====================================================================
# ЗАГОЛОВОК
# =====================================================================
print("\n" + "═" * 120)
print("║" + " " * 118 + "║")
print("║" + "АНАЛИЗ TRANSACTION_ID ДЛЯ СВЯЗИ DIVIDEND И DIVIDEND_REVERTED".center(118) + "║")
print("║" + " " * 118 + "║")
print("═" * 120 + "\n")

# =====================================================================
# СБОР ДАННЫХ ИЗ XML
# =====================================================================
print("┌─ Этап 1: Сбор данных из XML файла")
print("│")

records = []

for node in cash_in_outs.findall('node'):
    ticker = node.findtext('ticker', '').strip()

    if ticker in TICKERS_TO_ANALYZE:
        cio_type = node.findtext('type', '').strip()
        amount_str = node.findtext('amount', '0')
        amount = Decimal(amount_str)
        pay_d = node.findtext('pay_d', '')[:10]

        details_json_str = node.findtext('details', '')
        ca_id_from_details = None
        if details_json_str:
            try:
                details_data = json.loads(details_json_str)
                ca_id_from_details = details_data.get('corporate_action_id')
            except json.JSONDecodeError:
                pass

        ca_id_direct = node.findtext('corporate_action_id', '')
        record_id = node.findtext('id', '')
        transaction_id = node.findtext('transaction_id', '')

        records.append({
            'ticker': ticker,
            'type': cio_type,
            'amount': amount,
            'pay_d': pay_d,
            'ca_id': ca_id_from_details or ca_id_direct,
            'id': record_id,
            'transaction_id': transaction_id
        })

# Сортируем
records.sort(key=lambda x: (x['ticker'], x['pay_d']))

print(f"│  ✓ Найдено записей для анализа: {len(records)}")
print("└─" + "─" * 78 + "\n")

# =====================================================================
# РАЗДЕЛЕНИЕ НА КАТЕГОРИИ
# =====================================================================
print("┌─ Этап 2: Классификация записей")
print("│")

dividends = [r for r in records if r['type'].lower() == 'dividend' and r['amount'] > 0]
dividends_reverted = [r for r in records if r['type'].lower() == 'dividend_reverted']

print(f"│  • Дивидендов (dividend):            {len(dividends)}")
print(f"│  • Отмененных дивидендов (reverted): {len(dividends_reverted)}")
print("└─" + "─" * 78 + "\n")

# =====================================================================
# МЕТОД 1: СОПОСТАВЛЕНИЕ ПО TRANSACTION_ID
# =====================================================================
print("┌─ Этап 3: Сопоставление по TRANSACTION_ID")
print("│")

matched_by_transaction = 0
unmatched_reverted = []

for div_rev in dividends_reverted:
    # Ищем дивиденд с таким же transaction_id
    matching_div = None
    for div in dividends:
        if div['transaction_id'] == div_rev['transaction_id']:
            matching_div = div
            break

    if matching_div:
        print(f"│  ✓ ПАРА НАЙДЕНА [transaction_id: {div_rev['transaction_id']}]")
        print(f"│    ├─ Dividend:         {matching_div['pay_d']} │ {matching_div['ticker']:8} │ {str(matching_div['amount']):>10} USD")
        print(f"│    └─ Reverted:         {div_rev['pay_d']} │ {div_rev['ticker']:8} │ {str(div_rev['amount']):>10} USD")
        print("│")
        matched_by_transaction += 1
    else:
        unmatched_reverted.append(div_rev)
        print(f"│  ✗ НЕТ ПАРЫ: {div_rev['pay_d']} │ {div_rev['ticker']:8} │ {str(div_rev['amount']):>10} USD │ txn_id={div_rev['transaction_id']}")
        print("│")

print(f"│  Итого: {matched_by_transaction} из {len(dividends_reverted)} сопоставлено")
print("└─" + "─" * 78 + "\n")

# =====================================================================
# МЕТОД 2: СОПОСТАВЛЕНИЕ ПО CA_ID + СУММА + ДАТА
# =====================================================================
if matched_by_transaction < len(dividends_reverted):
    print("┌─ Этап 4: Альтернативное сопоставление (CA_ID + Сумма + Дата)")
    print("│")

    matched_by_alternative = 0

    for div_rev in unmatched_reverted:
        # Ищем по CA_ID, сумме и близкой дате
        div_rev_date = datetime.strptime(div_rev['pay_d'], '%Y-%m-%d').date()
        div_rev_amount = abs(div_rev['amount'])

        for div in dividends:
            if (div['ca_id'] == div_rev['ca_id'] and
                div['amount'] == div_rev_amount and
                div['ticker'] == div_rev['ticker']):

                div_date = datetime.strptime(div['pay_d'], '%Y-%m-%d').date()
                days_diff = abs((div_rev_date - div_date).days)

                print(f"│  ⚠ ВОЗМОЖНАЯ ПАРА (CA_ID + сумма, Δ{days_diff} дней)")
                print(f"│    ├─ Dividend:         {div['pay_d']} │ {div['ticker']:8} │ {str(div['amount']):>10} USD │ CA_ID={div['ca_id']}")
                print(f"│    └─ Reverted:         {div_rev['pay_d']} │ {div_rev['ticker']:8} │ {str(div_rev['amount']):>10} USD │ CA_ID={div_rev['ca_id']}")
                print("│")
                matched_by_alternative += 1
                break

    print(f"│  Итого: {matched_by_alternative} дополнительных совпадений найдено")
    print("└─" + "─" * 78 + "\n")

# =====================================================================
# ИТОГОВАЯ СВОДКА
# =====================================================================
print("═" * 120)
print("║" + " " * 118 + "║")
print("║" + "ИТОГОВЫЕ РЕЗУЛЬТАТЫ".center(118) + "║")
print("║" + " " * 118 + "║")
print("═" * 120)
print()

print("┌─ Статистика сопоставления")
print("│")
print(f"│  Всего отмененных дивидендов:              {len(dividends_reverted)}")
print(f"│  Сопоставлено по TRANSACTION_ID:           {matched_by_transaction}")

if matched_by_transaction < len(dividends_reverted):
    alternative_matches = len(dividends_reverted) - matched_by_transaction
    print(f"│  Требуют альтернативного сопоставления:    {alternative_matches}")

print("│")
print("├─ Заключение")
print("│")

if matched_by_transaction == len(dividends_reverted):
    print("│  ✅ УСПЕШНО: Все отмененные дивиденды сопоставлены по TRANSACTION_ID!")
    print("│")
    print("│  Вывод: transaction_id у dividend и dividend_reverted СОВПАДАЕТ.")
    print("│         Можно использовать transaction_id как единственный критерий связи.")
else:
    print(f"│  ⚠️  ВНИМАНИЕ: По transaction_id сопоставлено только {matched_by_transaction} из {len(dividends_reverted)}")
    print("│")
    print("│  Рекомендация: Использовать комбинированный подход:")
    print("│                1. Приоритет - сопоставление по TRANSACTION_ID")
    print("│                2. Резервный метод - CA_ID + сумма + близкая дата")

print("│")
print("└─" + "─" * 78)
print("\n" + "═" * 120 + "\n")
