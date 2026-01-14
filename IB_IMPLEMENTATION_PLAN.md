# План внедрения анализа отчетов Interactive Brokers (IB)

**Дата создания:** 2026-01-03
**Статус:** В разработке
**Целевой брокер:** Interactive Brokers LLC

---

## 1. Анализ текущей архитектуры проекта

### 1.1 Текущее состояние
- **Поддерживаемый брокер:** Freedom Finance Global (FFG)
- **Формат отчетов FFG:** XML
- **Парсер FFG:** `reports_to_ndfl/FFG_ndfl.py` (1879 строк)
- **Модель БД:** `UploadedXMLFile` (хранение только XML файлов)
- **Папка хранения:** `media/xml_files/`

### 1.2 Особенности отчетов IB
- **Формат:** CSV (Activity Statement)
- **Кодировка:** UTF-8 с BOM
- **Структура:** Многосекционный CSV с заголовками разделов
- **Период:** Годовые отчеты (January 1 - December 31)
- **Язык:** Русский (локализованный)

---

## 2. Структура отчетов Interactive Brokers

### 2.1 Основные секции CSV отчета

| Секция | Название в CSV | Назначение |
|--------|----------------|-----------|
| **Информация о счете** | `Информация о счете` | Метаданные счета, базовая валюта |
| **Сделки** | `Сделки` | Все торговые операции (акции, опционы, варранты) |
| **Дивиденды** | `Дивиденды` | Дивидендные выплаты |
| **Удерживаемый налог** | `Удерживаемый налог` | Налоги, удержанные у источника |
| **Процент** | `Процент` | **ПРОЧИЙ ДОХОД** (после дивидендов): начисленные/списанные проценты по марже и cash остаткам |
| **Переводы** | `Переводы` | **КРИТИЧНО для FIFO**: акции, покинувшие/поступившие на счет без сделки (межброкерские переводы, депозитарий) |
| **Корпоративные действия** | `Корпоративные действия` | **КРИТИЧНО для FIFO**: сплиты, конвертации, слияния, изменения количества акций |
| **Комиссии** | Встроены в секцию "Сделки" | Торговые комиссии + ADR Fees (отдельная категория для дивидендов ADR) |
| **NAV (чистая стоимость)** | `Чистая стоимость активов (NAV)` | Итоги портфеля |
| **Рыночная переоценка** | `Рыночная переоценка: отчет об эффективности` | П/У по инструментам |
| **Реализованная П/У** | `Реализованная и нереализованная П/У` | Детализация прибыли/убытков |

### 2.2 Формат секции "Сделки"

**Заголовок:**
```csv
Сделки,Header,DataDiscriminator,Класс актива,Валюта,Символ,Дата/Время,Количество,Цена транзакции,Цена закрытия,Выручка,Комиссия/плата,Базис,Реализованная П/У,Рыноч. переоценка П/У,Код
```

**Пример данных:**
```csv
Сделки,Data,Order,Акции,USD,CMCSA,"2024-02-20, 09:30:00",200,41.15,41.66,-8230,-1,8231,0,102,O
Сделки,Data,Order,Акции,USD,CMCSA,"2024-10-28, 11:51:28",-350,41.88,41.84,14658,-2.23,,-13942.25,713.52,14,C;P
```

**Ключевые поля:**
- `DataDiscriminator`: `Order` (сделка), `SubTotal` (итог по символу), `Total` (итог по классу)
- `Класс актива`: `Акции`, `Опционы на акции и индексы`, `Варранты`, `Forex`
- `Валюта`: `USD`, `CAD`, `AUD` и т.д.
- `Символ`: Тикер акции или полное название опциона (напр. `CMCSA 31JAN25 38 P`)
- `Дата/Время`: Формат `"YYYY-MM-DD, HH:MM:SS"`
- `Количество`: Положительное = покупка, отрицательное = продажа
- `Выручка`: Отрицательное = вывод денег (покупка), положительное = поступление (продажа)
- `Комиссия/плата`: Всегда отрицательное значение
- `Базис`: Базовая стоимость позиции (для FIFO расчетов)
- `Реализованная П/У`: Фактическая прибыль/убыток по закрытым позициям
- `Код`: Коды сделки:
  - `O` - открытие позиции
  - `C` - закрытие позиции
  - `A` - исполнение опциона (assignment)
  - `Ep` - истечение опциона
  - `P` - partial (частичное исполнение)
  - `IM` - внутренняя миграция

### 2.3 Формат секции "Дивиденды"

**Заголовок:**
```csv
Дивиденды,Header,Валюта,Дата,Описание,Сумма
```

**Пример данных:**
```csv
Дивиденды,Data,USD,2024-04-24,CMCSA(US20030N1019) Наличный дивиденд USD 0.31 на акцию (Обыкновенный дивиденд),62
Дивиденды,Data,CAD,2024-03-27,NEO(CA64046G1063) Наличный дивиденд CAD 0.10 на акцию (Обыкновенный дивиденд),140.7
```

**Парсинг описания:**
- Формат: `SYMBOL(ISIN) Наличный дивиденд [ВАЛЮТА] [СУММА_НА_АКЦИЮ] на акцию (Тип)`
- Пример: `CMCSA(US20030N1019)` → Символ: `CMCSA`, ISIN: `US20030N1019`

### 2.4 Формат секции "Удерживаемый налог"

**Заголовок:**
```csv
Удерживаемый налог,Header,Валюта,Дата,Описание,Сумма,Код
```

**Пример данных:**
```csv
Удерживаемый налог,Data,USD,2024-04-24,CMCSA(US20030N1019) Наличный дивиденд USD 0.31 на акцию - US Налог,-18.6,
Удерживаемый налог,Data,CAD,2024-03-27,NEO(CA64046G1063) Наличный дивиденд CAD 0.10 на акцию - CA Налог,-21.11,
```

**Особенности:**
- Налог всегда отрицательный
- Код страны в описании: `US Налог`, `CA Налог`, `BR Налог`
- Привязывается к дивиденду по дате и символу

### 2.5 Типы опционов в IB

**Формат названия опциона:**
```
SYMBOL DDMMMYY STRIKE [C/P]
```

**Примеры:**
- `CMCSA 31JAN25 38 P` → CMCSA, страйк 38, пут, истекает 31 января 2025
- `DQ 17JAN25 50 C` → DQ, страйк 50, колл, истекает 17 января 2025

**Коды исполнения опционов:**
- `A` (Assignment) - исполнение опциона (поставка акций)
- `Ep` (Expiration) - истечение опциона без исполнения
- `C;Ep` - закрытие позиции через истечение

---

## 3. Архитектура решения

**ПРИНЦИП: Максимальное переиспользование кода, минимум дублирования**

### 3.1 Новые модели Django

#### 3.1.1 Универсальная модель BrokerReport (РЕКОМЕНДУЕТСЯ)

**Вместо создания отдельных моделей, используем полиморфизм:**

```python
class BrokerReport(models.Model):
    """Универсальная модель для отчетов всех брокеров"""
    BROKER_TYPES = [
        ('ffg', 'Freedom Finance Global'),
        ('ib', 'Interactive Brokers'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="Пользователь")
    broker_type = models.CharField(
        max_length=10,
        choices=BROKER_TYPES,
        verbose_name="Тип брокера"
    )
    report_file = models.FileField(
        upload_to='broker_reports/%Y/%m/',
        verbose_name="Файл отчета"
    )
    year = models.IntegerField(
        help_text="Год отчета",
        db_index=True,
        verbose_name="Год отчета"
    )
    uploaded_at = models.DateTimeField(auto_now_add=True, verbose_name="Дата загрузки")
    original_filename = models.CharField(max_length=255, verbose_name="Оригинальное имя файла")

    # Дополнительные поля (опционально, зависят от брокера)
    account_number = models.CharField(max_length=50, blank=True, verbose_name="Номер счета")
    base_currency = models.CharField(max_length=3, default='USD', verbose_name="Базовая валюта")

    class Meta:
        verbose_name = "Брокерский отчет"
        verbose_name_plural = "Брокерские отчеты"
        unique_together = ('user', 'broker_type', 'original_filename', 'year')
        ordering = ['-uploaded_at']

    def __str__(self):
        broker_display = self.get_broker_type_display()
        return f"{broker_display} - {self.original_filename} ({self.year})"

    @property
    def file_extension(self):
        """Определяет расширение файла"""
        return 'xml' if self.broker_type == 'ffg' else 'csv'
```

**Миграция существующих данных:**
```python
# Скрипт миграции UploadedXMLFile -> BrokerReport
def migrate_ffg_files():
    for old_file in UploadedXMLFile.objects.all():
        BrokerReport.objects.create(
            user=old_file.user,
            broker_type='ffg',
            report_file=old_file.xml_file,
            year=old_file.year,
            uploaded_at=old_file.uploaded_at,
            original_filename=old_file.original_filename
        )
```

### 3.2 Унифицированная архитектура парсеров

**ПРИНЦИП: Общие функции выносим в shared модуль, специфичные - в отдельные парсеры**

#### 3.2.1 Новая структура файлов

```
reports_to_ndfl/
├── parsers/
│   ├── __init__.py
│   ├── base.py              # Базовые классы и общие функции
│   ├── ffg_parser.py        # Парсер FFG (рефакторинг FFG_ndfl.py)
│   └── ib_parser.py         # Парсер IB (новый)
├── utils/
│   ├── __init__.py
│   ├── fifo.py              # ОБЩАЯ FIFO логика для всех брокеров
│   ├── currency.py          # Работа с курсами ЦБ РФ (переиспользование)
│   └── formatters.py        # Форматирование данных для отображения
└── models.py
```

#### 3.2.2 Базовый абстрактный парсер

```python
# parsers/base.py
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Tuple

class BaseBrokerParser(ABC):
    """Базовый класс для всех парсеров брокеров"""

    def __init__(self, request, user, target_year):
        self.request = request
        self.user = user
        self.target_year = target_year
        self.processing_error = False

    @abstractmethod
    def parse_trades(self, file_path) -> List[Dict]:
        """Парсит сделки из файла в унифицированный формат"""
        pass

    @abstractmethod
    def parse_dividends(self, file_path) -> List[Dict]:
        """Парсит дивиденды"""
        pass

    @abstractmethod
    def parse_commissions(self, file_path) -> Dict:
        """Парсит комиссии"""
        pass

    def process(self, files: List[str]) -> Tuple:
        """
        ЕДИНАЯ точка входа для обработки отчетов

        Возвращает унифицированный формат:
        (
            final_instrument_event_history,
            all_dividend_events,
            total_dividends_rub,
            total_sales_profit_rub,
            processing_error_flag,
            dividend_commissions,
            other_commissions,
            total_commissions_rub
        )
        """
        # 1. Парсинг всех файлов
        all_trades = []
        all_dividends = []
        all_commissions = {}

        for file_path in files:
            all_trades.extend(self.parse_trades(file_path))
            all_dividends.extend(self.parse_dividends(file_path))
            # ... и т.д.

        # 2. Нормализация данных (ОБЩАЯ функция)
        normalized_trades = self._normalize_trades(all_trades)

        # 3. FIFO расчет (ОБЩАЯ функция из utils.fifo)
        from ..utils.fifo import process_fifo
        fifo_result = process_fifo(normalized_trades)

        # 4. Форматирование для отображения (ОБЩАЯ функция)
        from ..utils.formatters import format_for_display
        display_data = format_for_display(fifo_result, all_dividends)

        return display_data

    def _normalize_trades(self, trades: List[Dict]) -> List[Dict]:
        """
        ОБЩАЯ функция нормализации сделок в единый формат

        Унифицированный формат trade_dict:
        {
            'trade_id': str,
            'datetime_obj': datetime,
            'operation': 'buy' | 'sell',
            'symbol': str,
            'isin': str (опционально),
            'quantity': Decimal,
            'price': Decimal,
            'commission': Decimal,
            'currency': str,
            'cbr_rate': Decimal,
            'broker_type': 'ffg' | 'ib',
            'is_option': bool,
            'option_details': dict (если опцион),
        }
        """
        # Общая логика нормализации
        pass
```

#### 3.2.3 Парсер IB (наследуется от базового)

```python
# parsers/ib_parser.py
from .base import BaseBrokerParser
import csv
from decimal import Decimal

class IBParser(BaseBrokerParser):
    """Парсер для Interactive Brokers CSV отчетов"""

    def parse_trades(self, file_path) -> List[Dict]:
        """Парсит сделки из IB CSV"""
        sections = self._parse_csv_sections(file_path)
        trades_section = sections.get('Сделки', {})

        trades = []
        for row in trades_section.get('data', []):
            if row[0] != 'Order':  # Фильтр DataDiscriminator
                continue

            # Конвертация в ОБЩИЙ формат
            trade = {
                'trade_id': self._generate_trade_id(row),
                'datetime_obj': self._parse_datetime(row[5]),  # Дата/Время
                'operation': 'buy' if Decimal(row[6]) > 0 else 'sell',
                'symbol': row[4],  # Символ
                'quantity': abs(Decimal(row[6].replace(',', ''))),
                'price': Decimal(row[7]) if row[7] else Decimal(0),
                'commission': abs(Decimal(row[10])) if row[10] else Decimal(0),
                'currency': row[3],
                'broker_type': 'ib',
                'is_option': row[2] == 'Опционы на акции и индексы',
                # ... остальные поля
            }
            trades.append(trade)

        return trades

    def _parse_csv_sections(self, file_path) -> Dict:
        """Разбивает CSV на секции (специфично для IB)"""
        # Логика парсинга CSV
        pass

    # ... остальные методы специфичные для IB
```

#### 3.2.4 Рефакторинг FFG парсера

```python
# parsers/ffg_parser.py
from .base import BaseBrokerParser
import xml.etree.ElementTree as ET

class FFGParser(BaseBrokerParser):
    """Парсер для Freedom Finance Global XML отчетов"""

    def parse_trades(self, file_path) -> List[Dict]:
        """Парсит сделки из FFG XML"""
        tree = ET.parse(file_path)
        root = tree.getroot()

        trades = []
        for node in root.findall('.//trades/detailed/node'):
            # Конвертация в ОБЩИЙ формат (тот же что у IB!)
            trade = {
                'trade_id': node.findtext('trade_id'),
                'datetime_obj': self._parse_datetime(node.findtext('date')),
                'operation': node.findtext('operation').lower(),
                'symbol': node.findtext('instr_nm'),
                'isin': node.findtext('isin'),
                'quantity': Decimal(node.findtext('q')),
                'price': Decimal(node.findtext('p')),
                'commission': Decimal(node.findtext('commission')),
                'currency': node.findtext('curr_c'),
                'broker_type': 'ffg',
                # ... остальные поля
            }
            trades.append(trade)

        return trades

    # ... остальные методы
```

### 3.3 Структура папок

```
NDFL/
├── media/
│   ├── xml_files/          # Отчеты FFG (существующая)
│   └── broker_reports/     # Единое хранилище отчетов (FFG+IB)
├── reports_to_ndfl/
│   ├── parsers/            # Базовый парсер + FFG/IB
│   ├── utils/              # FIFO, валюты, форматирование
│   ├── models.py           # Модели (BrokerReport)
│   ├── views.py            # Views (модифицировать для IB)
│   ├── admin.py            # Admin (добавить IB модель)
│   └── templates/
│       └── reports_to_ndfl/
│           └── upload.html  # UI (добавить выбор брокера)
```


### 3.4 Общий модуль FIFO (НЕТ дублирования!)

```python
# utils/fifo.py
"""
ЕДИНАЯ FIFO логика для всех брокеров
Работает с унифицированным форматом данных
"""
from collections import deque, defaultdict
from decimal import Decimal

def process_fifo(normalized_trades: List[Dict]) -> Dict:
    """
    УНИВЕРСАЛЬНАЯ FIFO обработка для FFG и IB

    Принимает: унифицированный список сделок
    Возвращает: {
        'instrument_history': Dict[symbol, List[events]],
        'total_profit_rub': Decimal,
        'short_sales': List[...],
        # ...
    }
    """
    buy_lots = defaultdict(deque)
    short_sales = defaultdict(deque)

    # Сортировка по дате (общая для всех)
    sorted_trades = sorted(normalized_trades, key=lambda x: x['datetime_obj'])

    for trade in sorted_trades:
        if trade['operation'] == 'buy':
            _process_buy(trade, buy_lots)
        else:  # sell
            _process_sell(trade, buy_lots, short_sales)

    return {
        'buy_lots': buy_lots,
        'short_sales': short_sales,
        # ...
    }

def _process_buy(trade, buy_lots):
    """Обработка покупки (общая логика)"""
    symbol = trade['symbol']
    buy_lots[symbol].append({
        'quantity_remaining': trade['quantity'],
        'price': trade['price'],
        'date': trade['datetime_obj'],
        'cost_rub': trade['quantity'] * trade['price'] * trade['cbr_rate'],
        'trade_id': trade['trade_id'],
        'broker': trade['broker_type']  # Для отслеживания источника
    })

def _process_sell(trade, buy_lots, short_sales):
    """Обработка продажи с FIFO (общая логика)"""
    # Единая логика для FFG и IB
    pass
```

### 3.5 Модификация Views (МИНИМАЛЬНЫЕ изменения!)

**Файл:** `reports_to_ndfl/views.py`

**Изменения:**

```python
def display_trades(request):
    """
    ЕДИНАЯ функция отображения для ВСЕХ брокеров
    НЕТ разветвления на FFG/IB - парсеры возвращают одинаковый формат!
    """
    user = request.user
    target_year = int(request.GET.get('year', datetime.now().year))

    # Получаем ВСЕ файлы пользователя за год
    ffg_files = BrokerReport.objects.filter(
        user=user, year=target_year, broker_type='ffg'
    )
    ib_files = BrokerReport.objects.filter(
        user=user, year=target_year, broker_type='ib'
    )

    # Обрабатываем через соответствующие парсеры
    from .parsers.ffg_parser import FFGParser
    from .parsers.ib_parser import IBParser

    results = []

    if ffg_files.exists():
        parser = FFGParser(request, user, target_year)
        file_paths = [f.report_file.path for f in ffg_files]
        results.append(parser.process(file_paths))

    if ib_files.exists():
        parser = IBParser(request, user, target_year)
        file_paths = [f.report_file.path for f in ib_files]
        results.append(parser.process(file_paths))

    # Объединяем результаты (если есть оба брокера)
    if len(results) > 1:
        final_result = _merge_broker_results(results)
    else:
        final_result = results[0] if results else _empty_result()

    # ОДИНАКОВЫЙ шаблон для всех!
    return render(request, 'reports_to_ndfl/trades_display.html', {
        'data': final_result,
        'year': target_year,
        # НЕТ флага broker_type - отображение универсальное!
    })

def _merge_broker_results(results: List[Tuple]) -> Tuple:
    """
    Объединяет результаты от разных брокеров
    Сделки сортируются по дате независимо от источника
    """
    # Логика слияния
    pass
```

### 3.6 Модификация шаблонов (УНИВЕРСАЛЬНОЕ отображение!)

**Файл:** `reports_to_ndfl/templates/reports_to_ndfl/upload.html`

**Минимальные изменения:**

```html
<!-- Выбор брокера при загрузке -->
<div class="broker-selection">
  <label>Тип брокера:</label>
  <select name="broker_type" id="broker-type-select">
    <option value="ffg">Freedom Finance Global (XML)</option>
    <option value="ib">Interactive Brokers (CSV)</option>
  </select>
</div>

<script>
// Динамическая подсказка формата
document.getElementById('broker-type-select').addEventListener('change', function() {
  const hint = this.value === 'ffg'
    ? 'Загрузите XML отчеты от Freedom Finance'
    : 'Загрузите CSV отчеты (Activity Statement) от Interactive Brokers';
  document.getElementById('format-hint').textContent = hint;
});
</script>

<!-- Список загруженных файлов - ОДИНАКОВЫЙ для всех -->
<table>
  <thead>
    <tr>
      <th>Брокер</th>
      <th>Файл</th>
      <th>Год</th>
      <th>Дата загрузки</th>
    </tr>
  </thead>
  <tbody>
    {% for report in user_reports %}
    <tr>
      <td>
        <span class="broker-badge broker-{{ report.broker_type }}">
          {{ report.get_broker_type_display }}
        </span>
      </td>
      <td>{{ report.original_filename }}</td>
      <td>{{ report.year }}</td>
      <td>{{ report.uploaded_at|date:"d.m.Y H:i" }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
```

**Файл:** `reports_to_ndfl/templates/reports_to_ndfl/trades_display.html`

**НЕТ ИЗМЕНЕНИЙ! Шаблон уже универсальный:**

```html
<!-- Отображение ОДИНАКОВОЕ для FFG и IB -->
<table class="trades-table">
  <thead>
    <tr>
      <th>Дата</th>
      <th>Инструмент</th>
      <th>Операция</th>
      <th>Количество</th>
      <th>Цена</th>
      <th>Сумма (RUB)</th>
      <th>П/У (RUB)</th>
      <!-- Источник данных (опционально) -->
      <th>Брокер</th>
    </tr>
  </thead>
  <tbody>
    {% for event in data.events %}
    <tr class="trade-row" style="{% if event.link_colors %}background: {{ event.link_colors.0 }};{% endif %}">
      <td>{{ event.date|date:"d.m.Y" }}</td>
      <td>{{ event.symbol }}</td>
      <td>{{ event.operation }}</td>
      <td>{{ event.quantity }}</td>
      <td>{{ event.price }}</td>
      <td>{{ event.amount_rub|floatformat:2 }}</td>
      <td class="{% if event.profit > 0 %}profit{% else %}loss{% endif %}">
        {{ event.profit|floatformat:2 }}
      </td>
      <!-- Бейдж брокера (маленькая иконка) -->
      <td><span class="broker-icon-{{ event.broker }}"></span></td>
    </tr>
    {% endfor %}
  </tbody>
</table>

<!-- Итоги - ОДИНАКОВЫЕ для всех -->
<div class="summary">
  <h3>Итоги за {{ year }}</h3>
  <p>Общая прибыль/убыток: {{ data.total_profit_rub|floatformat:2 }} RUB</p>
  <p>Дивиденды: {{ data.total_dividends_rub|floatformat:2 }} RUB</p>
  <p>Комиссии: {{ data.total_commissions_rub|floatformat:2 }} RUB</p>
</div>
```

**CSS для брокерских бейджей:**

```css
.broker-badge {
  padding: 2px 8px;
  border-radius: 3px;
  font-size: 0.85em;
}

.broker-ffg {
  background: #4CAF50;
  color: white;
}

.broker-ib {
  background: #2196F3;
  color: white;
}

.broker-icon-ffg::before { content: "FFG"; }
.broker-icon-ib::before { content: "IB"; }
```

---

## 4. Маппинг данных IB → FFG формат

### 4.1 Унифицированная структура trade_data_dict

| Поле (унифицированное) | Источник в IB CSV | Источник в FFG XML |
|------------------------|-------------------|-------------------|
| `trade_id` | Генерируется: `{Symbol}_{DateTime}_{Quantity}` | `<trade_id>` |
| `date` | `Дата/Время` | `<date>` |
| `datetime_obj` | Парсинг `Дата/Время` | Парсинг `<date>` |
| `operation` | `Количество` (>0='buy', <0='sell') | `<operation>` |
| `instr_nm` | `Символ` | `<instr_nm>` |
| `isin` | Извлечение из описания (для дивидендов) | `<isin>` |
| `p` | `Цена транзакции` | `<p>` |
| `q` | `abs(Количество)` | `<q>` |
| `summ` | `abs(Выручка)` | `<summ>` |
| `commission` | `abs(Комиссия/плата)` | `<commission>` |
| `curr_c` | `Валюта` | `<curr_c>` |
| `cbr_rate_decimal` | Запрос к ЦБ РФ API | Запрос к ЦБ РФ API |
| `is_option_delivery` | `Код` содержит 'A' | Проверка `trade_nb` |
| `class_type` | `Класс актива` | `<instr_type>` |
| `trade_code` | `Код` | - |

### 4.2 Маппинг классов активов

| IB "Класс актива" | FFG `<instr_type>` | Унифицированный тип |
|-------------------|-------------------|---------------------|
| Акции | 1 | `stock` |
| Опционы на акции и индексы | 4 | `option` |
| Варранты | - | `warrant` |
| Forex | 6 | `forex` |

### 4.3 Валютные курсы

**Проблема:** IB не предоставляет курсы ЦБ РФ, используются рыночные курсы на дату сделки.

**Решение:**
- Использовать существующий модуль `currency_CBRF` для получения курсов ЦБ РФ
- Для каждой сделки в валюте запрашивать курс на дату операции
- Кэшировать курсы для оптимизации

**Функция (переиспользуется из FFG):**
```python
from currency_CBRF.views import get_exchange_rate_for_date
```

---

## 5. Особенности IB требующие внимания

### 5.1 Опционы

**Отличия от FFG:**
- IB включает стоимость опциона в `Базис` сделки при исполнении
- FFG требует ручной привязки опционной покупки к поставке

**Решение:**
- Парсить код сделки `A` (assignment) как исполнение опциона
- Использовать поле `Базис` для FIFO вместо ручного расчета
- Сохранить историю опционов для отчетности

### 5.2 Корпоративные действия

**IB не включает в CSV:**
- Сплиты акций
- Конвертации
- Изменения тикеров

**Решение (фаза 2):**
- Добавить ручной ввод корпоративных событий через админку
- Или парсить дополнительные отчеты IB (Corporate Actions report)

### 5.3 Частичное исполнение сделок

**IB особенность:**
- Одна сделка может исполняться частями (код `P`)
- Каждая часть — отдельная строка с одинаковым временем

**Решение:**
- Генерировать уникальный `trade_id` с добавлением счетчика
- Агрегировать части при отображении (опционально)

### 5.4 Короткие продажи (шорты)

**IB явно маркирует:**
- В отчете "Рыночная переоценка" есть "Текущие короткие позиции"
- Количество отрицательное после всех покупок

**Решение:**
- Переиспользовать логику `pending_short_sales` из FFG парсера
- Адаптировать под структуру IB

### 5.5 Дивиденды в натуральной форме

**IB строки:**
```
NEO(CA64046G1063) Выплата в качестве дивиденда (Обыкновенный дивиденд),9.3
```

**Решение:**
- Парсить как обычный дивиденд
- Тип "Выплата в качестве дивиденда" = DRIP (Dividend Reinvestment)

### 5.6 Проценты (ПРОЧИЙ ДОХОД после дивидендов)

**IB учитывает:**
- Дебетовые проценты (за использование маржи) - отрицательные
- Кредитовые проценты (за остаток на счете) - положительные

**Категоризация для налоговой:**
- ⚠️ **НЕ включать в комиссии!**
- ✅ **ПРОЧИЙ ДОХОД** - отображается ПОСЛЕ дивидендов в итоговом отчете
- Кредитовые проценты = налогооблагаемый доход
- Дебетовые проценты = расход (НЕ уменьшает налогооблагаемую базу для НДФЛ)

**Решение:**
- Парсить секцию "Процент" отдельно
- Создать категорию `other_income` (прочий доход)
- Отображать ПОСЛЕ дивидендов, ДО комиссий
- Конвертировать в RUB по курсу ЦБ РФ на дату начисления

**Пример:**
```
Процент,Data,USD,2024-12-31,USD Credit Interest for Dec-2024,12.45,
```
→ Прочий доход: 12.45 USD × курс ЦБ = XXX RUB

### 5.7 Переводы (КРИТИЧНО для FIFO!)

**Проблема:**
Когда акции покидают/поступают на счет БЕЗ сделки купли-продажи (межброкерские переводы, депозитарные операции), FIFO ломается, если это не учесть.

**Примеры:**
- Перевод 100 акций AAPL с IB на другого брокера
- Поступление 50 акций GOOGL из другого депозитария

**Влияние на FIFO:**
- **Исходящий перевод** = уменьшение позиции БЕЗ продажи
  - Нужно списать из `buy_lots` по FIFO
  - НЕ считать как продажу (нет прибыли/убытка)
  - НЕ включать в налоговую базу
- **Входящий перевод** = увеличение позиции БЕЗ покупки
  - Добавить в `buy_lots` с cost basis из другого брокера
  - Использовать дату поступления

**Секция в IB CSV:**
```csv
Переводы,Header,DataDiscriminator,Класс актива,Символ,Дата/Время,Количество,Цена транзакции,Цена закрытия,Выручка,Код
Переводы,Data,Order,Акции,AAPL,"2024-06-15, 10:00:00",-100,150.00,150.50,0,ACATS
```

**Решение:**
- Парсить секцию "Переводы" в `IBParser`
- Обработать в FIFO как специальный тип операции `transfer_out` / `transfer_in`
- Не считать P/L для переводов
- Отображать в истории инструмента с меткой "Перевод"

### 5.8 Корпоративные действия (КРИТИЧНО для FIFO!)

**Проблема:**
Сплиты, конвертации, слияния изменяют количество/тикер акций в позиции. Если не учесть, FIFO покажет неверные остатки.

**Примеры:**
- **Сплит 1:10** - было 100 акций → стало 1000 акций
- **Обратный сплит 10:1** - было 1000 акций → стало 100 акций
- **Смена тикера** - GOOGL → GOOG (класс акций)
- **Конвертация** - конвертируемые привилегированные → обыкновенные

**Влияние на FIFO:**
- Нужно пересчитать `quantity_remaining` во всех `buy_lots` для символа
- Нужно пересчитать `price` пропорционально сплиту
- `cost_rub` остается неизменным (база не меняется)

**Секция в IB CSV:**
```csv
Корпоративные действия,Header,Класс актива,Символ,Дата/Время,Количество,Описание,Выручка,Код
Корпоративные действия,Data,Акции,AAPL,"2024-08-01, 09:30:00",900,AAPL(US0378331005) Split 1 for 10 (AAPL, APPLE INC, US0378331005),0,SO
```

**Решение Фаза 1 (MVP):**
- ⚠️ **Предупреждать пользователя** если найдена секция "Корпоративные действия"
- Требовать ручной корректировки или пропустить обработку
- Логировать корпоративные действия в отдельную таблицу

**Решение Фаза 2 (полная поддержка):**
- Парсить секцию "Корпоративные действия"
- Применять коэффициенты сплита к `buy_lots`
- Поддержать коды: `SO` (split ordinary), `TC` (ticker change), `TO` (takeover)

### 5.9 ADR Fees (отдельная категория комиссий)

**Проблема:**
Для депозитарных расписок (ADR) брокер удерживает комиссию за обслуживание. Это НЕ торговая комиссия и НЕ налог.

**Пример:**
```csv
Дивиденды,Data,USD,2024-06-15,VALE(US91912E1055) Cash Dividend USD 0.50 per Share (Ordinary Dividend),125.00,
Дивиденды,Data,USD,2024-06-15,VALE(US91912E1055) ADR Fee USD 0.02 per Share,-5.00,
```

**Категоризация:**
- ADR Fee = комиссия, связанная с дивидендами
- Отображать отдельно от торговых комиссий
- Включать в `dividend_commissions` (не в `other_commissions`)

**Решение:**
- Парсить ADR Fee из секции "Дивиденды" (не "Сделки")
- Привязывать к соответствующему дивиденду по символу и дате
- Уменьшать итоговую сумму дивиденда в отчете

---

## 6. План разработки (поэтапный с МИНИМУМ дублирования!)

### Фаза 0: Рефакторинг FFG кода (приоритет: КРИТИЧЕСКИЙ!)

**⚠️ Выполняется ПЕРЕД добавлением IB для избежания дублирования!**

**Задачи:**
1. **Создать структуру папок:**
   ```
   reports_to_ndfl/
   ├── parsers/
   │   ├── __init__.py
   │   └── base.py
   ├── utils/
   │   ├── __init__.py
   │   ├── fifo.py
   │   ├── currency.py
   │   └── formatters.py
   ```

2. **Извлечь ОБЩУЮ FIFO логику из `FFG_ndfl.py` → `utils/fifo.py`:**
   - Функция `process_fifo()` (универсальная)
   - Функции `_process_buy()`, `_process_sell()` (общие)
   - Обработка шортов (общая)

3. **Извлечь работу с валютами → `utils/currency.py`:**
   - Функция `get_cbr_rate_for_date()` (переиспользование существующей)
   - Кэширование курсов

4. **Создать `parsers/base.py`:**
   - Абстрактный класс `BaseBrokerParser`
   - Метод `process()` (единая точка входа)
   - Метод `_normalize_trades()` (общий)

5. **Рефакторинг `FFG_ndfl.py` → `parsers/ffg_parser.py`:**
   - Наследование от `BaseBrokerParser`
   - Использование `utils.fifo` вместо локальной логики
   - Возврат УНИФИЦИРОВАННОГО формата данных

6. **Обновить `views.py`:**
   - Использовать `FFGParser` вместо прямого вызова функций
   - Подготовка к добавлению других парсеров

**Результат:**
- ✅ Весь общий код вынесен в utils
- ✅ FFG парсер готов к расширению
- ✅ НЕТ дублирования FIFO логики
- ✅ Код готов для добавления IB без копипасты

**Время:** 4-6 часов

---

### Фаза 1: Базовая инфраструктура IB (приоритет: высокий)

**Задачи:**
1. ✅ Создать папку `media/broker_reports/`
2. **Создать универсальную модель `BrokerReport`** (НЕ отдельную для IB!):
   - Поле `broker_type` с choices
   - Миграция данных из `UploadedXMLFile`
3. Добавить миграцию БД: `python manage.py makemigrations && python manage.py migrate`
4. Обновить `admin.py` для новой модели
5. Обновить `views.py` для работы с `BrokerReport`

**Результат:** Универсальная модель для всех брокеров.

**Время:** 2-3 часа

---

### Фаза 2: Парсер IB (использует ОБЩИЙ код!) (приоритет: высокий)

**Задачи:**
1. **Создать `parsers/ib_parser.py`:**
   - Класс `IBParser(BaseBrokerParser)`  ← наследование!
   - Метод `parse_trades()` - специфичен для IB CSV
   - Метод `parse_transfers()` - **КРИТИЧНО**: парсинг секции "Переводы"
   - Метод `parse_corporate_actions()` - **КРИТИЧНО**: парсинг корпоративных действий (Фаза 1: предупреждение, Фаза 2: обработка)
   - Метод `_parse_csv_sections()` - парсинг CSV
   - Метод `_parse_option_name()` - парсинг опционов IB
   - Метод `_generate_trade_id()` - генерация уникальных ID с валидацией дубликатов

2. **ПЕРЕИСПОЛЬЗОВАТЬ существующий код:**
   - `utils.currency.get_cbr_rate_for_date()` ← уже есть!
   - `utils.fifo.process_fifo()` ← из Фазы 0! (нужно расширить для переводов)
   - `parsers.base.BaseBrokerParser.process()` ← общий метод!

3. **Минимальный специфичный код:**
   - Только парсинг CSV структуры
   - Только конвертация в унифицированный формат
   - Всё остальное - ПЕРЕИСПОЛЬЗОВАНИЕ!

4. **Валидация trade_id:**
   - Проверка уникальности `trade_id` в пределах всех загруженных файлов
   - Предупреждение при обнаружении дубликатов
   - Добавление счетчика к ID при коллизиях

5. Тестирование на одном файле (2024)

**Результат:** Парсер IB с МИНИМУМ нового кода (только специфика CSV).

**Время:** 4-5 часов (вместо 6-8 без рефакторинга!)

---

### Фаза 3: FIFO с поддержкой переводов и корпоративных действий (приоритет: ВЫСОКИЙ)

**⚠️ FIFO логика УЖЕ ОБЩАЯ после Фазы 0, но нужны расширения!**

**Задачи:**
1. **Расширить `utils/fifo.py` для новых типов операций:**
   - `transfer_out` - исходящий перевод акций (списание из buy_lots БЕЗ P/L)
   - `transfer_in` - входящий перевод акций (добавление в buy_lots)
   - `corporate_action` - применение коэффициентов сплита/конвертации

2. **Добавить обработку IB-специфичных кодов:**
   - Код `A` (исполнение опциона) - добавить в `utils/fifo.py` как общую логику
   - Код `P` (частичное исполнение) - обработать в `ib_parser.py`
   - Код `ACATS` (межброкерский перевод) - обработать как `transfer_out/in`
   - Код `SO` (split ordinary) - применить к buy_lots

3. **Функция применения сплита:**
   ```python
   def apply_stock_split(buy_lots, symbol, split_ratio, split_date):
       """
       Применяет коэффициент сплита ко всем позициям символа

       split_ratio: Decimal (например, 10.0 для сплита 1:10)
       Пересчитывает quantity, НЕ меняет cost_rub
       """
   ```

4. **Обработка переводов в FIFO:**
   - Исходящий перевод: списать из `buy_lots` по FIFO, но P/L = 0
   - Входящий перевод: добавить в `buy_lots` с указанной датой и cost basis

5. Тестирование FIFO на IB данных (включая файлы с переводами/сплитами)

**Результат:** FIFO работает для IB с учетом всех корпоративных действий!

**Время:** 3-4 часа (вместо 4-6 благодаря общей базе!)

---

### Фаза 4: Дивиденды, налоги, проценты (приоритет: средний)

**Задачи:**
1. **Добавить в `IBParser`:**
   - Метод `parse_dividends()` (специфичен для CSV)
   - Метод `parse_withholding_tax()` (специфичен для CSV)
   - Метод `parse_interest_income()` - **НОВОЕ**: парсинг секции "Процент"
   - Функция `_extract_isin_from_description()` (regex парсинг)
   - Функция `_parse_adr_fees()` - выделение ADR Fees из дивидендов

2. **ПЕРЕИСПОЛЬЗОВАТЬ:**
   - `utils.currency` для конвертации дивидендов и процентов ← уже есть!
   - Структуру `dividend_tax_details` из FFG ← уже есть!

3. **Категоризация доходов:**
   - **Дивиденды** → `dividend_events` (первая категория)
   - **Проценты** → `other_income` (ПОСЛЕ дивидендов, отдельная категория "Прочий доход")
   - **ADR Fees** → `dividend_commissions` (связаны с дивидендами)
   - **Торговые комиссии** → `other_commissions` (отдельная категория)

4. **Структура возвращаемых данных (расширить):**
   ```python
   return (
       final_instrument_event_history,
       all_dividend_events,
       total_dividends_rub,
       other_income_events,  # НОВОЕ: проценты
       total_other_income_rub,  # НОВОЕ
       total_sales_profit_rub,
       processing_error_flag,
       dividend_commissions,  # включая ADR Fees
       other_commissions,
       total_commissions_rub
   )
   ```

5. **Обновить шаблон отображения:**
   - Секция "Дивиденды"
   - Секция "Прочий доход" (ПОСЛЕ дивидендов)
   - Секция "Комиссии" (ПОСЛЕ прочего дохода)

**Результат:** Дивиденды, проценты, налоги IB в правильной категоризации.

**Время:** 3-4 часа

---

### Фаза 5: UI уже готов! (приоритет: средний)

**⚠️ UI УНИВЕРСАЛЬНЫЙ - минимальные изменения!**

**Задачи:**
1. **Модифицировать `upload.html`:**
   - Добавить `<select>` выбора брокера (5 строк кода)
   - JavaScript для подсказки формата (10 строк)
   - Стили для бейджей брокеров (CSS)

2. **`display_trades()` УЖЕ РАБОТАЕТ:**
   - Парсеры возвращают ОДИНАКОВЫЙ формат
   - Шаблон `trades_display.html` НЕ МЕНЯЕТСЯ!
   - Только добавить бейдж брокера в строку таблицы

3. **Функция слияния результатов:**
   - `_merge_broker_results()` - если есть FFG + IB одновременно

**Результат:** UI показывает FFG и IB одинаково!

**Время:** 1-2 часа (вместо 3-4!)

---

### Фаза 6: Дополнительные фичи (приоритет: низкий)

**Задачи:**
1. Парсинг процентов по марже (IB-специфично)
2. Обработка варрантов (добавить в `IBParser`)
3. **Цветовая маркировка:**
   - ПЕРЕИСПОЛЬЗОВАТЬ логику из FFG! ← уже есть в `utils/formatters.py`
   - Работает автоматически для IB!

**Результат:** Полная функциональность IB.

**Время:** 2-3 часа

---

### Фаза 7: Тестирование (приоритет: высокий)

**Задачи:**
1. Тестирование на всех годах (2019-2025)
2. Unit-тесты:
   - `IBParser.parse_trades()`
   - `IBParser._parse_option_name()`
   - `utils.fifo.process_fifo()` (для обоих брокеров!)
3. Интеграционные тесты:
   - FFG + IB одновременно
   - Слияние результатов

**Результат:** Стабильная система.

**Время:** 3-4 часа

---

## ИТОГО по времени:

### БЕЗ рефакторинга (старый подход):
- Фазы 1-7: ~20-26 часов
- **С учетом аудита (переводы, проценты, валидация):** ~28-35 часов
- **Дублирование кода:** ВЫСОКОЕ
- **Сложность поддержки:** ВЫСОКАЯ
- **Риск ошибок:** ВЫСОКИЙ (копипаста FIFO логики)

### С рефакторингом (НОВЫЙ подход) + критические фичи из аудита:
- Фаза 0 (рефакторинг): 4-6 часов
- Фазы 1-7 (IB с учетом аудита): 18-25 часов
- **ИТОГО MVP: 22-31 часа**
- **Дублирование кода:** МИНИМАЛЬНОЕ
- **Сложность поддержки:** НИЗКАЯ
- **Готовность к новым брокерам:** ВЫСОКАЯ
- **Полнота реализации:** Все критические фичи включены

### Что добавилось после аудита:
- ✅ Секция "Переводы" (КРИТИЧНО для FIFO) - +1-2 часа
- ✅ Проценты как "Прочий доход" (отдельная категория) - +1 час
- ✅ ADR Fees в комиссиях по дивидендам - +0.5 часа
- ✅ Валидация trade_id и обработка ошибок - +1 час
- ✅ Предупреждения о корпоративных действиях - +0.5 часа
- **ИТОГО:** +4-5 часов на критические фичи

### Экономия с рефакторингом:
- ⏱️ **Время:** ~6-9 часов (благодаря переиспользованию FIFO/currency/formatters)
- 📦 **Размер кода:** -50% дублирования
- 🔧 **Поддержка:** Изменения в общей логике применяются ко ВСЕМ брокерам
- 🚀 **Масштабируемость:** Добавление 3-го брокера займет ~8-10 часов (не 25-30!)
- ✅ **Надежность:** Единая FIFO логика = меньше багов

### Фаза 8 (опционально - полная поддержка корп. действий):
- Автоматическое применение сплитов/конвертаций: +4-6 часов
- **ОБЩИЙ ИТОГО (полный функционал):** 26-37 часов

---

## 7. Ключевые технические детали

### 7.1 Парсинг CSV с BOM

```python
import csv
import codecs

def _parse_ib_csv_file(file_path):
    """Парсит CSV отчет IB с учетом UTF-8 BOM"""
    sections = {}
    current_section = None

    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or not row[0].strip():
                continue

            section_name = row[0].strip()

            # Новая секция
            if row[1] == 'Header':
                current_section = section_name
                sections[current_section] = {'header': row[2:], 'data': []}

            # Данные секции
            elif row[1] == 'Data' and current_section:
                sections[current_section]['data'].append(row[2:])

    return sections
```

### 7.2 Парсинг названия опциона

```python
import re
from datetime import datetime
from decimal import Decimal

def _parse_option_name(option_name):
    """
    Парсит название опциона IB

    Формат: "SYMBOL DDMMMYY STRIKE [C/P]"
    Пример: "CMCSA 31JAN25 38 P"

    Возвращает:
    {
        'symbol': 'CMCSA',
        'expiry_date': date(2025, 1, 31),
        'option_type': 'P',
        'strike': Decimal('38')
    }
    """
    pattern = r'^([A-Z]+)\s+(\d{2})([A-Z]{3})(\d{2})\s+([\d.]+)\s+([CP])$'
    match = re.match(pattern, option_name.strip())

    if not match:
        return None

    symbol, day, month_str, year_short, strike_str, opt_type = match.groups()

    # Маппинг месяцев
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }

    month = month_map[month_str]
    year = 2000 + int(year_short)  # 25 -> 2025
    day = int(day)

    expiry_date = datetime(year, month, day).date()
    strike = Decimal(strike_str)

    return {
        'symbol': symbol,
        'expiry_date': expiry_date,
        'option_type': opt_type,
        'strike': strike
    }
```

### 7.3 Генерация trade_id для IB

```python
def _generate_ib_trade_id(row_data, row_index):
    """
    Генерирует уникальный trade_id для сделки IB

    Формат: IB_{Symbol}_{DateTime}_{Index}
    Пример: IB_CMCSA_20240220093000_1
    """
    symbol = row_data['Символ']
    date_time_str = row_data['Дата/Время'].replace(', ', '_').replace(':', '').replace('-', '')

    return f"IB_{symbol}_{date_time_str}_{row_index}"
```

### 7.4 Извлечение ISIN из описания дивиденда

```python
def _extract_isin_from_dividend_description(description):
    """
    Извлекает ISIN из описания дивиденда

    Формат: "SYMBOL(ISIN) Наличный дивиденд ..."
    Пример: "CMCSA(US20030N1019) Наличный дивиденд USD 0.31 на акцию"

    Возвращает: ('CMCSA', 'US20030N1019')
    """
    pattern = r'^([A-Z0-9.]+)\(([A-Z0-9]{12})\)'
    match = re.match(pattern, description.strip())

    if match:
        symbol, isin = match.groups()
        return symbol, isin

    return None, None
```

### 7.5 Определение операции (buy/sell) из количества

```python
def _determine_operation_from_quantity(quantity_str):
    """
    Определяет тип операции из количества

    Положительное количество = покупка
    Отрицательное количество = продажа

    Возвращает: ('buy'|'sell', abs_quantity)
    """
    # Удаляем запятые из чисел (IB использует "10,000")
    quantity_clean = quantity_str.replace(',', '')
    quantity = Decimal(quantity_clean)

    if quantity > 0:
        return 'buy', quantity
    else:
        return 'sell', abs(quantity)
```

---

## 8. Риски и ограничения

### 8.1 Технические риски

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Изменение формата CSV в IB | Средняя | Высокое | Версионирование парсера, валидация структуры секций |
| Отсутствие курсов ЦБ РФ для даты | Низкая | Среднее | Использование ближайшей даты с алиасом |
| Некорректный FIFO для сложных случаев | Средняя | Высокое | Unit-тесты, ручная проверка, обработка переводов/сплитов |
| Несовпадение итогов с IB отчетом | Средняя | Среднее | Разные методы округления, разные курсы (IB vs ЦБ РФ) |
| Коллизии trade_id при нескольких файлах | Средняя | Высокое | Валидация уникальности, счетчик для дубликатов |
| Необработанные корпоративные действия | Высокая | Критическое | **Фаза 1**: предупреждение + ручная корректировка; **Фаза 2**: автоматическая обработка |
| Пропущенные переводы акций | Средняя | Критическое | **Обязательная** обработка секции "Переводы" |
| Дробные акции (fractional shares) | Средняя | Среднее | Поддержка Decimal для quantity, округление в отчетах |
| Несколько валют для одного символа | Низкая | Среднее | Конвертация каждой операции отдельно, группировка по символу |

### 8.2 Функциональные ограничения

**Что НЕ будет поддерживаться в MVP (Фаза 1):**
1. Фьючерсы и опционы на фьючерсы
2. Бонды и облигации
3. CFD (Contract for Difference)
4. Forex Margin Trading и FX сделки как отдельный объект учета
5. ~~Корпоративные действия (сплиты, тендеры, слияния)~~ ← **Фаза 1**: предупреждение, **Фаза 2**: полная поддержка
6. ~~Переводы между счетами IB~~ ← **ОБЯЗАТЕЛЬНО в Фазе 1**

**Что ОБЯЗАТЕЛЬНО поддерживается в MVP:**
1. ✅ Переводы акций (секция "Переводы") - критично для FIFO
2. ✅ Предупреждение о корпоративных действиях (полная поддержка в Фазе 2)
3. ✅ Проценты как "Прочий доход" (после дивидендов)
4. ✅ ADR Fees в категории "Комиссии по дивидендам"
5. ✅ Валидация уникальности trade_id

**Почему некоторые ограничения:**
- Отсутствуют в предоставленных файлах
- Требуют специфической логики учета
- Низкий приоритет для MVP
- **FX учитываем только через комиссии (прочие комиссии)**

### 8.3 Обработка ошибок и граничные случаи

**НОВОЕ: Обязательная обработка:**

1. **Отсутствующие секции в CSV:**
   - Предупреждение если нет критичных секций ("Сделки", "Дивиденды")
   - Пропуск опциональных секций без ошибки

2. **Некорректные данные:**
   - Валидация формата дат, чисел, валют
   - Логирование строк с ошибками парсинга
   - Продолжение обработки остальных строк

3. **FIFO аномалии:**
   - Продажа БЕЗ предварительной покупки → short sale
   - Отрицательный остаток после всех операций → предупреждение
   - Перенос short position на следующий год → `pending_short_sales`

4. **Дубликаты trade_id:**
   - Обнаружение при загрузке нескольких файлов
   - Автодобавление счетчика: `IB_AAPL_20240101_1` → `IB_AAPL_20240101_1_dup1`
   - Логирование всех коллизий

5. **Отсутствие курса ЦБ РФ:**
   - Использовать курс ближайшего рабочего дня (вперед)
   - Логировать замены курсов
   - Отображать в UI с пометкой "курс на [дата]"

### 8.4 Различия в расчетах

**IB vs ЦБ РФ курсы:**
- IB использует рыночные курсы на момент сделки
- ЦБ РФ публикует официальные курсы раз в день
- Возможны расхождения до 1-2%

**Решение:**
- Использовать курсы ЦБ РФ для налоговой отчетности (обязательно)
- Добавить поле "Курс IB" для справки (опционально)

---

## 9. Критерии успеха

### 9.1 Минимальный функционал (MVP)

**Обязательные функции:**
- [x] Загрузка CSV файлов IB в БД
- [ ] Парсинг сделок с акциями
- [ ] Парсинг сделок с опционами
- [ ] **Парсинг переводов акций (секция "Переводы")** ← КРИТИЧНО
- [ ] **Обнаружение корпоративных действий с предупреждением** ← КРИТИЧНО
- [ ] Корректный FIFO расчет для простых случаев
- [ ] **FIFO с учетом переводов (без P/L)** ← КРИТИЧНО
- [ ] Парсинг дивидендов
- [ ] Парсинг удержанных налогов
- [ ] **Парсинг процентов → категория "Прочий доход" (ПОСЛЕ дивидендов)** ← ОБЯЗАТЕЛЬНО
- [ ] **Парсинг ADR Fees → категория "Комиссии по дивидендам"** ← ОБЯЗАТЕЛЬНО
- [ ] Конвертация всех сумм в RUB по курсам ЦБ РФ
- [ ] **Валидация уникальности trade_id** ← ОБЯЗАТЕЛЬНО
- [ ] **Обработка ошибок парсинга с логированием** ← ОБЯЗАТЕЛЬНО
- [ ] UI для выбора брокера (FFG/IB)
- [ ] Отображение результатов в едином формате
- [ ] **Отображение "Прочий доход" ПОСЛЕ дивидендов в UI** ← ОБЯЗАТЕЛЬНО

### 9.2 Полный функционал (Фаза 2+)

- [ ] Обработка всех типов активов (акции, опционы, варранты)
- [ ] Обработка шортов с покрытием
- [ ] Исполнение опционов (assignment)
- [ ] Частичные исполнения сделок
- [ ] **Автоматическая обработка корпоративных действий (сплиты, конвертации)** ← Фаза 2
- [ ] **Применение коэффициентов сплита к buy_lots в FIFO** ← Фаза 2
- [ ] **Смена тикеров (ticker change)** ← Фаза 2
- [ ] Проценты по марже (уже в MVP как "Прочий доход")
- [ ] Цветовая маркировка связанных сделок
- [ ] Агрегация сделок по дням
- [ ] Экспорт в Excel/PDF
- [ ] Обработка ошибок и валидация (базовая в MVP, расширенная в Фазе 2)
- [ ] **Дробные акции (fractional shares) с точными Decimal расчетами** ← Фаза 2
- [ ] **Мультивалютные позиции одного символа** ← Фаза 2

### 9.3 Метрики качества

**Точность расчетов:**
- Погрешность FIFO < 1 RUB на сделку
- Совпадение общей прибыли/убытка с IB отчетом (с учетом разницы курсов)

**Производительность:**
- Обработка 1 годового отчета < 10 секунд
- Обработка 7 отчетов (2019-2025) < 1 минуты

**Стабильность:**
- Обработка всех 7 файлов без ошибок
- Корректная обработка edge cases (пустые секции, нулевые комиссии)

---

## 10. Следующие шаги

### 10.1 Немедленные действия

1. **Ревью плана:**
   - ✅ План обновлен с учетом критических находок
   - Обсудить с заказчиком приоритеты фаз
   - Подтвердить подход к корпоративным действиям (предупреждение vs автообработка в MVP)

2. **Настройка окружения:**
   - Создать ветку git: `feature/ib-integration`
   - Создать тестовую БД для экспериментов

3. **Фаза 0 (ОБЯЗАТЕЛЬНЫЙ рефакторинг перед IB):**
   - Вынести FIFO логику в `utils/fifo.py`
   - Создать базовый класс `BaseBrokerParser`
   - Рефакторинг FFG парсера
   - **Критично:** подготовить FIFO для переводов и корпоративных действий

4. **Фаза 1 (начало разработки IB):**
    - Создать универсальную модель `BrokerReport`
    - Применить миграции + миграция данных из `UploadedXMLFile`
    - Тестовая загрузка одного CSV файла
    - **Зафиксировать решения:**
      - FX сделки не считаем, только комиссии (прочие комиссии)
      - Проценты = "Прочий доход" ПОСЛЕ дивидендов
      - Переводы ОБЯЗАТЕЛЬНЫ для MVP
      - Корпоративные действия: предупреждение в MVP

### 10.2 График разработки (обновленный с учетом аудита)

**С рефакторингом (РЕКОМЕНДУЕТСЯ):**

- **Фаза 0:** 4-6 часов (рефакторинг FFG, создание utils/parsers)
- **Фаза 1:** 2-3 часа (универсальная модель BrokerReport, миграции)
- **Фаза 2:** 4-5 часов (парсинг IB: сделки, переводы, предупреждения о корп. действиях, trade_id валидация)
- **Фаза 3:** 3-4 часа (FIFO с переводами, опционы, валидация)
- **Фаза 4:** 3-4 часа (дивиденды, налоги, **проценты как прочий доход**, ADR Fees)
- **Фаза 5:** 1-2 часа (UI: выбор брокера, отображение прочего дохода)
- **Фаза 6:** 2-3 часа (обработка ошибок, логирование, edge cases)
- **Фаза 7:** 3-4 часа (тестирование всех годов 2019-2025)

**ИТОГО (MVP с критическими фичами):** ~22-31 часа

**Фаза 8 (будущее - полная поддержка корп. действий):** +4-6 часов
- Автоматическое применение сплитов
- Смена тикеров
- Конвертации

**ОБЩИЙ ИТОГО (полный функционал):** ~26-37 часов

---

## 11. Вопросы для уточнения

1. **Приоритет функций:**
   - Нужна ли обработка варрантов в первой версии?
   - Нужна ли цветовая маркировка для IB (как в FFG)?

2. **Отчетность:**
   - Какой формат итогового отчета требуется (HTML, PDF, Excel)?
   - Нужно ли разделение отчетов FFG и IB или объединенный?

3. **Корпоративные действия:**
   - ✅ **Решено (аудит)**: Фаза 1 (MVP) - предупреждение пользователю при обнаружении
   - ✅ **Фаза 2** - автоматическая обработка сплитов/конвертаций
   - Есть ли дополнительные отчеты от IB с корпоративными действиями?

4. **Переводы акций:**
   - ✅ **Решено (аудит)**: ОБЯЗАТЕЛЬНЫ в MVP (критично для FIFO)
   - Нужно ли отображать в истории инструмента или только учитывать в FIFO?

5. **Проценты:**
   - ✅ **Решено (пользователь)**: "Прочий доход" ПОСЛЕ дивидендов
   - Отображать кредитовые и дебетовые проценты отдельно или суммарно?

6. **Тестирование:**
   - Есть ли эталонные значения для сверки расчетов?
   - Какие edge cases критичны?

7. **Подтвержденные решения:**
   - ✅ FX сделки не считаем, только комиссии (прочие комиссии)
   - ✅ Проценты = прочий доход после дивидендов (НЕ комиссии)
   - ✅ Переводы обязательны в MVP
   - ✅ ADR Fees = комиссии по дивидендам (не торговые комиссии)

---

## 12. Заключение

Данный план обеспечивает внедрение поддержки Interactive Brokers с **МАКСИМАЛЬНЫМ переиспользованием кода** и **МИНИМАЛЬНЫМ дублированием**.

### Ключевые преимущества подхода:

**1. Переиспользование кода (DRY принцип):**
- ✅ ОДНА FIFO логика для всех брокеров (`utils/fifo.py`)
- ✅ ОДНА система работы с курсами ЦБ РФ (`utils/currency.py`)
- ✅ ОДНА система форматирования (`utils/formatters.py`)
- ✅ ОДИН базовый класс парсера (`parsers/base.py`)

**2. Унифицированное отображение:**
- ✅ ОДИН шаблон для всех брокеров
- ✅ ОДНА структура данных на выходе
- ✅ ОДИН UI (только выбор типа при загрузке)
- ✅ Сделки FFG и IB выглядят ОДИНАКОВО

**3. Масштабируемость:**
- ✅ Добавление 3-го брокера: ~6-8 часов (не 20!)
- ✅ Изменения в FIFO автоматически применяются ко ВСЕМ
- ✅ Исправление багов в одном месте, а не в двух

**4. Архитектура без дублирования:**

```
User → UI (единый интерфейс)
         ↓
    Views (единая функция display_trades)
         ↓
    ┌────┴────┐
    ↓         ↓
FFGParser  IBParser  ← оба наследуют BaseBrokerParser
    ↓         ↓
  XML      CSV       ← только специфика формата
 парсинг  парсинг
    ↓         ↓
    └────┬────┘
         ↓
  Унифицированный формат данных
         ↓
    ┌────┴────┐
    ↓         ↓         ↓
utils.fifo  utils.currency  utils.formatters
    ↓         ↓         ↓
    └─────────┴─────────┘
         ↓
  ОДИНАКОВЫЙ результат
         ↓
  Единый шаблон отображения
```

### Что НЕ дублируется:

- ❌ FIFO логика (общая в `utils/fifo.py`)
- ❌ Курсы ЦБ РФ (общая в `utils/currency.py`)
- ❌ Цветовая маркировка (общая в `utils/formatters.py`)
- ❌ Шаблоны отображения (один `trades_display.html`)
- ❌ Обработка шортов (общая FIFO логика)
- ❌ Расчет прибыли/убытка (общая FIFO логика)

### Что специфично для каждого брокера:

- ✅ **FFGParser:** Парсинг XML структуры
- ✅ **IBParser:** Парсинг CSV структуры
- ✅ **IBParser:** Парсинг названий опционов IB формата
- ✅ **IBParser:** Извлечение ISIN из описаний

**ИТОГО специфичного кода для IB:** ~300-400 строк (вместо 1800+ при дублировании!)

---

### Следующий шаг: Начать с Фазы 0 (Рефакторинг)

**Почему важно:**
- Сначала вынести общий код из FFG
- Затем добавить IB, переиспользуя готовые модули
- Избежать копипасты FIFO логики (~800 строк!)

**Вопрос для подтверждения:**
Начинаем с рефакторинга FFG кода (Фаза 0) или сразу добавляем IB поверх существующего кода?

**Рекомендация:** Фаза 0 → экономия времени на последующих фазах + чистая архитектура.

---

**Готовность к разработке:** ✅
**Статус плана:** Обновлен с акцентом на переиспользование кода
**Архитектура:** DRY (Don't Repeat Yourself)
**Автор:** Claude Code
**Дата:** 2026-01-03 (обновлено)
