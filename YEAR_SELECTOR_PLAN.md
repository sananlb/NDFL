# План: Улучшение выбора года для расчёта

## Текущее состояние
- Пользователь вводит год вручную в поле `<input type="number">`
- Кнопка "Рассчитать данные за указанный год" всегда активна
- Нет валидации: можно нажать кнопку без загруженных отчётов или без указания года

## Цели
1. Кнопка "Рассчитать данные за указанный год" должна быть неактивна (`disabled`), пока:
   - Не загружен хотя бы один отчёт **для выбранного брокера**
   - Не выбран год для расчёта
2. Заменить ручной ввод года на выбор из доступных годов (кнопки/чипы)
3. **Фильтровать годы по выбранному брокеру** (годы FFG не показываются при выборе IB и наоборот)
4. **Добавить серверную валидацию** наличия отчётов перед обработкой

---

## Изменения

### 1. Backend: `views.py`

#### 1.1. Добавить в контекст список доступных годов (с фильтрацией по брокеру)
```python
# В функции upload_xml_file(), после формирования previously_uploaded_files
selected_broker_type = request.session.get('last_broker_type', 'ffg')

# Фильтруем отчёты по выбранному брокеру
uploaded_reports_for_broker = BrokerReport.objects.filter(user=user, broker_type=selected_broker_type)
available_years = sorted(
    uploaded_reports_for_broker.values_list('year', flat=True).distinct(),
    reverse=True
)

# Используем существующую переменную target_report_year_for_title как selected_year
selected_year = request.session.get('last_target_year')
# Проверяем, что выбранный год есть среди доступных для текущего брокера
if selected_year and selected_year not in available_years:
    selected_year = None

context['available_years'] = available_years
context['selected_year'] = selected_year
context['has_uploaded_reports'] = uploaded_reports_for_broker.exists()
```

#### 1.2. Добавить серверную валидацию в process_trades
```python
if action == 'process_trades':
    year_str_from_form = request.POST.get('year_for_process')
    broker_type = request.POST.get('broker_type', 'ffg')

    # НОВОЕ: Серверная проверка наличия отчётов
    reports_exist = BrokerReport.objects.filter(user=user, broker_type=broker_type).exists()
    if not reports_exist:
        messages.error(request, f'Нет загруженных отчётов для брокера {broker_type.upper()}. Сначала загрузите отчёты.')
        return redirect('upload_xml_file')

    if not year_str_from_form:
        messages.error(request, 'Пожалуйста, выберите целевой год для анализа сделок.')
        return redirect('upload_xml_file')

    # ... остальной код без изменений
```

---

### 2. Frontend: `upload.html`

#### 2.1. Заменить input на кнопки выбора года

**Было:**
```html
<label for="year_input_process">Целевой год для анализа:</label>
<input type="number" name="year_for_process" id="year_input_process" required placeholder="YYYY" value="...">
```

**Станет:**
```html
<label>Целевой год для анализа:</label>
{% if available_years %}
    <div class="year-selector" id="year_selector">
        {% for year in available_years %}
            <button type="button"
                    class="year-btn {% if year == selected_year %}selected{% endif %}"
                    data-year="{{ year }}"
                    onclick="selectYear({{ year }}, this)">
                {{ year }}
            </button>
        {% endfor %}
    </div>
    <input type="hidden" name="year_for_process" id="year_input_process" value="{{ selected_year|default:'' }}">
{% else %}
    <p class="no-years-message">Сначала загрузите отчёты для выбранного брокера</p>
{% endif %}
```

#### 2.2. Добавить CSS для кнопок годов
```css
.year-selector {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-bottom: 15px;
}

.year-btn {
    padding: 10px 20px;
    border: 2px solid #007bff;
    background-color: #fff;
    color: #007bff;
    border-radius: 5px;
    cursor: pointer;
    font-size: 1em;
    font-weight: bold;
    transition: all 0.2s ease;
}

.year-btn:hover {
    background-color: #e6f0ff;
}

.year-btn.selected {
    background-color: #007bff;
    color: white;
}

.no-years-message {
    color: #666;
    font-style: italic;
}
```

#### 2.3. Добавить JavaScript для выбора года и управления кнопкой
```javascript
// ИСПРАВЛЕНО: передаём btn как аргумент вместо использования event.target
function selectYear(year, btn) {
    // Убираем selected со всех кнопок
    document.querySelectorAll('.year-btn').forEach(b => {
        b.classList.remove('selected');
    });

    // Добавляем selected на нажатую кнопку
    btn.classList.add('selected');

    // Устанавливаем значение в скрытый input
    document.getElementById('year_input_process').value = year;

    // Активируем кнопку расчёта
    updateProcessButton();
}

function updateProcessButton() {
    const yearInput = document.getElementById('year_input_process');
    const processBtn = document.getElementById('process_btn');
    const hasYear = yearInput && yearInput.value;
    const hasReports = {{ has_uploaded_reports|yesno:"true,false" }};

    if (processBtn) {
        processBtn.disabled = !(hasYear && hasReports);
    }
}

// Вызываем при загрузке страницы для установки начального состояния
document.addEventListener('DOMContentLoaded', updateProcessButton);

// НОВОЕ: При смене брокера перезагружаем страницу для обновления списка годов
document.getElementById('broker_type_select').addEventListener('change', function() {
    // Сохраняем выбор брокера и перезагружаем для обновления available_years
    // Вариант 1: Просто перезагрузить (годы обновятся после загрузки отчёта)
    // Вариант 2: Сделать AJAX-запрос для получения годов без перезагрузки
    // Пока используем простой вариант - очищаем выбор года при смене брокера
    const yearInput = document.getElementById('year_input_process');
    if (yearInput) {
        yearInput.value = '';
    }
    document.querySelectorAll('.year-btn').forEach(b => {
        b.classList.remove('selected');
    });
    updateProcessButton();
});
```

#### 2.4. Добавить id и disabled к кнопке расчёта
**Было:**
```html
<button type="submit" name="action" value="process_trades">Рассчитать данные за указанный год</button>
```

**Станет:**
```html
<button type="submit" name="action" value="process_trades" id="process_btn" disabled>
    Рассчитать данные за указанный год
</button>
```

#### 2.5. Добавить CSS для неактивной кнопки
```css
button[type="submit"]:disabled {
    background-color: #cccccc;
    cursor: not-allowed;
    opacity: 0.6;
}

button[type="submit"]:disabled:hover {
    background-color: #cccccc;
}
```

---

### 3. PDF шаблон: `pdf_report.html`
- Изменений не требуется

---

## Порядок реализации

1. **views.py**:
   - Добавить `available_years` с фильтрацией по `selected_broker_type`
   - Добавить `selected_year` в контекст (использовать `last_target_year` из сессии)
   - Добавить `has_uploaded_reports` с фильтрацией по брокеру
   - Добавить серверную валидацию в `process_trades`
2. **upload.html**:
   - Добавить CSS стили
   - Заменить input на кнопки годов с `onclick="selectYear({{ year }}, this)"`
   - Добавить JavaScript с исправленной функцией `selectYear(year, btn)`
   - Обновить кнопку расчёта (добавить `id` и `disabled`)
3. Тестирование локально
4. Коммит и деплой на сервер

---

## Исправленные проблемы (из ревью)

| Проблема | Решение |
|----------|---------|
| `selected_year` не определён в контексте | Добавлен `context['selected_year']` из `last_target_year` |
| `event.target` без передачи event | Изменено на `selectYear(year, this)` и `btn.classList.add()` |
| Годы не фильтруются по брокеру | `available_years` фильтруется по `selected_broker_type` |
| Нет серверной валидации | Добавлена проверка `reports_exist` в `process_trades` |
| Выбранный год может быть "чужим" | Проверка `if selected_year not in available_years` |

---

## Ограничения текущего решения

1. **При смене брокера в селекте** список годов не обновляется динамически (требуется перезагрузка страницы или AJAX). Текущее решение: очищаем выбор года при смене брокера на фронте.

2. **Альтернатива (AJAX)**: можно добавить endpoint для получения годов по брокеру и обновлять кнопки динамически. Это усложнит реализацию.

---

## Дополнительные улучшения (опционально)

- [ ] AJAX-обновление списка годов при смене брокера
- [ ] Автоматический выбор последнего года при загрузке страницы
- [ ] Показ количества отчётов для каждого года: `2024 (3 отчёта)`
- [ ] Анимация при выборе года
