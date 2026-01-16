# reports_to_ndfl/views.py

from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import HttpResponse
from django.template.loader import render_to_string
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict, Counter
from decimal import Decimal
from django.contrib.auth.decorators import login_required
from .models import BrokerReport
import json
import re
import io

from xhtml2pdf import pisa, default
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
from django.conf import settings
from django.contrib.staticfiles import finders

# Регистрируем шрифты DejaVu для поддержки кириллицы в PDF
_fonts_registered = False
def register_fonts():
    global _fonts_registered
    if not _fonts_registered:
        font_path = os.path.join(settings.BASE_DIR, 'reports_to_ndfl', 'static', 'fonts')
        pdfmetrics.registerFont(TTFont('DejaVuSans', os.path.join(font_path, 'DejaVuSans.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', os.path.join(font_path, 'DejaVuSans-Bold.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-Oblique', os.path.join(font_path, 'DejaVuSans-Oblique.ttf')))
        pdfmetrics.registerFont(TTFont('DejaVuSans-BoldOblique', os.path.join(font_path, 'DejaVuSans-BoldOblique.ttf')))
        pdfmetrics.registerFontFamily(
            'DejaVuSans',
            normal='DejaVuSans',
            bold='DejaVuSans-Bold',
            italic='DejaVuSans-Oblique',
            boldItalic='DejaVuSans-BoldOblique',
        )
        default.DEFAULT_FONT['dejavusans'] = 'DejaVuSans'
        default.DEFAULT_FONT['dejavu sans'] = 'DejaVuSans'
        default.DEFAULT_FONT['dejavusans-bold'] = 'DejaVuSans-Bold'
        default.DEFAULT_FONT['dejavu sans bold'] = 'DejaVuSans-Bold'
        _fonts_registered = True


def _pisa_link_callback(uri, _rel):
    if uri.startswith(('http://', 'https://', 'file://')):
        return uri
    result = finders.find(uri)
    if result:
        if isinstance(result, (list, tuple)):
            result = result[0]
        return result
    return uri

# Импортируем функцию из нового файла
from .parsers import FFGParser, IBParser


def _attach_dividend_fees(dividend_events, dividend_commissions_data):
    report = {
        'total_fee_details': 0,
        'matched_fee_details': 0,
        'unmatched_fee_details': 0,
        'expected_fee_sum_rub': Decimal(0),
        'assigned_fee_sum_rub': Decimal(0),
        'ok': True,
        'issues': [],
    }

    if not dividend_commissions_data:
        return report

    # Делает функцию идемпотентной: повторный вызов не "удвоит" комиссии.
    for div_event in dividend_events or []:
        div_event.pop('fee_rub', None)

    # Если дивидендов нет, но FEE есть — сопоставить невозможно, это ошибка данных/парсинга.
    if not dividend_events:
        for _category, data in dividend_commissions_data.items():
            report['total_fee_details'] += len(data.get('details', []))
            report['expected_fee_sum_rub'] += sum(
                (d.get('amount_rub', Decimal(0)) for d in data.get('details', [])),
                Decimal(0),
            )
        if report['total_fee_details']:
            report['ok'] = False
            report['unmatched_fee_details'] = report['total_fee_details']
            report['issues'].append('Найдены комиссии FEE, но дивиденды за год не найдены — сопоставление невозможно.')
        return report

    dividends_by_match_key = {}
    dividends_by_dividend_key_currency = defaultdict(list)
    dividends_by_ticker_currency = defaultdict(list)

    for div_event in dividend_events:
        dividend_match_key = div_event.get('dividend_match_key')
        if dividend_match_key:
            dividends_by_match_key[dividend_match_key] = div_event

        dividend_key = div_event.get('dividend_key')
        currency = (div_event.get('currency') or '').upper()
        if dividend_key and currency:
            dividends_by_dividend_key_currency[(dividend_key, currency)].append(div_event)

        ticker = (div_event.get('ticker') or '').upper()
        if ticker and currency:
            dividends_by_ticker_currency[(ticker, currency)].append(div_event)

    def _nearest_by_date(candidates, target_date):
        if not candidates:
            return None
        if not target_date:
            return candidates[0]
        best = None
        best_delta = None
        for candidate in candidates:
            d = candidate.get('date')
            if not d:
                continue
            delta = abs((d - target_date).days)
            if best is None or delta < best_delta or (delta == best_delta and d < best.get('date')):
                best = candidate
                best_delta = delta
        return best or candidates[0]

    def _fee_date(detail):
        d = detail.get('date_obj')
        if d:
            return d
        date_str = detail.get('date')
        if date_str:
            try:
                return datetime.strptime(date_str, '%d.%m.%Y').date()
            except ValueError:
                return None
        return None

    for _category, data in dividend_commissions_data.items():
        for detail in data.get('details', []):
            amount_rub = detail.get('amount_rub', Decimal(0))
            report['total_fee_details'] += 1
            report['expected_fee_sum_rub'] += amount_rub

            # 1) Точное совпадение: match_key (дата+валюта+нормализованное описание)
            dividend_match_key = detail.get('dividend_match_key')
            matched_div = dividends_by_match_key.get(dividend_match_key) if dividend_match_key else None

            # 2) Fallback: по dividend_key+валюта + ближайшая дата
            if matched_div is None:
                dividend_key = detail.get('dividend_key')
                currency = (detail.get('currency') or '').upper()
                if dividend_key and currency:
                    candidates = dividends_by_dividend_key_currency.get((dividend_key, currency), [])
                    matched_div = _nearest_by_date(candidates, _fee_date(detail))

            # 3) Fallback: по тикеру (из дивиденда) + валюта + ближайшая дата
            if matched_div is None:
                ticker = None
                comment = detail.get('comment') or ''
                m = re.match(r'^([A-Z0-9.\-]+)\s*\(', comment.strip())
                if m:
                    ticker = m.group(1)
                currency = (detail.get('currency') or '').upper()
                if ticker and currency:
                    candidates = dividends_by_ticker_currency.get((ticker, currency), [])
                    matched_div = _nearest_by_date(candidates, _fee_date(detail))

            # 4) Последний шанс: ближайшая дата по всем дивидендам (чтобы не терять FEE)
            if matched_div is None:
                matched_div = _nearest_by_date(dividend_events, _fee_date(detail))

            if matched_div is None:
                report['unmatched_fee_details'] += 1
                report['issues'].append('Не удалось сопоставить комиссию FEE ни с одним дивидендом.')
                continue

            report['matched_fee_details'] += 1
            matched_div['fee_rub'] = matched_div.get('fee_rub', Decimal(0)) + amount_rub

    report['assigned_fee_sum_rub'] = sum((d.get('fee_rub', Decimal(0)) for d in dividend_events), Decimal(0))
    if report['assigned_fee_sum_rub'] != report['expected_fee_sum_rub']:
        report['ok'] = False
        report['issues'].append(
            f"Сумма FEE по деталям ({report['expected_fee_sum_rub']}) не равна сумме FEE по дивидендам ({report['assigned_fee_sum_rub']})."
        )
    if report['unmatched_fee_details'] > 0:
        report['ok'] = False

    return report


# parse_year_from_date_end остается здесь, так как используется для первичного чтения файла
def parse_year_from_date_end(xml_string_content):
    try:
        match_attr = re.search(r'<broker_report[^>]*date_end="(\d{4})-\d{2}-\d{2}', xml_string_content)
        if match_attr: return int(match_attr.group(1))
        root = ET.fromstring(xml_string_content)
        date_end_el = root.find('.//date_end')
        if date_end_el is not None and date_end_el.text:
            match_tag = re.match(r"(\d{4})", date_end_el.text.strip())
            if match_tag: return int(match_tag.group(1))
    except ET.ParseError: pass
    except Exception as e: pass
    return None


def parse_year_from_ib_filename(filename):
    matches = re.findall(r'(\d{4})', filename or '')
    if matches:
        try:
            return int(matches[-1])
        except ValueError:
            return None
    return None


def parse_account_number_from_ffg_xml(xml_string_content):
    """Извлекает номер счёта (client_code) из XML отчёта FFG."""
    try:
        root = ET.fromstring(xml_string_content)
        client_code_el = root.find('.//plainAccountInfoData/client_code')
        if client_code_el is not None and client_code_el.text:
            return client_code_el.text.strip()
    except ET.ParseError:
        pass
    except Exception:
        pass
    return None


def parse_account_number_from_ib_csv(file_path):
    """Извлекает номер счёта из CSV отчёта Interactive Brokers."""
    import csv
    try:
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 4:
                    # Нормализуем ё -> е для корректного сравнения
                    col0 = row[0].strip().replace('ё', 'е').replace('Ё', 'Е')
                    col2 = row[2].strip().replace('ё', 'е').replace('Ё', 'Е')

                    # Ищем строку "Информация о счете,Data,Счет,НОМЕР" (с е или ё)
                    if col0 == 'Информация о счете' and row[1].strip() == 'Data' and col2 == 'Счет':
                        return row[3].strip()
                    # Также проверяем английский вариант "Account Information,Data,Account,NUMBER"
                    if row[0].strip() == 'Account Information' and row[1].strip() == 'Data' and row[2].strip() == 'Account':
                        return row[3].strip()
    except Exception:
        pass
    return None


def _remove_reports_for_other_broker(user, broker_type):
    other_broker = 'ib' if broker_type == 'ffg' else 'ffg'
    other_reports = BrokerReport.objects.filter(user=user, broker_type=other_broker)
    for report in other_reports:
        if report.report_file:
            report.report_file.delete(save=False)
    other_reports.delete()

@login_required
def delete_xml_file(request, file_id):
    try:
        file_to_delete = BrokerReport.objects.get(pk=file_id, user=request.user)
        file_name = file_to_delete.original_filename
        file_year = file_to_delete.year
        
        # Удаляем файл с диска
        if file_to_delete.report_file:
            file_to_delete.report_file.delete()
        
        # Удаляем запись из БД
        file_to_delete.delete()
        
        messages.success(request, f"Файл '{file_name}' (отчет за {file_year} год) успешно удален.")
    except BrokerReport.DoesNotExist:
        messages.error(request, "Файл не найден или у вас нет прав для его удаления.")
    
    return redirect('upload_xml_file')

@login_required
def upload_xml_file(request):
    user = request.user

    # Включение/выключение debug режима через GET-параметр (сохраняем в сессии,
    # чтобы он действовал на следующий тяжёлый прогон парсинга после редиректа).
    if request.method == 'GET' and 'debug_events' in request.GET:
        debug_val = str(request.GET.get('debug_events', '')).strip().lower()
        request.session['debug_events'] = debug_val in ('1', 'true', 'yes', 'on')

    debug_events = bool(request.session.get('debug_events', False))

    # Обработка смены брокера через GET-параметр (при перезагрузке страницы)
    if request.method == 'GET' and 'set_broker' in request.GET:
        new_broker = request.GET.get('set_broker')
        if new_broker in ['ffg', 'ib']:
            request.session['last_broker_type'] = new_broker
            # Сбрасываем выбранный год при смене брокера
            if 'last_target_year' in request.session:
                del request.session['last_target_year']
        return redirect('upload_xml_file')

    selected_broker_type = request.session.get('last_broker_type', 'ffg')

    # Фильтруем отчёты по выбранному брокеру для получения доступных годов
    uploaded_reports_for_broker = BrokerReport.objects.filter(user=user, broker_type=selected_broker_type)
    available_years = sorted(
        uploaded_reports_for_broker.values_list('year', flat=True).distinct()
    )

    # Получаем выбранный год из сессии и проверяем его валидность
    selected_year = request.session.get('last_target_year')
    if selected_year and selected_year not in available_years:
        selected_year = None

    context = {
        'target_report_year_for_title': None,
        'instrument_event_history': {},
        'dividend_history': [],
        'total_dividends_rub': Decimal(0),
        'total_sales_profit_rub': Decimal(0),
        'parsing_error_occurred': False,
        'processing_has_run_for_current_display': False,
        'previously_uploaded_files': BrokerReport.objects.filter(user=user).order_by('-year', '-uploaded_at'),
        'dividend_commissions': {},
        'other_commissions': {},
        'total_dividend_commissions_rub': Decimal(0),
        'total_other_commissions_rub': Decimal(0),
        'selected_broker_type': selected_broker_type,
        'available_years': available_years,
        'selected_year': selected_year,
        'has_uploaded_reports': uploaded_reports_for_broker.exists(),
        'debug_events': debug_events,
        'debug_acquisition_events': [],
        'debug_group_type_counts': {},
    }
    context['target_report_year_for_title'] = request.session.get('last_target_year', None)

    if request.method == 'POST':
        action = request.POST.get('action')
        if action == 'delete_all_reports':
            reports = BrokerReport.objects.filter(user=user)
            for report in reports:
                if report.report_file:
                    report.report_file.delete(save=False)
            count = reports.count()
            reports.delete()
            messages.success(request, f"Удалено отчетов: {count}.")
            return redirect('upload_xml_file')
        if action == 'process_trades':
            year_str_from_form = request.POST.get('year_for_process')
            broker_type = request.POST.get('broker_type', 'ffg')

            # Серверная проверка наличия отчётов для выбранного брокера
            reports_exist = BrokerReport.objects.filter(user=user, broker_type=broker_type).exists()
            if not reports_exist:
                broker_display = 'Freedom Finance Global' if broker_type == 'ffg' else 'Interactive Brokers'
                messages.error(request, f'Нет загруженных отчётов для брокера {broker_display}. Сначала загрузите отчёты.')
                return redirect('upload_xml_file')

            if not year_str_from_form:
                messages.error(request, 'Пожалуйста, выберите целевой год для анализа сделок.')
                return redirect('upload_xml_file')
            try:
                target_report_year = int(year_str_from_form)

                # Проверяем, что выбранный год существует среди отчётов для данного брокера
                valid_years = BrokerReport.objects.filter(
                    user=user, broker_type=broker_type
                ).values_list('year', flat=True).distinct()
                if target_report_year not in valid_years:
                    broker_display = 'Freedom Finance Global' if broker_type == 'ffg' else 'Interactive Brokers'
                    messages.error(request, f'Нет отчётов за {target_report_year} год для брокера {broker_display}.')
                    return redirect('upload_xml_file')

                request.session['last_target_year'] = target_report_year
                request.session['run_processing_for_year'] = target_report_year
                request.session['last_broker_type'] = broker_type
                request.session['run_processing_broker_type'] = broker_type
            except ValueError:
                messages.error(request, 'Некорректный формат целевого года в форме.')
            return redirect('upload_xml_file')
        elif action == 'upload_reports':
            broker_type = request.POST.get('broker_type', 'ffg')
            uploaded_files_from_form = request.FILES.getlist('report_file')
            if not uploaded_files_from_form:
                messages.error(request, 'Пожалуйста, выберите хотя бы один файл для загрузки.')
                return redirect('upload_xml_file')

            # Валидация формата файлов
            expected_ext = '.csv' if broker_type == 'ib' else '.xml'
            invalid_files = []
            for f in uploaded_files_from_form:
                if not f.name.lower().endswith(expected_ext):
                    invalid_files.append(f.name)
            if invalid_files:
                ext_upper = expected_ext.upper()
                messages.error(request, f'Неверный формат файла. Для выбранного брокера требуется формат {ext_upper}. Неподходящие файлы: {", ".join(invalid_files)}')
                return redirect('upload_xml_file')

            _remove_reports_for_other_broker(user, broker_type)
            request.session['last_broker_type'] = broker_type
            parsing_error_in_upload_phase = False
            for uploaded_file_from_form in uploaded_files_from_form:
                original_name = uploaded_file_from_form.name; xml_string = ""; file_year_from_xml = None
                try:
                    content_bytes = uploaded_file_from_form.read(); uploaded_file_from_form.seek(0)
                    if broker_type == 'ffg':
                        try: xml_string = content_bytes.decode('utf-8')
                        except UnicodeDecodeError:
                            try: xml_string = content_bytes.decode('windows-1251', errors='replace')
                            except UnicodeDecodeError:
                                messages.error(request, f"Файл {original_name}: не удалось определить кодировку. Файл пропущен.")
                                parsing_error_in_upload_phase = True;
                                continue
                        if xml_string:
                            file_year_from_xml = parse_year_from_date_end(xml_string)
                    else:
                        file_year_from_xml = parse_year_from_ib_filename(original_name)

                    if file_year_from_xml is None:
                        messages.error(request, f"Файл {original_name}: не удалось определить год отчета. Файл пропущен.")
                        parsing_error_in_upload_phase = True;
                        continue
                    if BrokerReport.objects.filter(user=user, broker_type=broker_type, original_filename=original_name, year=file_year_from_xml).exists():
                        messages.warning(request, f"Файл '{original_name}' для {file_year_from_xml} года уже был загружен. Пропуск.")
                        continue

                    # Извлекаем номер счёта из отчёта
                    account_number = None
                    if broker_type == 'ffg' and xml_string:
                        account_number = parse_account_number_from_ffg_xml(xml_string)

                    instance = BrokerReport(
                        user=user,
                        broker_type=broker_type,
                        report_file=uploaded_file_from_form,
                        year=file_year_from_xml,
                        original_filename=original_name,
                        account_number=account_number or '',
                    )
                    instance.save()

                    # Для IB извлекаем номер счёта после сохранения файла
                    if broker_type == 'ib' and instance.report_file:
                        ib_account = parse_account_number_from_ib_csv(instance.report_file.path)
                        if ib_account:
                            instance.account_number = ib_account
                            instance.save(update_fields=['account_number'])

                    messages.success(request, f"Файл {original_name} (отчет за {file_year_from_xml} год) успешно загружен.")
                except Exception as e:
                    messages.error(request, f"Ошибка при первичной обработке файла {original_name}: {e}. Файл пропущен.")
                    parsing_error_in_upload_phase = True
            if parsing_error_in_upload_phase: messages.warning(request, "При загрузке некоторых файлов возникли ошибки.")
            return redirect('upload_xml_file')
        else:
            messages.error(request, "Неизвестное или отсутствующее действие в запросе.")
            return redirect('upload_xml_file')
    else: # GET request
        year_to_process = request.session.pop('run_processing_for_year', None)
        broker_type_to_process = request.session.pop('run_processing_broker_type', None)
        if year_to_process is not None:
            # Важно: сохраняем сессию ДО тяжёлого парсинга.
            # Если запрос оборвётся по таймауту (nginx/gunicorn), Django может не успеть
            # записать обновлённую сессию в БД, и флаги "run_processing_*" останутся,
            # вызывая повторный запуск парсинга при каждом заходе на страницу.
            try:
                request.session.save()
            except Exception:
                # Фоллбэк: пусть SessionMiddleware попробует сохранить на ответе.
                request.session.modified = True

            context['target_report_year_for_title'] = year_to_process
            if broker_type_to_process:
                context['selected_broker_type'] = broker_type_to_process

            if broker_type_to_process == 'ib':
                parser = IBParser(request, user, year_to_process)
            else:
                parser = FFGParser(request, user, year_to_process)

            instrument_event_history, dividend_events, total_dividends_rub, \
            total_sales_profit, parsing_error_current_run, \
            dividend_commissions_data, other_commissions_data, total_other_commissions_rub_val, \
            profit_by_income_code = parser.process()

            # Явное преобразование defaultdict в обычные dict
            # Это должно гарантировать, что в шаблон попадут стандартные dict,
            # что может помочь избежать необычного поведения с Decimal.
            if isinstance(dividend_commissions_data, defaultdict):
                temp_div_comm = {}
                for category_key, data_dict_item in dividend_commissions_data.items(): # Переименовано для ясности
                    temp_div_comm[category_key] = {
                        'amount_by_currency': dict(data_dict_item['amount_by_currency']), # Преобразуем вложенный defaultdict
                        'amount_rub': data_dict_item['amount_rub'],
                        'details': data_dict_item['details'] # details уже является списком словарей
                    }
                dividend_commissions_data = temp_div_comm
            if isinstance(other_commissions_data, defaultdict):
                converted_other_commissions = {}
                for category, data_dict in other_commissions_data.items():
                    converted_other_commissions[category] = {
                        'currencies': dict(data_dict['currencies']), # Convert inner defaultdict
                        'total_rub': data_dict['total_rub'],
                        'raw_events': data_dict['raw_events'] # raw_events is a list of dicts, no further defaultdict conversion needed here
                    }
                other_commissions_data = converted_other_commissions


            # Разделяем историю операций по кодам дохода: 1530 (акции) и 1532 (опционы/ПФИ)
            instrument_history_1530 = {}  # Ценные бумаги
            instrument_history_1532 = {}  # ПФИ / опционы

            for key in sorted(instrument_event_history.keys()):
                events = instrument_event_history[key]
                # Проверяем код дохода: ПФИ = опционы (OPTION_) и варранты (WARRANT_)
                is_pfi = key.startswith('OPTION_') or key.startswith('WARRANT_')
                if is_pfi:
                    instrument_history_1532[key] = events
                else:
                    instrument_history_1530[key] = events

            # Вычисляем сумму комиссий, связанных с дивидендами
            total_dividend_commissions_rub = sum(
                (data.get('amount_rub', Decimal(0)) for data in dividend_commissions_data.values()),
                Decimal(0)
            )

            fee_matching_report = None

            context['instrument_history_1530'] = instrument_history_1530
            context['instrument_history_1532'] = instrument_history_1532
            context['dividend_history'] = dividend_events
            context['total_dividends_rub'] = total_dividends_rub
            context['total_sales_profit_rub'] = total_sales_profit
            context['profit_by_income_code'] = profit_by_income_code
            context['parsing_error_occurred'] = parsing_error_current_run
            context['processing_has_run_for_current_display'] = True
            context['dividend_commissions'] = dividend_commissions_data
            context['other_commissions'] = other_commissions_data
            context['total_dividend_commissions_rub'] = total_dividend_commissions_rub
            context['total_other_commissions_rub'] = total_other_commissions_rub_val
            context['dividend_fee_matching_report'] = fee_matching_report

            if debug_events:
                debug_group_type_counts = {}
                debug_acquisition_events = []

                for grouping_key, event_list in instrument_event_history.items():
                    counts = Counter()
                    for wrapper in event_list or []:
                        if isinstance(wrapper, dict):
                            display_type = wrapper.get('display_type')
                            details = wrapper.get('event_details') or {}
                            dt_obj = wrapper.get('datetime_obj')
                        else:
                            display_type = getattr(wrapper, 'display_type', None)
                            details = getattr(wrapper, 'event_details', {}) or {}
                            dt_obj = getattr(wrapper, 'datetime_obj', None)

                        counts[display_type] += 1

                        if display_type == 'acquisition_info':
                            debug_acquisition_events.append({
                                'grouping_key': grouping_key,
                                'datetime_obj': dt_obj,
                                'acquisition_type': details.get('acquisition_type'),
                                'ticker': details.get('ticker'),
                                'source_ticker': details.get('source_ticker'),
                                'quantity': details.get('quantity'),
                                'cost': details.get('cost'),
                                'currency': details.get('currency'),
                                'cost_rub': details.get('cost_rub'),
                                'is_relevant_for_target_year': details.get('is_relevant_for_target_year'),
                            })

                    debug_group_type_counts[grouping_key] = dict(counts)

                context['debug_group_type_counts'] = debug_group_type_counts
                context['debug_acquisition_events'] = debug_acquisition_events

    return render(request, 'reports_to_ndfl/upload.html', context)


@login_required
def download_pdf(request):
    """Генерация PDF-отчета с расчетами (без информации о пользователе)."""
    user = request.user
    year_str = request.GET.get('year')

    if not year_str:
        messages.error(request, 'Не указан год для генерации PDF.')
        return redirect('upload_xml_file')

    try:
        target_year = int(year_str)
    except ValueError:
        messages.error(request, 'Некорректный формат года.')
        return redirect('upload_xml_file')

    # Определяем тип брокера из сессии
    broker_type = request.session.get('last_broker_type', 'ffg')

    # Запускаем парсер для получения данных
    if broker_type == 'ib':
        parser = IBParser(request, user, target_year)
    else:
        parser = FFGParser(request, user, target_year)

    instrument_event_history, dividend_events, total_dividends_rub, \
    total_sales_profit, parsing_error, \
    dividend_commissions_data, other_commissions_data, total_other_commissions_rub_val, \
    profit_by_income_code = parser.process()

    # Преобразуем defaultdict в обычные dict
    if isinstance(dividend_commissions_data, defaultdict):
        temp_div_comm = {}
        for category_key, data_dict_item in dividend_commissions_data.items():
            temp_div_comm[category_key] = {
                'amount_by_currency': dict(data_dict_item['amount_by_currency']),
                'amount_rub': data_dict_item['amount_rub'],
                'details': data_dict_item['details']
            }
        dividend_commissions_data = temp_div_comm

    if isinstance(other_commissions_data, defaultdict):
        converted_other_commissions = {}
        for category, data_dict in other_commissions_data.items():
            converted_other_commissions[category] = {
                'currencies': dict(data_dict['currencies']),
                'total_rub': data_dict['total_rub'],
                'raw_events': data_dict['raw_events']
            }
        other_commissions_data = converted_other_commissions

    # Разделяем историю операций по кодам дохода: 1530 (акции) и 1532 (опционы/ПФИ)
    instrument_history_1530 = {}  # Ценные бумаги
    instrument_history_1532 = {}  # ПФИ / опционы

    for key in sorted(instrument_event_history.keys()):
        events = instrument_event_history[key]
        is_pfi = key.startswith('OPTION_') or key.startswith('WARRANT_')
        if is_pfi:
            instrument_history_1532[key] = events
        else:
            instrument_history_1530[key] = events

    # Регистрируем шрифты для кириллицы
    register_fonts()

    # Получаем название брокера для отображения в PDF
    broker_display_name = 'Freedom Finance Global' if broker_type == 'ffg' else 'Interactive Brokers'

    # Получаем номер счёта из отчёта за целевой год
    account_number = None
    report_with_account = BrokerReport.objects.filter(
        user=user, broker_type=broker_type, year=target_year
    ).exclude(account_number='').first()
    if report_with_account:
        account_number = report_with_account.account_number

    # Получаем комментарий пользователя
    user_comment = request.GET.get('comment', '').strip()

    # Вычисляем сумму комиссий, связанных с дивидендами
    total_dividend_commissions_rub = sum(
        (data.get('amount_rub', Decimal(0)) for data in dividend_commissions_data.values()),
        Decimal(0)
    )

    fee_matching_report = None

    # Контекст для PDF шаблона (БЕЗ информации о пользователе)
    context = {
        'target_report_year_for_title': target_year,
        'broker_name': broker_display_name,
        'account_number': account_number,
        'user_comment': user_comment,
        'instrument_history_1530': instrument_history_1530,
        'instrument_history_1532': instrument_history_1532,
        'dividend_history': dividend_events,
        'total_dividends_rub': total_dividends_rub,
        'total_sales_profit_rub': total_sales_profit,
        'profit_by_income_code': profit_by_income_code,
        'dividend_commissions': dividend_commissions_data,
        'other_commissions': other_commissions_data,
        'total_dividend_commissions_rub': total_dividend_commissions_rub,
        'total_other_commissions_rub': total_other_commissions_rub_val,
        'dividend_fee_matching_report': fee_matching_report,
        'generation_date': datetime.now().strftime('%d.%m.%Y %H:%M'),
    }

    # Рендерим HTML для PDF
    html_string = render_to_string('reports_to_ndfl/pdf_report.html', context)

    # Генерируем PDF
    result = io.BytesIO()
    pdf = pisa.pisaDocument(
        io.BytesIO(html_string.encode('utf-8')),
        result,
        encoding='utf-8',
        link_callback=_pisa_link_callback,
    )

    if pdf.err:
        messages.error(request, 'Ошибка при генерации PDF.')
        return redirect('upload_xml_file')

    # Возвращаем PDF как файл для скачивания
    response = HttpResponse(result.getvalue(), content_type='application/pdf')
    response['Content-Disposition'] = f'attachment; filename="ndfl_report_{target_year}.pdf"'
    return response
