# reports_to_ndfl/views.py

from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import HttpResponse
from django.template.loader import render_to_string
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict
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
    context = {
        'target_report_year_for_title': None,
        'instrument_event_history': {},
        'dividend_history': [],
        'total_dividends_rub': Decimal(0),
        'other_income_history': [],
        'total_other_income_rub': Decimal(0),
        'total_sales_profit_rub': Decimal(0),
        'parsing_error_occurred': False,
        'processing_has_run_for_current_display': False,
        'previously_uploaded_files': BrokerReport.objects.filter(user=user).order_by('-year', '-uploaded_at'),
        'dividend_commissions': {}, # Инициализировано как пустой словарь
        'other_commissions': {},   # Инициализировано как пустой словарь
        'total_other_commissions_rub': Decimal(0), # Добавлено в контекст и будет заполнено
        'selected_broker_type': request.session.get('last_broker_type', 'ffg'),
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
            if not year_str_from_form:
                messages.error(request, 'Пожалуйста, укажите целевой год для анализа сделок.')
                return redirect('upload_xml_file')
            try:
                target_report_year = int(year_str_from_form)
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
                    instance = BrokerReport(
                        user=user,
                        broker_type=broker_type,
                        report_file=uploaded_file_from_form,
                        year=file_year_from_xml,
                        original_filename=original_name,
                    )
                    instance.save()
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
            context['target_report_year_for_title'] = year_to_process
            if broker_type_to_process:
                context['selected_broker_type'] = broker_type_to_process

            if broker_type_to_process == 'ib':
                parser = IBParser(request, user, year_to_process)
                instrument_event_history, dividend_events, total_dividends_rub, \
                total_sales_profit, parsing_error_current_run, \
                dividend_commissions_data, other_commissions_data, total_other_commissions_rub_val, \
                other_income_events, total_other_income_rub_val = \
                    parser.process()
            else:
                parser = FFGParser(request, user, year_to_process)
                instrument_event_history, dividend_events, total_dividends_rub, \
                total_sales_profit, parsing_error_current_run, \
                dividend_commissions_data, other_commissions_data, total_other_commissions_rub_val, \
                other_income_events, total_other_income_rub_val = \
                    parser.process()

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


            # Сортируем так, чтобы опционы (OPTION_*) показывались первыми
            sorted_instrument_history = {}
            # Сначала добавляем опционы
            for key in sorted(instrument_event_history.keys()):
                if key.startswith('OPTION_'):
                    sorted_instrument_history[key] = instrument_event_history[key]
            # Затем добавляем остальные инструменты
            for key in sorted(instrument_event_history.keys()):
                if not key.startswith('OPTION_'):
                    sorted_instrument_history[key] = instrument_event_history[key]

            context['instrument_event_history'] = sorted_instrument_history
            context['dividend_history'] = dividend_events
            context['total_dividends_rub'] = total_dividends_rub
            context['other_income_history'] = other_income_events
            context['total_other_income_rub'] = total_other_income_rub_val
            context['total_sales_profit_rub'] = total_sales_profit
            context['parsing_error_occurred'] = parsing_error_current_run
            context['processing_has_run_for_current_display'] = True
            context['dividend_commissions'] = dividend_commissions_data
            context['other_commissions'] = other_commissions_data
            context['total_other_commissions_rub'] = total_other_commissions_rub_val # Устанавливаем итоговую сумму здесь

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
    other_income_events, total_other_income_rub_val = parser.process()

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

    # Сортируем инструменты
    sorted_instrument_history = {}
    for key in sorted(instrument_event_history.keys()):
        if key.startswith('OPTION_'):
            sorted_instrument_history[key] = instrument_event_history[key]
    for key in sorted(instrument_event_history.keys()):
        if not key.startswith('OPTION_'):
            sorted_instrument_history[key] = instrument_event_history[key]

    # Регистрируем шрифты для кириллицы
    register_fonts()

    # Контекст для PDF шаблона (БЕЗ информации о пользователе)
    context = {
        'target_report_year_for_title': target_year,
        'instrument_event_history': sorted_instrument_history,
        'dividend_history': dividend_events,
        'total_dividends_rub': total_dividends_rub,
        'other_income_history': other_income_events,
        'total_other_income_rub': total_other_income_rub_val,
        'total_sales_profit_rub': total_sales_profit,
        'dividend_commissions': dividend_commissions_data,
        'other_commissions': other_commissions_data,
        'total_other_commissions_rub': total_other_commissions_rub_val,
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
