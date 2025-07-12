# reports_to_ndfl/views.py

from django.shortcuts import render, redirect
from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict
from decimal import Decimal
from django.contrib.auth.decorators import login_required
from .models import UploadedXMLFile
import json
import re # Add this import for parse_year_from_date_end

# Импортируем функцию из нового файла
from .FFG_ndfl import process_and_get_trade_data


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

@login_required
def delete_xml_file(request, file_id):
    try:
        file_to_delete = UploadedXMLFile.objects.get(pk=file_id, user=request.user)
        file_name = file_to_delete.original_filename
        file_year = file_to_delete.year
        
        # Удаляем файл с диска
        if file_to_delete.xml_file:
            file_to_delete.xml_file.delete()
        
        # Удаляем запись из БД
        file_to_delete.delete()
        
        messages.success(request, f"Файл '{file_name}' (отчет за {file_year} год) успешно удален.")
    except UploadedXMLFile.DoesNotExist:
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
        'total_sales_profit_rub': Decimal(0),
        'parsing_error_occurred': False,
        'processing_has_run_for_current_display': False,
        'previously_uploaded_files': UploadedXMLFile.objects.filter(user=user).order_by('-year', '-uploaded_at'),
        'dividend_commissions': {}, # Инициализировано как пустой словарь
        'other_commissions': {},   # Инициализировано как пустой словарь
        'total_other_commissions_rub': Decimal(0), # Добавлено в контекст и будет заполнено
    }
    context['target_report_year_for_title'] = request.session.get('last_target_year', None)

    if request.method == 'POST':
        action = request.POST.get('action')
        if action == 'process_trades':
            year_str_from_form = request.POST.get('year_for_process')
            if not year_str_from_form:
                messages.error(request, 'Пожалуйста, укажите целевой год для анализа сделок.')
                return redirect('upload_xml_file')
            try:
                target_report_year = int(year_str_from_form)
                request.session['last_target_year'] = target_report_year
                request.session['run_processing_for_year'] = target_report_year
            except ValueError:
                messages.error(request, 'Некорректный формат целевого года в форме.')
            return redirect('upload_xml_file')
        elif action == 'upload_reports':
            uploaded_files_from_form = request.FILES.getlist('xml_file')
            if not uploaded_files_from_form:
                messages.error(request, 'Пожалуйста, выберите хотя бы один файл для загрузки.')
                return redirect('upload_xml_file')
            parsing_error_in_upload_phase = False
            for uploaded_file_from_form in uploaded_files_from_form:
                original_name = uploaded_file_from_form.name; xml_string = ""; file_year_from_xml = None
                try:
                    content_bytes = uploaded_file_from_form.read(); uploaded_file_from_form.seek(0)
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


                    if file_year_from_xml is None:
                        messages.error(request, f"Файл {original_name}: не удалось извлечь год из XML. Файл пропущен.")
                        parsing_error_in_upload_phase = True;
                        continue
                    if UploadedXMLFile.objects.filter(user=user, original_filename=original_name, year=file_year_from_xml).exists():
                        messages.warning(request, f"Файл '{original_name}' для {file_year_from_xml} года уже был загружен. Пропуск.")
                        continue
                    instance = UploadedXMLFile(user=user, xml_file=uploaded_file_from_form, year=file_year_from_xml, original_filename=original_name)
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
        if year_to_process is not None:
            context['target_report_year_for_title'] = year_to_process

            # Вызов функции из FFG_ndfl.py: ожидаем 8 значений
            instrument_event_history, dividend_events, total_dividends_rub, \
            total_sales_profit, parsing_error_current_run, \
            dividend_commissions_data, other_commissions_data, total_other_commissions_rub_val = \
                process_and_get_trade_data(request, user, year_to_process)

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


            context['instrument_event_history'] = instrument_event_history
            context['dividend_history'] = dividend_events
            context['total_dividends_rub'] = total_dividends_rub
            context['total_sales_profit_rub'] = total_sales_profit
            context['parsing_error_occurred'] = parsing_error_current_run
            context['processing_has_run_for_current_display'] = True
            context['dividend_commissions'] = dividend_commissions_data
            context['other_commissions'] = other_commissions_data
            context['total_other_commissions_rub'] = total_other_commissions_rub_val # Устанавливаем итоговую сумму здесь

    return render(request, 'reports_to_ndfl/upload.html', context)