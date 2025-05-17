# reports_to_ndfl/views.py
from django.shortcuts import render
from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
import re
from django.contrib.auth.decorators import login_required
# from django.db import IntegrityError # Не используется напрямую, но можно для try-except instance.save()

from .models import UploadedXMLFile

def parse_year_from_date_end(xml_string_content):
    """
    Извлекает год из тега <date_end> в XML-строке.
    Пример: <date_end>2023-12-31 23:59:59</date_end> -> 2023
    """
    try:
        # Попытка найти <broker_report date_end="YYYY-MM-DD HH:MM:SS">
        match_attr = re.search(r'<broker_report[^>]*date_end="(\d{4})-\d{2}-\d{2}', xml_string_content)
        if match_attr:
            return int(match_attr.group(1))

        # Если не нашли в атрибуте, ищем в теге <date_end>
        root = ET.fromstring(xml_string_content) # Может вызвать ParseError
        date_end_el = root.find('.//date_end')
        if date_end_el is not None and date_end_el.text:
            match_tag = re.match(r"(\d{4})", date_end_el.text.strip())
            if match_tag:
                return int(match_tag.group(1))
    except ET.ParseError:
        # Ошибка парсинга XML, год извлечь не удалось из тега
        pass # Продолжаем, так как могли извлечь из атрибута
    except Exception:
        # Другие возможные ошибки при обработке
        pass
    return None

@login_required
def upload_xml_file(request):
    user = request.user
    context = {
        'target_report_year_for_title': request.session.get('last_target_year', None),
        'instrument_trade_history': {},
        'parsing_error_occurred': False # Инициализируем
    }

    previously_uploaded_files = UploadedXMLFile.objects.filter(user=user).order_by('-year', '-uploaded_at')
    context['previously_uploaded_files'] = previously_uploaded_files

    if request.method == 'POST':
        year_str_from_form = request.POST.get('year')
        if not year_str_from_form:
            messages.error(request, 'Пожалуйста, укажите целевой год отчета в форме.')
            return render(request, 'reports_to_ndfl/upload.html', context)
        try:
            target_report_year = int(year_str_from_form)
            context['target_report_year_for_title'] = target_report_year
            request.session['last_target_year'] = target_report_year
        except ValueError:
            messages.error(request, 'Некорректный формат целевого года в форме.')
            return render(request, 'reports_to_ndfl/upload.html', context)

        uploaded_files_from_form = request.FILES.getlist('xml_file')
        if not uploaded_files_from_form:
            messages.error(request, 'Пожалуйста, выберите хотя бы один файл для загрузки.')
            return render(request, 'reports_to_ndfl/upload.html', context)

        files_processed_count = 0
        files_skipped_due_to_duplicate = 0

        for uploaded_file_from_form in uploaded_files_from_form: # Переименовал переменную
            original_name = uploaded_file_from_form.name
            xml_string = ""
            file_year_from_xml = None
            
            try:
                content_bytes = uploaded_file_from_form.read()
                uploaded_file_from_form.seek(0) # Важно для Django, если файл будет сохранен

                try:
                    xml_string = content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        xml_string = content_bytes.decode('windows-1251')
                    except UnicodeDecodeError:
                        messages.error(request, f"Файл {original_name}: не удалось определить кодировку (UTF-8, windows-1251). Файл пропущен.")
                        context['parsing_error_occurred'] = True
                        continue 
                
                if xml_string:
                    file_year_from_xml = parse_year_from_date_end(xml_string)

                if file_year_from_xml is None:
                    messages.error(request, f"Файл {original_name}: не удалось извлечь год из XML (<date_end> или атрибут broker_report). Файл пропущен.")
                    context['parsing_error_occurred'] = True
                    continue

                if UploadedXMLFile.objects.filter(user=user, original_filename=original_name, year=file_year_from_xml).exists():
                    messages.warning(request, f"Файл '{original_name}' для {file_year_from_xml} года уже был загружен вами ранее. Пропуск.")
                    files_skipped_due_to_duplicate += 1
                    continue
                
                instance = UploadedXMLFile(
                    user=user, 
                    xml_file=uploaded_file_from_form, 
                    year=file_year_from_xml,
                    original_filename=original_name
                )
                instance.save()
                files_processed_count += 1
                messages.success(request, f"Файл {original_name} (отчет за {file_year_from_xml} год) успешно загружен.")

            except Exception as e:
                messages.error(request, f"Ошибка при первичной обработке файла {original_name}: {e}. Файл пропущен.")
                context['parsing_error_occurred'] = True
                continue
        
        if files_processed_count > 0:
             context['previously_uploaded_files'] = UploadedXMLFile.objects.filter(user=user).order_by('-year', '-uploaded_at')

        if files_processed_count == 0 and uploaded_files_from_form and files_skipped_due_to_duplicate == 0:
             messages.warning(request, "Ни один из выбранных файлов не был успешно загружен (проверьте ошибки выше).")
             return render(request, 'reports_to_ndfl/upload.html', context)

        # --- Основная логика анализа ---
        full_instrument_trade_history = defaultdict(list)
        seen_trades_for_instrument = defaultdict(set)
        
        relevant_files_for_history = UploadedXMLFile.objects.filter(
            user=user, 
            year__lte=target_report_year
        ).order_by('year', 'uploaded_at')

        if not relevant_files_for_history.exists():
            if files_processed_count > 0 or files_skipped_due_to_duplicate > 0: # Если только что загружали или были дубликаты
                messages.info(request, f"Хотя файлы были загружены, у вас нет сохраненных отчетов с годом {target_report_year} или ранее для анализа полной истории.")
            else: # Если вообще не было файлов, подходящих под критерий
                messages.info(request, f"У вас нет загруженных файлов с годом отчета {target_report_year} или ранее для анализа истории.")
            return render(request, 'reports_to_ndfl/upload.html', context)

        trade_detail_tags = [
            'trade_id', 'date', 'operation', 'instr_nm', 
            'instr_type', 'instr_kind', 'p', 'curr_c', 
            'q', 'summ', 'commission', 'issue_nb'
        ]

        for file_instance in relevant_files_for_history:
            try:
                with file_instance.xml_file.open('rb') as xml_file_content_stream:
                    content_bytes = xml_file_content_stream.read()
                    xml_string_loop = "" # Используем другую переменную, чтобы не затереть xml_string от загрузки
                    try:
                        xml_string_loop = content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            xml_string_loop = content_bytes.decode('windows-1251')
                        except UnicodeDecodeError:
                            messages.warning(f"Кодировка файла {file_instance.original_filename} (год {file_instance.year}) не определена. Пропуск для истории.")
                            context['parsing_error_occurred'] = True
                            continue
                    if not xml_string_loop: continue

                    root = ET.fromstring(xml_string_loop)
                    trades_element = root.find('.//trades')
                    if trades_element:
                        detailed_element = trades_element.find('detailed')
                        if detailed_element:
                            for node_element in detailed_element.findall('node'):
                                issue_nb_el = node_element.find('issue_nb')
                                if not (issue_nb_el is not None and issue_nb_el.text and issue_nb_el.text.strip()): 
                                    # Пропускаем, если нет issue_nb или он пустой
                                    continue
                                
                                current_issue_nb = issue_nb_el.text.strip()
                                trade_data = {'file_source': f"{file_instance.original_filename} (за {file_instance.year})"}
                                for tag in trade_detail_tags:
                                    data_el = node_element.find(tag)
                                    trade_data[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)

                                signature_fields = (
                                    trade_data.get('trade_id'), 
                                    trade_data.get('date'), 
                                    trade_data.get('operation'), 
                                    trade_data.get('q'), 
                                    trade_data.get('p'), 
                                    current_issue_nb 
                                )
                                trade_signature = tuple(str(f).strip() if f is not None else "NONE_SIG" for f in signature_fields) # Изменил "NONE" на "NONE_SIG" для большей уникальности

                                if trade_signature not in seen_trades_for_instrument[current_issue_nb]:
                                    if trade_data.get('date'):
                                        try:
                                            trade_data['datetime_obj'] = datetime.strptime(trade_data['date'], '%Y-%m-%d %H:%M:%S')
                                        except ValueError:
                                            trade_data['datetime_obj'] = None
                                            messages.warning(request, f"Некорректный формат даты '{trade_data.get('date')}' (issue_nb: {current_issue_nb}) в файле {file_instance.original_filename} (год {file_instance.year}).")
                                    else:
                                        trade_data['datetime_obj'] = None
                                    
                                    full_instrument_trade_history[current_issue_nb].append(trade_data)
                                    seen_trades_for_instrument[current_issue_nb].add(trade_signature)
            except ET.ParseError:
                messages.warning(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} (год {file_instance.year}) при построении истории. Файл пропущен.")
                context['parsing_error_occurred'] = True
            except Exception as e:
                messages.warning(request, f"Ошибка при чтении файла {file_instance.original_filename} (год {file_instance.year}) для истории: {e}. Файл пропущен.")
                context['parsing_error_occurred'] = True
        
        for issue_nb in full_instrument_trade_history:
            full_instrument_trade_history[issue_nb].sort(key=lambda x: x.get('datetime_obj') or datetime.min)

        instruments_with_sales_in_target_year = set()
        files_for_target_report_year = UploadedXMLFile.objects.filter(user=user, year=target_report_year)

        if not files_for_target_report_year.exists():
             messages.info(request, f"У вас нет загруженных отчетов, соответствующих целевому году {target_report_year}, для определения инструментов с продажами.")
        
        for file_instance in files_for_target_report_year:
            try:
                with file_instance.xml_file.open('rb') as xml_file_content_stream: # Используем stream
                    content_bytes = xml_file_content_stream.read()
                    xml_string_loop = "" # Используем другую переменную
                    try:
                        xml_string_loop = content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            xml_string_loop = content_bytes.decode('windows-1251')
                        except UnicodeDecodeError:
                            messages.warning(f"Кодировка файла {file_instance.original_filename} (год {file_instance.year}) для поиска продаж не определена. Пропуск.")
                            context['parsing_error_occurred'] = True
                            continue
                    if not xml_string_loop: continue

                    root = ET.fromstring(xml_string_loop)
                    trades_element = root.find('.//trades')
                    if trades_element:
                        detailed_element = trades_element.find('detailed')
                        if detailed_element:
                            for node_element in detailed_element.findall('node'):
                                operation_el = node_element.find('operation')
                                issue_nb_el = node_element.find('issue_nb')
                                if (operation_el is not None and operation_el.text and
                                    operation_el.text.strip().lower() == 'sell' and
                                    issue_nb_el is not None and issue_nb_el.text and issue_nb_el.text.strip()):
                                    instruments_with_sales_in_target_year.add(issue_nb_el.text.strip())
            except ET.ParseError:
                messages.warning(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} (год {file_instance.year}) при поиске продаж. Файл пропущен.")
                context['parsing_error_occurred'] = True
            except Exception as e:
                messages.warning(request, f"Ошибка при обработке файла {file_instance.original_filename} (год {file_instance.year}) для поиска продаж: {e}. Файл пропущен.")
                context['parsing_error_occurred'] = True


        if not instruments_with_sales_in_target_year and files_for_target_report_year.exists():
            messages.info(request, f"Не найдено сделок продажи в ваших файлах, соответствующих целевому году {target_report_year}.")

        instrument_trade_history_filtered = {}
        if instruments_with_sales_in_target_year: # Только если были найдены инструменты с продажами
            for issue_nb, trades_list in full_instrument_trade_history.items():
                if issue_nb in instruments_with_sales_in_target_year:
                    instrument_trade_history_filtered[issue_nb] = trades_list
            
            if not instrument_trade_history_filtered and full_instrument_trade_history:
                 # Это сообщение может быть избыточным, если full_instrument_trade_history просто не содержит нужных issue_nb
                 # messages.info(request, f"Для инструментов с продажами в {target_report_year} году не найдено общей истории сделок (проверьте файлы за этот год или ранее).")
                 pass
        elif full_instrument_trade_history and not files_for_target_report_year.exists():
             # Если есть история, но нет файлов за целевой год, чтобы определить, что показывать
             messages.info(request, f"Загрузите отчет за {target_report_year} год, чтобы отфильтровать историю по инструментам с продажами в этом году.")
        
        context['instrument_trade_history'] = instrument_trade_history_filtered
        
        return render(request, 'reports_to_ndfl/upload.html', context)
    else: # GET request
        return render(request, 'reports_to_ndfl/upload.html', context)