# reports_to_ndfl/views.py
from django.shortcuts import render, redirect
from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict, deque 
import re
from django.contrib.auth.decorators import login_required
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP 
import logging

from .models import UploadedXMLFile
from currency_CBRF.models import Currency, ExchangeRate 
from currency_CBRF.services import fetch_daily_rates # fetch_daily_rates сам сохраняет на дату ответа ЦБ

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def parse_year_from_date_end(xml_string_content):
    try:
        match_attr = re.search(r'<broker_report[^>]*date_end="(\d{4})-\d{2}-\d{2}', xml_string_content)
        if match_attr:
            return int(match_attr.group(1))
        root = ET.fromstring(xml_string_content) 
        date_end_el = root.find('.//date_end')
        if date_end_el is not None and date_end_el.text:
            match_tag = re.match(r"(\d{4})", date_end_el.text.strip())
            if match_tag:
                return int(match_tag.group(1))
    except ET.ParseError:
        logger.warning("Ошибка парсинга XML при извлечении года из date_end (ParseError).")
    except Exception as e:
        logger.warning(f"Неожиданная ошибка при извлечении года из date_end: {e}")
    return None

def _get_exchange_rate_for_date(request, currency_obj, target_date_obj, rate_purpose_message=""):
    """
    Получает объект ExchangeRate для указанной валюты и ЗАПРАШИВАЕМОЙ ДАТЫ (target_date_obj).
    Если курс на target_date_obj не найден, пытается загрузить его с ЦБ.
    Если ЦБ возвращает курс на другую дату, этот курс сохраняется И создается "алиас"
    для target_date_obj с данными этого курса.
    Возвращает кортеж (ExchangeRate_object, was_exact_target_date_found_in_db_initially_or_aliased).
    """
    if not isinstance(target_date_obj, date):
        logger.error(f"VIEW: Передана не дата в _get_exchange_rate_for_date: {target_date_obj} для {currency_obj.char_code} {rate_purpose_message}")
        return None, False 

    # 1. Попытка найти курс на ТОЧНУЮ ЗАПРАШИВАЕМУЮ дату в БД
    exact_rate_obj = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
    if exact_rate_obj:
        # logger.info(f"VIEW: Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} НАЙДЕН в БД (прямое попадание).")
        return exact_rate_obj, True 

    # 2. Если на точную дату нет, вызываем fetch_daily_rates (который попытается загрузить и СОХРАНИТЬ на дату ответа ЦБ)
    messages.info(request, f"VIEW: Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} не найден в БД. Попытка загрузки с ЦБ РФ.")
    cbr_date_str_to_fetch = target_date_obj.strftime('%d/%m/%Y')
    
    # fetch_daily_rates сам обрабатывает сохранение на actual_rates_date_from_cbr.
    # Он возвращает (list_of_all_parsed_rates_from_xml, actual_rates_date_from_cbr)
    _, actual_rates_date_from_cbr = fetch_daily_rates(cbr_date_str_to_fetch)

    if actual_rates_date_from_cbr: 
        # 3. После вызова сервиса, пытаемся найти курс на ТОЧНУЮ ЗАПРАШИВАЕМУЮ дату.
        #    Он мог быть сохранен сервисом, если ЦБ ответил курсом на эту дату.
        rate_on_target_date_after_fetch = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
        if rate_on_target_date_after_fetch:
            # logger.info(f"VIEW: Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} НАЙДЕН в БД после вызова сервиса (точная дата).")
            return rate_on_target_date_after_fetch, True

        # 4. Если на ТОЧНУЮ ЗАПРАШИВАЕМУЮ дату все еще нет, НО сервис вернул курс на ДРУГУЮ ДАТУ (actual_rates_date_from_cbr),
        #    то создаем "алиас" для target_date_obj, используя данные с actual_rates_date_from_cbr.
        if actual_rates_date_from_cbr != target_date_obj:
            messages.info(request, f"VIEW: ЦБ РФ ответил курсами на {actual_rates_date_from_cbr.strftime('%d.%m.%Y')} для запроса на {target_date_obj.strftime('%d.%m.%Y')} ({currency_obj.char_code} {rate_purpose_message}).")
            
            # Ищем курс, который сервис сохранил на actual_rates_date_from_cbr
            rate_on_actual_cbr_date = ExchangeRate.objects.filter(currency=currency_obj, date=actual_rates_date_from_cbr).first()
            
            if rate_on_actual_cbr_date:
                # Создаем новую запись для target_date_obj, если ее еще нет
                # (проверка .exists() здесь избыточна, т.к. мы бы вышли на шаге 1 или после rate_on_target_date_after_fetch)
                try:
                    aliased_rate, created = ExchangeRate.objects.get_or_create(
                        currency=currency_obj,
                        date=target_date_obj,
                        defaults={
                            'value': rate_on_actual_cbr_date.value,
                            'nominal': rate_on_actual_cbr_date.nominal
                        }
                    )
                    if created:
                        messages.success(request, f"VIEW: Создан 'алиас' курса для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} используя данные от {actual_rates_date_from_cbr.strftime('%d.%m.%Y')}.")
                        logger.info(f"VIEW: Создан 'алиас' курса для {currency_obj.char_code} на {target_date_obj} с данными от {actual_rates_date_from_cbr}.")
                    return aliased_rate, True # Теперь курс на точную дату существует
                except Exception as e_alias:
                    logger.error(f"VIEW: Ошибка при создании 'алиаса' курса для {currency_obj.char_code} на {target_date_obj}: {e_alias}", exc_info=True)
            else:
                logger.warning(f"VIEW: Сервис вернул дату {actual_rates_date_from_cbr}, но курс для {currency_obj.char_code} на эту дату не найден в БД после сохранения сервисом.")

    # 5. Если после всех попыток (включая "алиасинг") курс на точную дату не найден,
    #    ищем ближайший предыдущий.
    final_fallback_rate = ExchangeRate.objects.filter(currency=currency_obj, date__lte=target_date_obj).order_by('-date').first()
    if final_fallback_rate:
        if final_fallback_rate.date != target_date_obj:
             messages.info(request, f"VIEW: Для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} используется ближайший курс от {final_fallback_rate.date.strftime('%d.%m.%Y')} из БД.")
        # else: # Если он на target_date_obj, то предыдущие шаги должны были его вернуть с флагом True
            # logger.info(f"VIEW: Используется курс на точную дату {target_date_obj} (найден как fallback).")
        return final_fallback_rate, final_fallback_rate.date == target_date_obj
    
    # Если ничего не найдено
    message_to_user = f"Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} не найден в БД даже после всех попыток."
    if not actual_rates_date_from_cbr and not exact_rate_obj : # Если сервис вообще ничего не вернул и вначале не было
        message_to_user = f"Критическая ошибка при попытке загрузить данные с ЦБ для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message}. Курс не найден."
        messages.error(request, message_to_user) 
    else:
        messages.warning(request, message_to_user)
    logger.warning(message_to_user)
    return None, False


# _calculate_fifo_costs_for_instrument остается таким же, как в артефакте django_views_py_fifo_costs_v3
def _calculate_fifo_costs_for_instrument(request, trades_list_sorted, issue_nb_for_messages):
    buy_lots_deque = deque() 
    for trade in trades_list_sorted:
        trade['fifo_cost_rub_str'] = None 
        trade['fifo_cost_rub_decimal'] = None 
        try:
            trade['q_decimal'] = Decimal(trade.get('q', '0'))
            trade['p_decimal'] = Decimal(trade.get('p', '0'))
            raw_commission = trade.get('commission')
            trade['commission_decimal'] = Decimal(raw_commission if raw_commission and raw_commission.strip() else '0')
        except InvalidOperation as e:
            messages.error(request, f"Ошибка конвертации числовых данных для сделки {trade.get('trade_id', 'N/A')} (инстр: {issue_nb_for_messages}): {e}. Расчет FIFO невозможен.")
            trade['fifo_cost_rub_str'] = "Ошибка данных"
            continue 
        operation = trade.get('operation', '').strip().lower()
        currency = trade.get('curr_c', '').strip().upper()
        if operation == 'buy':
            if trade['q_decimal'] <= 0: continue
            cost_in_currency = (trade['p_decimal'] * trade['q_decimal']) + trade['commission_decimal']
            cost_in_rub = cost_in_currency 
            if currency != 'RUB':
                rate_decimal_for_buy = trade.get('transaction_cbr_rate_decimal') 
                if rate_decimal_for_buy is not None: 
                    try: cost_in_rub = (cost_in_currency * rate_decimal_for_buy).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except InvalidOperation: 
                        messages.error(request, f"Ошибка при умножении на курс для покупки {trade.get('trade_id', 'N/A')} (инстр: {issue_nb_for_messages})."); continue 
                else:
                    messages.warning(request, f"Курс ЦБ для валюты {currency} на дату покупки {trade.get('date')} (сделка {trade.get('trade_id', 'N/A')}, инстр: {issue_nb_for_messages}) не был найден. Затраты по этой покупке не будут корректно учтены в FIFO в RUB."); continue 
            cost_per_share_rub = (cost_in_rub / trade['q_decimal']).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP) 
            buy_lots_deque.append({'q_remaining': trade['q_decimal'], 'cost_per_share_rub': cost_per_share_rub, 'original_trade_id': trade.get('trade_id', 'N/A') })
        elif operation == 'sell':
            if trade['q_decimal'] <= 0: continue
            sell_q_needed = trade['q_decimal']; cost_of_shares_sold_rub = Decimal(0); temp_q_covered_by_fifo = Decimal(0)
            while sell_q_needed > 0 and buy_lots_deque:
                buy_lot = buy_lots_deque[0]; q_to_take_from_lot = min(sell_q_needed, buy_lot['q_remaining'])
                cost_for_this_portion = (q_to_take_from_lot * buy_lot['cost_per_share_rub'])
                cost_of_shares_sold_rub += cost_for_this_portion; sell_q_needed -= q_to_take_from_lot; temp_q_covered_by_fifo += q_to_take_from_lot
                buy_lot['q_remaining'] -= q_to_take_from_lot
                if buy_lot['q_remaining'] <= Decimal('0.000001'): buy_lots_deque.popleft()
            commission_sell_rub = trade['commission_decimal'] 
            if currency != 'RUB':
                rate_decimal_for_sell = trade.get('transaction_cbr_rate_decimal')
                if rate_decimal_for_sell is not None:
                    try: commission_sell_rub = (trade['commission_decimal'] * rate_decimal_for_sell).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    except InvalidOperation: messages.error(request, f"Ошибка при пересчете комиссии за продажу {trade.get('trade_id', 'N/A')} в рубли (инстр: {issue_nb_for_messages}). Комиссия не будет добавлена к затратам."); commission_sell_rub = Decimal(0) 
                else: messages.warning(request, f"Курс ЦБ для валюты {currency} на дату продажи {trade.get('date')} (сделка {trade.get('trade_id', 'N/A')}, инстр: {issue_nb_for_messages}) не найден. Комиссия за продажу не будет пересчитана в рубли и добавлена к затратам."); commission_sell_rub = Decimal(0) 
            total_fifo_expenses_rub = cost_of_shares_sold_rub + commission_sell_rub
            total_fifo_expenses_rub = total_fifo_expenses_rub.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            trade['fifo_cost_rub_decimal'] = total_fifo_expenses_rub
            if sell_q_needed <= Decimal('0.000001'): trade['fifo_cost_rub_str'] = f"{total_fifo_expenses_rub:.2f}"
            else: 
                messages.warning(request, f"Недостаточно покупок для полного покрытия продажи {trade.get('trade_id', 'N/A')} (инстр: {issue_nb_for_messages}) от {trade.get('date')}. Требовалось: {trade['q_decimal']}, покрыто FIFO: {temp_q_covered_by_fifo}. Рассчитанная себестоимость акций для покрытой части: {cost_of_shares_sold_rub:.2f} RUB. Комиссия за продажу: {commission_sell_rub:.2f} RUB. Итого затраты (частично): {total_fifo_expenses_rub:.2f} RUB.")
                trade['fifo_cost_rub_str'] = f"Частично: {total_fifo_expenses_rub:.2f} RUB (акции для {temp_q_covered_by_fifo} шт. + комиссия продажи)"


def _process_and_get_trade_data(request, user, target_report_year):
    full_instrument_trade_history = defaultdict(list)
    seen_trades_for_instrument = defaultdict(set)
    parsing_error_in_process = False 
    relevant_files_for_history = UploadedXMLFile.objects.filter(user=user, year__lte=target_report_year).order_by('year', 'uploaded_at')
    if not relevant_files_for_history.exists():
        messages.info(request, f"У вас нет загруженных файлов с годом отчета {target_report_year} или ранее для анализа истории.")
        return {}, False
    trade_detail_tags = ['trade_id', 'date', 'operation', 'instr_nm', 'instr_type', 'instr_kind', 'p', 'curr_c', 'q', 'summ', 'commission', 'issue_nb']
    for file_instance in relevant_files_for_history:
        try:
            with file_instance.xml_file.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read(); xml_string_loop = "" 
                try: xml_string_loop = content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    try: xml_string_loop = content_bytes.decode('windows-1251')
                    except UnicodeDecodeError: messages.warning(request, f"Кодировка файла {file_instance.original_filename} (год {file_instance.year}) не определена. Пропуск для истории."); parsing_error_in_process = True; continue
                if not xml_string_loop: continue
                root = ET.fromstring(xml_string_loop); trades_element = root.find('.//trades')
                if trades_element:
                    detailed_element = trades_element.find('detailed')
                    if detailed_element:
                        for node_element in detailed_element.findall('node'):
                            issue_nb_el = node_element.find('issue_nb')
                            if not (issue_nb_el is not None and issue_nb_el.text and issue_nb_el.text.strip()): continue
                            current_issue_nb = issue_nb_el.text.strip(); trade_data = {'file_source': f"{file_instance.original_filename} (за {file_instance.year})"}
                            for tag in trade_detail_tags: data_el = node_element.find(tag); trade_data[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)
                            signature_fields = (trade_data.get('trade_id'), trade_data.get('date'), trade_data.get('operation'), trade_data.get('q'), trade_data.get('p'), current_issue_nb)
                            trade_signature = tuple(str(f).strip() if f is not None else "NONE_SIG" for f in signature_fields)
                            if trade_signature not in seen_trades_for_instrument[current_issue_nb]:
                                trade_datetime_obj = None
                                if trade_data.get('date'):
                                    try: trade_datetime_obj = datetime.strptime(trade_data['date'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError: messages.warning(request, f"Некорректный формат даты '{trade_data.get('date')}' (issue_nb: {current_issue_nb}) ...")
                                trade_data['datetime_obj'] = trade_datetime_obj
                                trade_data['transaction_cbr_rate_str'] = "-"; trade_data['transaction_cbr_rate_decimal'] = None
                                operation_type = trade_data.get('operation', '').strip().lower(); currency_code_from_xml = trade_data.get('curr_c', '').strip().upper()
                                if trade_datetime_obj:
                                    trade_date_obj_for_rate = trade_datetime_obj.date()
                                    if currency_code_from_xml in ['RUB', 'РУБ', 'РУБ.']:
                                        trade_data['transaction_cbr_rate_str'] = "1.0000"; trade_data['transaction_cbr_rate_decimal'] = Decimal("1.0000")
                                    elif currency_code_from_xml:
                                        try:
                                            currency_model_obj = Currency.objects.filter(char_code=currency_code_from_xml).first()
                                            if currency_model_obj:
                                                rate_purpose_msg = f"для сделки {trade_data.get('trade_id', 'N/A')} ({operation_type})"
                                                rate_model_obj, exact_date_found = _get_exchange_rate_for_date(request, currency_model_obj, trade_date_obj_for_rate, rate_purpose_msg)
                                                if rate_model_obj and hasattr(rate_model_obj, 'unit_rate') and rate_model_obj.unit_rate is not None:
                                                    trade_data['transaction_cbr_rate_decimal'] = rate_model_obj.unit_rate
                                                    trade_data['transaction_cbr_rate_str'] = f"{rate_model_obj.unit_rate:.4f}"
                                                    # Сообщение об использовании неточного курса теперь выводится внутри _get_exchange_rate_for_date, если это применимо
                                                else: 
                                                    trade_data['transaction_cbr_rate_str'] = "не найден"
                                                    # Сообщение об ошибке/предупреждение уже выведено из _get_exchange_rate_for_date
                                            else: 
                                                trade_data['transaction_cbr_rate_str'] = "валюта не найдена"
                                                messages.warning(request, f"Валюта {currency_code_from_xml} {rate_purpose_msg if 'rate_purpose_msg' in locals() else ''} не найдена в справочнике БД.")
                                        except Exception as e_rate_fetch: 
                                            logger.error(f"Ошибка при получении курса для {currency_code_from_xml} (сделка {trade_data.get('trade_id', 'N/A')}): {e_rate_fetch}", exc_info=True)
                                            trade_data['transaction_cbr_rate_str'] = "ошибка курса"
                                            messages.error(request, f"Ошибка получения курса для {currency_code_from_xml} (сделка {trade_data.get('trade_id', 'N/A')}).")
                                full_instrument_trade_history[current_issue_nb].append(trade_data); seen_trades_for_instrument[current_issue_nb].add(trade_signature)
        except ET.ParseError: messages.warning(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} ... Файл пропущен."); parsing_error_in_process = True; logger.warning(f"Ошибка парсинга XML в файле {file_instance.original_filename} ...", exc_info=True)
        except Exception as e: messages.warning(request, f"Ошибка при чтении/обработке файла {file_instance.original_filename} ...: {e}. Файл пропущен."); parsing_error_in_process = True; logger.error(f"Ошибка при чтении/обработке файла {file_instance.original_filename} ...: {e}", exc_info=True)
    for issue_nb, trades_for_instrument in full_instrument_trade_history.items():
        trades_for_instrument.sort(key=lambda x: x.get('datetime_obj') or datetime.min)
        _calculate_fifo_costs_for_instrument(request, trades_for_instrument, issue_nb)
    instruments_with_sales_in_target_year = set(); files_for_target_report_year = UploadedXMLFile.objects.filter(user=user, year=target_report_year); found_sales_in_target_year_files = False
    for file_instance in files_for_target_report_year:
        try:
            with file_instance.xml_file.open('rb') as xml_file_content_stream: 
                content_bytes = xml_file_content_stream.read(); xml_string_loop = "" 
                try: xml_string_loop = content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    try: xml_string_loop = content_bytes.decode('windows-1251')
                    except UnicodeDecodeError: messages.warning(request, f"Кодировка файла {file_instance.original_filename} (год {file_instance.year}) для поиска продаж не определена. Пропуск."); parsing_error_in_process = True; continue
                if not xml_string_loop: continue
                root = ET.fromstring(xml_string_loop); trades_element = root.find('.//trades')
                if trades_element:
                    detailed_element = trades_element.find('detailed')
                    if detailed_element:
                        for node_element in detailed_element.findall('node'):
                            operation_el = node_element.find('operation'); issue_nb_el = node_element.find('issue_nb')
                            if (operation_el is not None and operation_el.text and operation_el.text.strip().lower() == 'sell' and issue_nb_el is not None and issue_nb_el.text and issue_nb_el.text.strip()):
                                instruments_with_sales_in_target_year.add(issue_nb_el.text.strip()); found_sales_in_target_year_files = True
        except ET.ParseError: messages.warning(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} ... при поиске продаж. Файл пропущен."); parsing_error_in_process = True; logger.warning(f"Ошибка парсинга XML в файле {file_instance.original_filename} ... при поиске продаж.", exc_info=True)
        except Exception as e: messages.warning(request, f"Ошибка при обработке файла {file_instance.original_filename} ... для поиска продаж: {e}. Файл пропущен."); parsing_error_in_process = True; logger.error(f"Ошибка при обработке файла {file_instance.original_filename} ... для поиска продаж: {e}", exc_info=True)
    if files_for_target_report_year.exists() and not found_sales_in_target_year_files: messages.info(request, f"Не найдено сделок продажи в ваших файлах, соответствующих целевому году {target_report_year}.")
    instrument_trade_history_filtered = {}
    if instruments_with_sales_in_target_year: 
        for issue_nb, trades_list in full_instrument_trade_history.items():
            if issue_nb in instruments_with_sales_in_target_year: instrument_trade_history_filtered[issue_nb] = trades_list
        if not instrument_trade_history_filtered and full_instrument_trade_history: messages.info(request, f"Для инструментов с продажами в {target_report_year} году не найдено общей истории сделок (проверьте файлы за этот год или ранее).")
    return instrument_trade_history_filtered, parsing_error_in_process

@login_required
def upload_xml_file(request):
    user = request.user; context = {'target_report_year_for_title': request.session.get('last_target_year', None), 'instrument_trade_history': {}, 'parsing_error_occurred': False, 'previously_uploaded_files': UploadedXMLFile.objects.filter(user=user).order_by('-year', '-uploaded_at')}
    if request.method == 'POST':
        action = request.POST.get('action'); year_str_from_form = request.POST.get('year_for_process'); target_report_year = None
        if action == 'process_trades':
            if not year_str_from_form: messages.error(request, 'Пожалуйста, укажите целевой год для анализа сделок.'); return redirect('upload_xml_file')
            try: target_report_year = int(year_str_from_form); request.session['last_target_year'] = target_report_year 
            except ValueError: messages.error(request, 'Некорректный формат целевого года в форме.'); return redirect('upload_xml_file')
            instrument_trade_history, parsing_error = _process_and_get_trade_data(request, user, target_report_year)
            request.session['last_processed_year_for_trades'] = target_report_year; request.session['last_process_had_error'] = parsing_error; return redirect('upload_xml_file')
        elif action == 'upload_reports':
            uploaded_files_from_form = request.FILES.getlist('xml_file')
            if not uploaded_files_from_form: messages.error(request, 'Пожалуйста, выберите хотя бы один файл для загрузки.'); return redirect('upload_xml_file')
            parsing_error_in_upload = False 
            for uploaded_file_from_form in uploaded_files_from_form:
                original_name = uploaded_file_from_form.name; xml_string = ""; file_year_from_xml = None
                try:
                    content_bytes = uploaded_file_from_form.read(); uploaded_file_from_form.seek(0) 
                    try: xml_string = content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        try: xml_string = content_bytes.decode('windows-1251')
                        except UnicodeDecodeError: messages.error(request, f"Файл {original_name}: не удалось определить кодировку. Файл пропущен."); parsing_error_in_upload = True; continue 
                    if xml_string: file_year_from_xml = parse_year_from_date_end(xml_string)
                    if file_year_from_xml is None: messages.error(request, f"Файл {original_name}: не удалось извлечь год из XML. Файл пропущен."); parsing_error_in_upload = True; continue
                    if UploadedXMLFile.objects.filter(user=user, original_filename=original_name, year=file_year_from_xml).exists(): messages.warning(request, f"Файл '{original_name}' для {file_year_from_xml} года уже был загружен. Пропуск."); continue
                    instance = UploadedXMLFile(user=user, xml_file=uploaded_file_from_form, year=file_year_from_xml, original_filename=original_name); instance.save()
                    messages.success(request, f"Файл {original_name} (отчет за {file_year_from_xml} год) успешно загружен.")
                except Exception as e: messages.error(request, f"Ошибка при первичной обработке файла {original_name}: {e}. Файл пропущен."); parsing_error_in_upload = True; logger.error(f"Ошибка при первичной обработке файла {original_name}: {e}", exc_info=True)
            if parsing_error_in_upload: messages.warning(request, "При загрузке некоторых файлов возникли ошибки."); return redirect('upload_xml_file') 
        else: messages.error(request, "Неизвестное действие."); return redirect('upload_xml_file')
    else: # GET request
        last_processed_year = request.session.pop('last_processed_year_for_trades', None); last_process_had_error = request.session.pop('last_process_had_error', False)
        if last_processed_year is not None:
            context['target_report_year_for_title'] = last_processed_year
            instrument_trade_history, parsing_error_current_run = _process_and_get_trade_data(request, user, last_processed_year)
            context['instrument_trade_history'] = instrument_trade_history; context['parsing_error_occurred'] = parsing_error_current_run or last_process_had_error
        return render(request, 'reports_to_ndfl/upload.html', context)

