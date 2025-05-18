# reports_to_ndfl/views.py
from django.shortcuts import render, redirect
from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime, date 
from collections import defaultdict, deque
import re
from django.contrib.auth.decorators import login_required
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, Context
import logging

from .models import UploadedXMLFile
from currency_CBRF.models import Currency, ExchangeRate
from currency_CBRF.services import fetch_daily_rates

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

decimal_context = Context(prec=36, rounding=ROUND_HALF_UP)

PARSING_ERROR_MARKER = "CA_PARSING_ERROR"
NOT_A_RELEVANT_CONVERSION_MARKER = "CA_NOT_RELEVANT_CONVERSION"
_processing_had_error = [False]


def parse_year_from_date_end(xml_string_content):
    try:
        match_attr = re.search(r'<broker_report[^>]*date_end="(\d{4})-\d{2}-\d{2}', xml_string_content)
        if match_attr: return int(match_attr.group(1))
        root = ET.fromstring(xml_string_content)
        date_end_el = root.find('.//date_end')
        if date_end_el is not None and date_end_el.text:
            match_tag = re.match(r"(\d{4})", date_end_el.text.strip())
            if match_tag: return int(match_tag.group(1))
    except ET.ParseError: logger.warning("Ошибка парсинга XML при извлечении года (ParseError).")
    except Exception as e: logger.warning(f"Неожиданная ошибка при извлечении года: {e}")
    return None

def _get_exchange_rate_for_date(request, currency_obj, target_date_obj, rate_purpose_message=""):
    if not isinstance(target_date_obj, date):
        logger.error(f"VIEW: Передана не дата в _get_exchange_rate_for_date: {target_date_obj} для {currency_obj.char_code} {rate_purpose_message}")
        return None, False
    exact_rate_obj = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
    if exact_rate_obj: return exact_rate_obj, True
    
    cbr_date_str_to_fetch = target_date_obj.strftime('%d/%m/%Y')
    parsed_rates_list_from_service, actual_rates_date_from_cbr = fetch_daily_rates(cbr_date_str_to_fetch)
    if actual_rates_date_from_cbr:
        rate_on_target_date_after_fetch = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
        if rate_on_target_date_after_fetch: return rate_on_target_date_after_fetch, True
        if actual_rates_date_from_cbr != target_date_obj:
            rate_data_for_alias_creation = None
            if parsed_rates_list_from_service:
                for rate_info in parsed_rates_list_from_service:
                    if rate_info.get('char_code') == currency_obj.char_code:
                        rate_data_for_alias_creation = rate_info; break
            if rate_data_for_alias_creation:
                try:
                    aliased_rate, created = ExchangeRate.objects.get_or_create(
                        currency=currency_obj, date=target_date_obj,
                        defaults={'value': rate_data_for_alias_creation['value'], 'nominal': rate_data_for_alias_creation['nominal']}
                    )
                    if created: messages.info(request, f"Создан 'алиас' курса для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} исп. данные от {actual_rates_date_from_cbr.strftime('%d.%m.%Y')}.")
                    return aliased_rate, True
                except KeyError as e_key: logger.error(f"KeyError при создании 'алиаса' ({e_key}) для {currency_obj.char_code} на {target_date_obj}. Данные: {rate_data_for_alias_creation}", exc_info=True)
                except Exception as e_alias: logger.error(f"Ошибка при создании 'алиаса' курса для {currency_obj.char_code} на {target_date_obj}: {e_alias}", exc_info=True)
    
    final_fallback_rate = ExchangeRate.objects.filter(currency=currency_obj, date__lte=target_date_obj).order_by('-date').first()
    if final_fallback_rate:
        if final_fallback_rate.date != target_date_obj: messages.info(request, f"Для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} используется ближайший курс от {final_fallback_rate.date.strftime('%d.%m.%Y')}.")
        return final_fallback_rate, final_fallback_rate.date == target_date_obj
    
    message_to_user = f"Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} не найден."
    if not actual_rates_date_from_cbr : message_to_user = f"Критическая ошибка при загрузке с ЦБ. {message_to_user}"; messages.error(request, message_to_user)
    else: messages.warning(request, message_to_user)
    logger.warning(message_to_user); return None, False

def _parse_full_conversion_comment(comment_str):
    if not comment_str: return None
    pattern = re.compile(
        r"Conversion of securities\s+"
        r"(?P<old_ticker>[A-Z0-9\.\-\_]+)\s*\((?P<old_isin>[A-Z0-9]+)\)\s*->\s*"
        r"(?P<new_ticker>[A-Z0-9\.\-\_]+)\s*\((?P<new_isin>[A-Z0-9]+)\)",
        re.IGNORECASE
    )
    match = pattern.search(comment_str)
    if match:
        return {
            'old_ticker': match.group('old_ticker').strip(), 'old_isin': match.group('old_isin').strip(),
            'new_ticker': match.group('new_ticker').strip(), 'new_isin': match.group('new_isin').strip(),
        }
    return None

def _extract_ca_nodes_from_file(file_instance):
    ca_nodes_in_file = []
    corp_action_tags = ['date', 'type', 'type_id', 'corporate_action_id', 'amount', 'asset_type', 'ticker', 'isin', 'currency', 'ex_date', 'comment']
    try:
        with file_instance.xml_file.open('rb') as xml_file_content_stream:
            content_bytes = xml_file_content_stream.read()
            xml_string_loop = ""
            try: xml_string_loop = content_bytes.decode('utf-8')
            except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
            
            if xml_string_loop:
                root = ET.fromstring(xml_string_loop)
                corp_actions_element = root.find('.//corporate_actions')
                if corp_actions_element:
                    detailed_corp_element = corp_actions_element.find('detailed')
                    if detailed_corp_element:
                        for node_element in detailed_corp_element.findall('node'):
                            ca_data_item = {tag: (node_element.findtext(tag, '').strip() if node_element.find(tag) is not None else None) for tag in corp_action_tags}
                            ca_data_item['file_source'] = f"{file_instance.original_filename} (за {file_instance.year})"
                            ca_nodes_in_file.append(ca_data_item)
    except Exception as e:
        logger.error(f"Ошибка при извлечении КД из файла {file_instance.original_filename}: {e}")
    return ca_nodes_in_file

def _parse_and_validate_ca_node_on_demand(request, raw_ca_node_data, ca_nodes_from_same_file):
    global _processing_had_error
    if not (raw_ca_node_data.get('type_id') == 'conversion' and \
            'Бумаги' in raw_ca_node_data.get('asset_type', '')):
        return NOT_A_RELEVANT_CONVERSION_MARKER

    ca_date_str = raw_ca_node_data.get('date')
    ca_datetime_obj = None # Будет date
    if ca_date_str:
        try: 
            ca_datetime_obj = datetime.strptime(ca_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"ON-DEMAND PARSE: Некорректная дата '{ca_date_str}' для КД {raw_ca_node_data.get('corporate_action_id')}")
            _processing_had_error[0] = True; return PARSING_ERROR_MARKER
    if not ca_datetime_obj: _processing_had_error[0] = True; return PARSING_ERROR_MARKER

    amount_in_ca_node_str = raw_ca_node_data.get('amount', '0')
    try: quantity_in_node = Decimal(amount_in_ca_node_str)
    except InvalidOperation:
        logger.warning(f"ON-DEMAND PARSE: Некорректное количество '{amount_in_ca_node_str}' для КД {raw_ca_node_data.get('corporate_action_id')}")
        _processing_had_error[0] = True; return PARSING_ERROR_MARKER
    
    if quantity_in_node <= 0: return NOT_A_RELEVANT_CONVERSION_MARKER 

    comment_text = raw_ca_node_data.get('comment', '')
    isin_in_ca_node = raw_ca_node_data.get('isin','').strip()
    corp_action_id_from_node = raw_ca_node_data.get('corporate_action_id')
    conversion_details_from_comment = _parse_full_conversion_comment(comment_text)

    if not (conversion_details_from_comment and \
            conversion_details_from_comment['new_isin'] == isin_in_ca_node and \
            conversion_details_from_comment.get('old_isin')):
        return NOT_A_RELEVANT_CONVERSION_MARKER 

    old_isin_from_comment = conversion_details_from_comment['old_isin']
    actual_old_quantity_removed = Decimal(0)
    found_removal_event = False

    for removal_ca_data in ca_nodes_from_same_file: 
        if removal_ca_data.get('corporate_action_id') == corp_action_id_from_node and \
           removal_ca_data.get('isin') == old_isin_from_comment and \
           removal_ca_data.get('type_id') == 'conversion':
            try:
                removed_qty_val = Decimal(removal_ca_data.get('amount', '0'))
                if removed_qty_val < 0:
                    actual_old_quantity_removed = abs(removed_qty_val)
                    found_removal_event = True; break
            except InvalidOperation:
                logger.warning(f"ON-DEMAND PARSE: Не удалось преобразовать кол-во '{removal_ca_data.get('amount')}' для списания КД {corp_action_id_from_node}")
    
    if not found_removal_event:
        error_message = (f"Критическая ошибка (ON-DEMAND PARSE) для КД ID: {corp_action_id_from_node} в файле {raw_ca_node_data.get('file_source')}: "
                         f"Зачислено {quantity_in_node} шт. {isin_in_ca_node} (старый ISIN: {old_isin_from_comment}). "
                         f"Не найдено парное СПИСАНИЕ старых бумаг {old_isin_from_comment} в том же файле. Конвертация не будет применена.")
        messages.error(request, error_message)
        logger.error(error_message)
        _processing_had_error[0] = True
        return PARSING_ERROR_MARKER

    parsed_event_for_fifo = {
        'datetime_obj': ca_datetime_obj, 'new_isin': isin_in_ca_node, 'new_quantity': quantity_in_node,
        'old_isin': old_isin_from_comment, 'corp_action_id': corp_action_id_from_node, 'comment': comment_text
    }
    display_event_data = {
        'display_type': 'conversion_info', 'datetime_obj': ca_datetime_obj,
        'old_isin': old_isin_from_comment, 'old_ticker': conversion_details_from_comment.get('old_ticker'),
        'old_quantity_removed': actual_old_quantity_removed,
        'new_isin': isin_in_ca_node, 'new_ticker': conversion_details_from_comment.get('new_ticker'),
        'new_quantity_received': quantity_in_node,
        'ratio_comment': comment_text, 'corp_action_id': corp_action_id_from_node,
        'file_source': raw_ca_node_data.get('file_source', 'N/A')
    }
    return {'fifo_data': parsed_event_for_fifo, 'display_data': display_event_data}

def _apply_conversion_on_demand(request, target_isin, operation_date, buy_lots_deques,
                                relevant_files_for_history, applied_corp_action_ids, 
                                memoized_parsed_ca_results, conversion_events_for_display_accumulator,
                                file_ca_nodes_cache):
    global _processing_had_error
    conversion_applied_this_call = False
    
    for file_instance in relevant_files_for_history:
        file_id = file_instance.id
        if file_id not in file_ca_nodes_cache:
            file_ca_nodes_cache[file_id] = _extract_ca_nodes_from_file(file_instance)
        current_file_raw_cas = file_ca_nodes_cache[file_id]
        if not current_file_raw_cas: continue

        for raw_ca_item_data in current_file_raw_cas:
            ca_id = raw_ca_item_data.get('corporate_action_id')
            if not ca_id or ca_id in applied_corp_action_ids: continue

            parsed_ca_info = memoized_parsed_ca_results.get(ca_id)
            if parsed_ca_info is None:
                parsed_ca_info = _parse_and_validate_ca_node_on_demand(request, raw_ca_item_data, current_file_raw_cas)
                memoized_parsed_ca_results[ca_id] = parsed_ca_info
            
            if parsed_ca_info in [PARSING_ERROR_MARKER, NOT_A_RELEVANT_CONVERSION_MARKER, None]: continue

            ca_event_fifo_data = parsed_ca_info['fifo_data']
            if ca_event_fifo_data['new_isin'] == target_isin and \
               ca_event_fifo_data['datetime_obj'] <= operation_date: 
                old_isin = ca_event_fifo_data['old_isin']
                new_isin = ca_event_fifo_data['new_isin']
                new_quantity_from_ca = ca_event_fifo_data['new_quantity']
                conversion_date = ca_event_fifo_data['datetime_obj']

                logger.info(f"FIFO (ON-DEMAND): Применяется конвертация (ID: {ca_id}): {old_isin} -> {new_quantity_from_ca} {new_isin} на {conversion_date} из файла {raw_ca_item_data.get('file_source')}.")
                total_cost_basis_of_old_shares_rub = Decimal(0)
                total_qty_of_old_shares_removed = Decimal(0)
                old_shares_queue = buy_lots_deques[old_isin]

                if not old_shares_queue:
                    logger.warning(f"FIFO (ON-DEMAND): Нет акций {old_isin} для списания при конвертации в {new_isin} (ID: {ca_id}).")
                
                while old_shares_queue:
                    buy_lot = old_shares_queue.popleft()
                    total_cost_basis_of_old_shares_rub += decimal_context.multiply(buy_lot['q_remaining'], buy_lot['cost_per_share_rub'])
                    total_qty_of_old_shares_removed += buy_lot['q_remaining']

                if total_qty_of_old_shares_removed > 0:
                     logger.info(f"FIFO (ON-DEMAND): Для конвертации (ID: {ca_id}) списано ВСЕГО {total_qty_of_old_shares_removed} шт. {old_isin} общей стоимостью {total_cost_basis_of_old_shares_rub.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP):.2f} RUB.")

                if new_quantity_from_ca > 0:
                    cost_per_new_share_rub = Decimal(0)
                    if total_qty_of_old_shares_removed > 0 : 
                        cost_per_new_share_rub = decimal_context.divide(total_cost_basis_of_old_shares_rub, new_quantity_from_ca)
                    
                    new_lot = {
                        'q_remaining': new_quantity_from_ca,
                        'cost_per_share_rub': cost_per_new_share_rub.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP),
                        'date': conversion_date, 
                        'original_trade_id': f"CONV_IN_{ca_id}"
                    }
                    inserted = False; target_queue = buy_lots_deques[new_isin]
                    for i_idx in range(len(target_queue)): 
                        if conversion_date < target_queue[i_idx]['date']: 
                            target_queue.insert(i_idx, new_lot); inserted = True; break
                    if not inserted: target_queue.append(new_lot)
                    logger.info(f"FIFO (ON-DEMAND): В результате конвертации (ID: {ca_id}) зачислено {new_quantity_from_ca} шт. {new_isin} по ~{cost_per_new_share_rub:.6f} RUB/шт.")
                    
                    conversion_events_for_display_accumulator.append(parsed_ca_info['display_data'])
                    conversion_applied_this_call = True
                elif new_quantity_from_ca == 0 and total_qty_of_old_shares_removed > 0:
                     messages.warning(request, f"При конвертации (ID: {ca_id}) было списано {total_qty_of_old_shares_removed} шт. {old_isin}, но не получено новых акций {new_isin}.")
                
                applied_corp_action_ids.add(ca_id)
                if conversion_applied_this_call: return True
    return False

def _process_all_operations_for_fifo(request, operations_to_process, 
                                     full_trade_history_map_for_fifo_update, 
                                     relevant_files_for_history,
                                     conversion_events_for_display_accumulator):
    global _processing_had_error
    buy_lots_deques = defaultdict(deque)
    applied_corp_action_ids = set()
    memoized_parsed_ca_results = {}
    file_ca_nodes_cache = {}

    for op in operations_to_process:
        op_type = op.get('op_type')
        op_isin = op.get('isin')
        op_date = op.get('datetime_obj').date() if op.get('datetime_obj') else date.min
        trade_dict_ref = op.get('original_trade_dict_ref') if op_type == 'trade' else None
        
        if op.get('operation_type') == 'buy' or op_type == 'initial_holding':
            if op['quantity'] <= 0: continue # quantity здесь Decimal
            if op_type == 'initial_holding': cost_in_rub = op['total_cost_rub'] # Decimal
            else: 
                # price_per_share, quantity, commission здесь Decimal
                cost_in_currency = (op['price_per_share'] * op['quantity']) + op['commission'] 
                cost_in_rub = cost_in_currency
                if op['currency'] != 'RUB':
                    if op['cbr_rate_decimal'] is not None: # Decimal
                        cost_in_rub = (cost_in_currency * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    else: 
                        if trade_dict_ref: trade_dict_ref['fifo_cost_rub_str'] = "Ошибка курса покупки (FIFO)"
                        logger.error(f"FIFO: Непредвиденная ошибка курса для покупки {op.get('trade_id','N/A')}")
                        _processing_had_error[0] = True; continue 
            cost_per_share_rub = (cost_in_rub / op['quantity']).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            buy_lots_deques[op_isin].append({
                'q_remaining': op['quantity'], 'cost_per_share_rub': cost_per_share_rub, 
                'date': op_date, 
                'original_trade_id': op.get('trade_id', 'INITIAL' if op_type == 'initial_holding' else 'BUY_NO_ID')
            })
            if op_type == 'initial_holding': logger.info(f"FIFO: Добавлен нач. остаток {op_isin}: {op['quantity']} @ {cost_per_share_rub:.6f} RUB")

        elif op.get('operation_type') == 'sell':
            if not trade_dict_ref: logger.warning(f"FIFO: Пропуск продажи без trade_dict_ref: {op}"); continue
            if op['quantity'] <= 0: # quantity здесь Decimal
                trade_dict_ref['fifo_cost_rub_str'] = "0.00 (нулевое кол-во)"; trade_dict_ref['fifo_cost_rub_decimal'] = Decimal(0); continue
            
            trade_dict_ref['fifo_cost_rub_str'] = None; trade_dict_ref['fifo_cost_rub_decimal'] = None
            sell_q_to_cover = op['quantity'] # Decimal
            final_cost_of_shares_sold_rub = Decimal(0)
            final_q_covered_by_fifo = Decimal(0)
            max_conversion_attempts = 7
            attempt_count = 0

            while sell_q_to_cover > Decimal('0.000001') and attempt_count < max_conversion_attempts:
                attempt_count += 1
                current_buy_queue = buy_lots_deques[op_isin]
                qty_covered_in_this_pass = Decimal(0); cost_in_this_pass = Decimal(0)

                while sell_q_to_cover > Decimal('0.000001') and current_buy_queue:
                    buy_lot = current_buy_queue[0] 
                    q_to_take_from_lot = min(sell_q_to_cover, buy_lot['q_remaining'])
                    cost_for_this_portion = (q_to_take_from_lot * buy_lot['cost_per_share_rub'])
                    cost_in_this_pass += cost_for_this_portion
                    sell_q_to_cover -= q_to_take_from_lot
                    qty_covered_in_this_pass += q_to_take_from_lot
                    buy_lot['q_remaining'] -= q_to_take_from_lot
                    if buy_lot['q_remaining'] <= Decimal('0.000001'): current_buy_queue.popleft()

                final_cost_of_shares_sold_rub += cost_in_this_pass
                final_q_covered_by_fifo += qty_covered_in_this_pass

                if sell_q_to_cover <= Decimal('0.000001'): break
                
                was_conversion_applied = _apply_conversion_on_demand(
                    request, op_isin, op_date, buy_lots_deques,
                    relevant_files_for_history,
                    applied_corp_action_ids, memoized_parsed_ca_results,
                    conversion_events_for_display_accumulator, file_ca_nodes_cache
                )
                if not was_conversion_applied: break
            
            if attempt_count >= max_conversion_attempts and sell_q_to_cover > Decimal('0.000001'):
                logger.warning(f"FIFO: Достигнуто макс. число попыток ({max_conversion_attempts}) применения конвертаций для продажи {op.get('trade_id','N/A')} ({op_isin}). Непокрытое кол-во: {sell_q_to_cover}")
            commission_sell_rub = op['commission'] # Decimal
            if op['currency'] != 'RUB':
                if op['cbr_rate_decimal'] is not None: # Decimal
                    commission_sell_rub = (op['commission'] * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    messages.error(request, f"Нет курса для расчета комиссии продажи {op.get('trade_id','N/A')} ({op_isin}).")
                    _processing_had_error[0] = True; commission_sell_rub = Decimal(0) 
            total_fifo_expenses_rub = final_cost_of_shares_sold_rub + commission_sell_rub
            total_fifo_expenses_rub = total_fifo_expenses_rub.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            trade_dict_ref['fifo_cost_rub_decimal'] = total_fifo_expenses_rub
            if sell_q_to_cover <= Decimal('0.000001'): 
                trade_dict_ref['fifo_cost_rub_str'] = f"{total_fifo_expenses_rub:.2f}"
            else: 
                uncovered_qty = op['quantity'] - final_q_covered_by_fifo
                msg = (f"Недостаточно покупок/конвертаций для продажи {op.get('trade_id','N/A')} ({op_isin}). "
                       f"Требовалось: {op['quantity']}, покрыто FIFO: {final_q_covered_by_fifo} (не покрыто: {uncovered_qty}). "
                       f"FIFO затраты (по покрытой части + полная комиссия): {total_fifo_expenses_rub:.2f} RUB.")
                messages.warning(request, msg); logger.warning(msg)
                trade_dict_ref['fifo_cost_rub_str'] = f"Частично: {total_fifo_expenses_rub:.2f} (для {final_q_covered_by_fifo} из {op['quantity']} шт.)"

def _str_to_decimal_safe(val_str, field_name_for_log="", trade_id_for_log=""):
    """Преобразует строку в Decimal, обрабатывая None и пустые строки как 0."""
    if val_str is None:
        return Decimal(0)
    if isinstance(val_str, str) and not val_str.strip():
        return Decimal(0)
    try:
        return Decimal(str(val_str)) # str() для обработки если val_str уже число
    except InvalidOperation:
        logger.error(f"Ошибка преобразования '{field_name_for_log}' в Decimal: '{val_str}' для trade_id: {trade_id_for_log}")
        return Decimal(0)


def _process_and_get_trade_data(request, user, target_report_year):
    global _processing_had_error
    _processing_had_error[0] = False 

    full_instrument_trade_history_for_fifo = defaultdict(list)
    trade_and_holding_ops = [] 
    
    relevant_files_for_history = UploadedXMLFile.objects.filter(user=user, year__lte=target_report_year).order_by('year', 'uploaded_at')
    if not relevant_files_for_history.exists():
        messages.info(request, f"У вас нет загруженных файлов с годом отчета {target_report_year} или ранее для анализа истории.")
        return {}, _processing_had_error[0]

    trade_detail_tags = ['trade_id', 'date', 'operation', 'instr_nm', 'instr_type', 'instr_kind', 'p', 'curr_c', 'q', 'summ', 'commission', 'issue_nb']
    
    earliest_report_start_datetime = None 
    if relevant_files_for_history:
        first_file_instance = relevant_files_for_history.first()
        try:
            with first_file_instance.xml_file.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read(); xml_string_temp = ""
                try: xml_string_temp = content_bytes.decode('utf-8')
                except UnicodeDecodeError: xml_string_temp = content_bytes.decode('windows-1251', errors='replace')
                if xml_string_temp:
                    root_temp = ET.fromstring(xml_string_temp)
                    date_start_el_temp = root_temp.find('.//date_start')
                    if date_start_el_temp is not None and date_start_el_temp.text:
                        earliest_report_start_datetime = datetime.strptime(date_start_el_temp.text.strip(), '%Y-%m-%d %H:%M:%S')
        except Exception as e_early_date:
            logger.error(f"Не удалось определить самую раннюю дату начала отчета из {first_file_instance.original_filename}: {e_early_date}")
            _processing_had_error[0] = True
    
    processed_initial_holdings_file_ids = set()
    for file_instance in relevant_files_for_history:
        try:
            with file_instance.xml_file.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read(); xml_string_loop = ""
                try: xml_string_loop = content_bytes.decode('utf-8')
                except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
                if not xml_string_loop: continue
                root = ET.fromstring(xml_string_loop)

                current_file_date_start_str = root.findtext('.//date_start', default='').strip()
                current_file_start_dt = None 
                if current_file_date_start_str:
                    try: current_file_start_dt = datetime.strptime(current_file_date_start_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError: logger.warning(f"Некорректная date_start {current_file_date_start_str} в {file_instance.original_filename}")

                if earliest_report_start_datetime and current_file_start_dt == earliest_report_start_datetime and file_instance.id not in processed_initial_holdings_file_ids:
                    account_at_start_el = root.find('.//account_at_start')
                    if account_at_start_el is not None:
                        positions_el = account_at_start_el.find('.//positions_from_ts/ps/pos')
                        if positions_el: 
                            for pos_node in positions_el.findall('node'): 
                                try:
                                    isin_el = pos_node.find('issue_nb') 
                                    isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                    if not isin: 
                                        instr_nm_log = pos_node.findtext('name', 'N/A').strip()
                                        logger.info(f"Пропуск НО в {file_instance.original_filename} для '{instr_nm_log}': отсутствует или невалидный ISIN (в <issue_nb>).")
                                        continue 
                                    
                                    quantity = _str_to_decimal_safe(pos_node.findtext('q', '0'), 'q НО', isin)
                                    if quantity <= 0: continue
                                    bal_price_per_share_curr = _str_to_decimal_safe(pos_node.findtext('bal_price_a', '0'), 'bal_price_a НО', isin)
                                    currency_code = pos_node.findtext('curr', 'RUB').strip().upper()
                                    rate_decimal_init = Decimal("1.0")
                                    total_cost_rub_init = (quantity * bal_price_per_share_curr) # Будет Decimal
                                    
                                    if currency_code != 'RUB':
                                        currency_model_init = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model_init and earliest_report_start_datetime: 
                                            rate_obj_init, _ = _get_exchange_rate_for_date(request, currency_model_init, earliest_report_start_datetime.date(), f"для НО {isin}")
                                            if rate_obj_init and rate_obj_init.unit_rate is not None:
                                                rate_decimal_init = rate_obj_init.unit_rate
                                                total_cost_rub_init = (quantity * bal_price_per_share_curr * rate_decimal_init).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                            else: _processing_had_error[0] = True; messages.warning(request, f"Не найден курс для НО {isin} ({currency_code}) на {earliest_report_start_datetime.date().strftime('%d.%m.%Y') if earliest_report_start_datetime else 'N/A'}.")
                                        else: _processing_had_error[0] = True; messages.warning(request, f"Валюта {currency_code} для НО {isin} не найдена.")
                                    
                                    op_details_dict_for_ref = {
                                        'date': earliest_report_start_datetime.strftime('%Y-%m-%d %H:%M:%S') if earliest_report_start_datetime else "N/A",
                                        'trade_id': f'INITIAL_{isin}', 'operation': 'initial_holding',
                                        'instr_nm': pos_node.findtext('name', isin).strip(), 
                                        'isin': isin,
                                        'p': bal_price_per_share_curr, # Decimal
                                        'curr_c': currency_code, 
                                        'q': quantity, # Decimal
                                        'summ': quantity * bal_price_per_share_curr, # Decimal
                                        'commission': Decimal(0), # Decimal
                                        'transaction_cbr_rate_str': f"{rate_decimal_init:.4f}" if rate_decimal_init else "-",
                                        'file_source': f"Нач. остаток из {file_instance.original_filename}",
                                        'total_cost_rub_str': f"{total_cost_rub_init:.2f}" 
                                    }
                                    trade_and_holding_ops.append({
                                        'op_type': 'initial_holding', 
                                        'datetime_obj': earliest_report_start_datetime, 
                                        'isin': isin, 'quantity': quantity, 'price_per_share': bal_price_per_share_curr,
                                        'total_cost_rub': total_cost_rub_init, 'commission': Decimal(0), 'currency': currency_code,
                                        'cbr_rate_decimal': rate_decimal_init,
                                        'original_trade_dict_ref': op_details_dict_for_ref,
                                        'operation_type': 'buy', 
                                        'file_source': op_details_dict_for_ref['file_source']
                                    })
                                    logger.info(f"Добавлен начальный остаток: {quantity} {isin} на {earliest_report_start_datetime.date() if earliest_report_start_datetime else 'N/A'} с RUB стоимостью {total_cost_rub_init}")
                                except (AttributeError, ValueError, InvalidOperation) as e_init: # InvalidOperation уже обрабатывается в _str_to_decimal_safe
                                     _processing_had_error[0] = True; logger.error(f"Ошибка парсинга НО в {file_instance.original_filename}: {e_init}", exc_info=True)
                        processed_initial_holdings_file_ids.add(file_instance.id)

                trades_element = root.find('.//trades')
                if trades_element:
                    detailed_element = trades_element.find('detailed')
                    if detailed_element:
                        for node_element in detailed_element.findall('node'):
                            trade_data_dict = {'file_source': f"{file_instance.original_filename} (за {file_instance.year})"}
                            current_trade_id_for_log = node_element.findtext('trade_id', 'N/A') # Для логирования ошибок

                            try:
                                instr_type_el = node_element.find('instr_type')
                                instr_type_val = instr_type_el.text.strip() if instr_type_el is not None and instr_type_el.text else None
                                if instr_type_val != '1': continue 
                                
                                isin_el = node_element.find('isin') 
                                current_isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                if not current_isin:
                                    date_log = node_element.findtext('date', 'N/A')
                                    instr_nm_log = node_element.findtext('instr_nm', 'N/A')
                                    logger.warning(f"Пропуск сделки с ценной бумагой (ID: {current_trade_id_for_log}, Дата: {date_log}, Инстр: {instr_nm_log}) в файле {file_instance.original_filename}: отсутствует или невалидный ISIN в теге <isin>.")
                                    _processing_had_error[0] = True
                                    continue
                                trade_data_dict['isin'] = current_isin
                                
                                for tag in trade_detail_tags: 
                                    data_el = node_element.find(tag)
                                    trade_data_dict[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)
                                
                                # Преобразование ключевых числовых полей в Decimal
                                trade_data_dict['p'] = _str_to_decimal_safe(trade_data_dict.get('p'), 'p', current_trade_id_for_log)
                                trade_data_dict['q'] = _str_to_decimal_safe(trade_data_dict.get('q'), 'q', current_trade_id_for_log)
                                trade_data_dict['summ'] = _str_to_decimal_safe(trade_data_dict.get('summ'), 'summ', current_trade_id_for_log)
                                trade_data_dict['commission'] = _str_to_decimal_safe(trade_data_dict.get('commission'), 'commission', current_trade_id_for_log)
                                
                                op_datetime_obj = None 
                                if trade_data_dict.get('date'):
                                    try: op_datetime_obj = datetime.strptime(trade_data_dict['date'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError: _processing_had_error[0] = True; messages.warning(request, f"Некорректная дата сделки {current_trade_id_for_log} ({current_isin})."); continue
                                if not op_datetime_obj: _processing_had_error[0] = True; messages.warning(request, f"Отсутствует дата для сделки {current_trade_id_for_log} ({current_isin})."); continue

                                rate_decimal, rate_str = None, "-"
                                currency_code = trade_data_dict.get('curr_c', '').strip().upper()
                                if currency_code:
                                    if currency_code in ['RUB', 'РУБ', 'РУБ.']: rate_decimal, rate_str = Decimal("1.0000"), "1.0000"
                                    else:
                                        currency_model = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model:
                                            rate_obj_trade, fetched_exactly = _get_exchange_rate_for_date(request, currency_model, op_datetime_obj.date(), f"для сделки {current_trade_id_for_log}")
                                            if rate_obj_trade and rate_obj_trade.unit_rate is not None:
                                                rate_decimal = rate_obj_trade.unit_rate; rate_str = f"{rate_decimal:.4f}"
                                                if not fetched_exactly: rate_str += " (ближ.)"
                                            else: _processing_had_error[0] = True; rate_str = "не найден"; messages.error(request, f"Курс {currency_code} не найден для сделки {current_trade_id_for_log} на {op_datetime_obj.date().strftime('%d.%m.%Y')}.")
                                        else: _processing_had_error[0] = True; rate_str = "валюта не найдена"; messages.error(request, f"Валюта {currency_code} не найдена для сделки {current_trade_id_for_log}.")
                                trade_data_dict['transaction_cbr_rate_str'] = rate_str
                                if currency_code != 'RUB' and rate_decimal is None: _processing_had_error[0] = True; logger.error(f"Пропуск сделки {current_trade_id_for_log} в FIFO (нет курса {currency_code})."); continue 
                                
                                full_instrument_trade_history_for_fifo[current_isin].append(trade_data_dict)

                                op_for_processing = {
                                    'op_type': 'trade', 
                                    'datetime_obj': op_datetime_obj, 
                                    'isin': current_isin,
                                    'trade_id': trade_data_dict.get('trade_id'),
                                    'operation_type': trade_data_dict.get('operation', '').strip().lower(),
                                    'quantity': trade_data_dict['q'], # Decimal
                                    'price_per_share': trade_data_dict['p'], # Decimal
                                    'commission': trade_data_dict['commission'], # Decimal
                                    'currency': currency_code,
                                    'cbr_rate_decimal': rate_decimal, # Decimal or None
                                    'original_trade_dict_ref': trade_data_dict, 
                                    'file_source': trade_data_dict['file_source']
                                }
                                trade_and_holding_ops.append(op_for_processing)
                            except Exception as e_node: # Общий обработчик для ошибок внутри цикла по node
                                _processing_had_error[0] = True
                                logger.error(f"Ошибка обработки узла сделки (ID: {current_trade_id_for_log}) в {file_instance.original_filename}: {e_node}", exc_info=True)
                                messages.error(request, f"Ошибка данных для сделки ID: {current_trade_id_for_log} в файле {file_instance.original_filename}.")
                                continue
        except ET.ParseError: _processing_had_error[0] = True; logger.warning(f"Ошибка парсинга XML: {file_instance.original_filename}", exc_info=True); messages.error(request, f"Ошибка парсинга XML в файле {file_instance.original_filename}.")
        except Exception as e: _processing_had_error[0] = True; logger.error(f"Ошибка обработки файла {file_instance.original_filename}: {e}", exc_info=True); messages.error(request, f"Неожиданная ошибка при обработке файла {file_instance.original_filename}.")

    conversion_events_for_display_accumulator = [] 
    trade_and_holding_ops.sort(key=lambda x: x.get('datetime_obj') or datetime.min)

    if trade_and_holding_ops:
        _process_all_operations_for_fifo(
            request, trade_and_holding_ops, 
            full_instrument_trade_history_for_fifo, 
            relevant_files_for_history, 
            conversion_events_for_display_accumulator
        )

    all_display_events = [] 
    for isin_key, trades_list_for_isin in full_instrument_trade_history_for_fifo.items():
        for trade_dict_updated_with_fifo in trades_list_for_isin: 
            dt_obj = datetime.min
            if trade_dict_updated_with_fifo.get('date'):
                try: dt_obj = datetime.strptime(trade_dict_updated_with_fifo['date'], '%Y-%m-%d %H:%M:%S')
                except ValueError: logger.warning(f"Некорректная дата в trade_dict_updated_with_fifo: {trade_dict_updated_with_fifo.get('date')}")
            all_display_events.append({
                'display_type': 'trade', 'datetime_obj': dt_obj, 
                'event_details': trade_dict_updated_with_fifo, # Содержит Decimal поля
                'isin_group_key': trade_dict_updated_with_fifo.get('isin') 
            })

    processed_no_refs_ids = set() 
    for op in trade_and_holding_ops: 
        if op.get('op_type') == 'initial_holding' and op.get('original_trade_dict_ref'):
            ref_id_check = id(op['original_trade_dict_ref'])
            if ref_id_check not in processed_no_refs_ids:
                all_display_events.append({
                    'display_type': 'initial_holding', 'datetime_obj': op['datetime_obj'], 
                    'event_details': op['original_trade_dict_ref'],  # Содержит Decimal поля
                    'isin_group_key': op.get('isin')
                })
                processed_no_refs_ids.add(ref_id_check)
    
    for conv_event_data in conversion_events_for_display_accumulator: 
        all_display_events.append({
            'display_type': 'conversion_info', 'datetime_obj': conv_event_data['datetime_obj'], 
            'event_details': conv_event_data,
            'isin_group_key': conv_event_data.get('new_isin')
        })
    
    # --- BEGIN NEW AGGREGATION LOGIC ---
    processed_events_for_aggregation = []
    loop_idx = 0 

    all_display_events.sort(key=lambda x: (
        x['event_details'].get('isin') if x.get('display_type') == 'trade' and x.get('event_details') else x.get('isin_group_key', ''), 
        (x.get('datetime_obj').date() if isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) if x.get('display_type') == 'trade' and x.get('datetime_obj') else (x.get('datetime_obj') or date.min),
        x['event_details'].get('p') if x.get('display_type') == 'trade' and x.get('event_details') else None, 
        x['event_details'].get('operation','').lower() if x.get('display_type') == 'trade' and x.get('event_details') else '', 
        x['event_details'].get('curr_c') if x.get('display_type') == 'trade' and x.get('event_details') else '', 
        (datetime.combine(x.get('datetime_obj'), datetime.min.time()) if isinstance(x.get('datetime_obj'), date) and not isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) or datetime.min
    ))

    while loop_idx < len(all_display_events):
        current_event_wrapper = all_display_events[loop_idx]
        details = current_event_wrapper.get('event_details')
        display_type = current_event_wrapper.get('display_type')

        if display_type == 'trade' and details and current_event_wrapper.get('datetime_obj'):
            key_date_obj = current_event_wrapper.get('datetime_obj')
            key_date_for_agg = key_date_obj.date() if isinstance(key_date_obj, datetime) else key_date_obj 

            key_isin = details.get('isin')
            key_price = details.get('p') # Должно быть Decimal из парсинга
            key_operation = details.get('operation','').lower()
            key_currency = details.get('curr_c')

            trades_to_potentially_aggregate = [current_event_wrapper]
            next_idx = loop_idx + 1 
            while next_idx < len(all_display_events):
                next_event_wrapper = all_display_events[next_idx]
                next_details = next_event_wrapper.get('event_details')
                next_display_type = next_event_wrapper.get('display_type')
                
                next_datetime_obj = next_event_wrapper.get('datetime_obj')
                next_date_for_agg = None
                if next_datetime_obj:
                    next_date_for_agg = next_datetime_obj.date() if isinstance(next_datetime_obj, datetime) else next_datetime_obj

                if (next_display_type == 'trade' and next_details and
                    next_date_for_agg == key_date_for_agg and 
                    next_details.get('isin') == key_isin and
                    next_details.get('p') == key_price and 
                    next_details.get('operation','').lower() == key_operation and
                    next_details.get('curr_c') == key_currency):
                    trades_to_potentially_aggregate.append(next_event_wrapper)
                    next_idx += 1
                else:
                    break

            if len(trades_to_potentially_aggregate) > 1:
                first_trade_wrapper = trades_to_potentially_aggregate[0]
                combined_details = first_trade_wrapper['event_details'].copy()
                
                total_q = Decimal(0)
                total_summ = Decimal(0)
                total_commission = Decimal(0)
                total_fifo_cost_rub = Decimal(0) 
                trade_ids_list = []

                for trade_wrapper_item in trades_to_potentially_aggregate:
                    detail_item = trade_wrapper_item['event_details']
                    # Поля 'q', 'summ', 'commission' должны быть Decimal на этом этапе
                    total_q += detail_item.get('q', Decimal(0)) 
                    total_summ += detail_item.get('summ', Decimal(0))
                    total_commission += detail_item.get('commission', Decimal(0))
                    
                    fifo_cost_val = detail_item.get('fifo_cost_rub_decimal', Decimal(0)) 
                    if not isinstance(fifo_cost_val, Decimal): # Доп. проверка, хотя должно быть Decimal
                        fifo_cost_val = _str_to_decimal_safe(fifo_cost_val, 'fifo_cost_rub_decimal aggregation', detail_item.get('trade_id'))
                    total_fifo_cost_rub += fifo_cost_val
                    trade_ids_list.append(str(detail_item.get('trade_id', '')))
                
                combined_details['q'] = total_q
                combined_details['summ'] = total_summ
                combined_details['commission'] = total_commission
                combined_details['fifo_cost_rub_decimal'] = total_fifo_cost_rub
                combined_details['fifo_cost_rub_str'] = f"{total_fifo_cost_rub:.2f}"
                
                aggregated_id_display_count = 3
                aggregated_id_str = ", ".join(filter(None, trade_ids_list[:aggregated_id_display_count]))
                if len(trade_ids_list) > aggregated_id_display_count:
                    aggregated_id_str += "..."
                combined_details['trade_id'] = f"Aggregated ({len(trade_ids_list)} trades): {aggregated_id_str}"
                combined_details['is_aggregated'] = True
                
                aggregated_wrapper = {
                    'display_type': 'trade',
                    'datetime_obj': first_trade_wrapper['datetime_obj'], 
                    'event_details': combined_details,
                    'isin_group_key': combined_details.get('isin') 
                }
                processed_events_for_aggregation.append(aggregated_wrapper)
                loop_idx = next_idx 
            else:
                if current_event_wrapper.get('event_details'): 
                    current_event_wrapper['event_details']['is_aggregated'] = False
                processed_events_for_aggregation.append(current_event_wrapper)
                loop_idx += 1
        else:
            if details: 
                 details['is_aggregated'] = False
            processed_events_for_aggregation.append(current_event_wrapper)
            loop_idx += 1
            
    all_display_events = processed_events_for_aggregation
    # --- END NEW AGGREGATION LOGIC ---

    all_display_events.sort(key=lambda x: (
        (datetime.combine(x.get('datetime_obj'), datetime.min.time()) if isinstance(x.get('datetime_obj'), date) and not isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) or datetime.min,
        0 if x.get('display_type') == 'initial_holding' else \
        (1 if x.get('display_type') == 'trade' and x.get('event_details') and x['event_details'].get('operation','').lower() == 'buy' else \
        (2 if x.get('display_type') == 'conversion_info' else 3))
    ))
    
    instruments_with_sales_in_target_year = set()
    files_for_target_report_year = UploadedXMLFile.objects.filter(user=user, year=target_report_year)
    if files_for_target_report_year.exists():
        for file_instance in files_for_target_report_year:
            try:
                with file_instance.xml_file.open('rb') as xml_file_content_stream:
                    content_bytes = xml_file_content_stream.read(); xml_string_loop = ""
                    try: xml_string_loop = content_bytes.decode('utf-8')
                    except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
                    if not xml_string_loop: continue
                    root = ET.fromstring(xml_string_loop); trades_element = root.find('.//trades')
                    if trades_element:
                        detailed_element = trades_element.find('detailed')
                        if detailed_element:
                            for node_element in detailed_element.findall('node'):
                                instr_type_el_sale = node_element.find('instr_type')
                                if instr_type_el_sale is None or instr_type_el_sale.text != '1': continue

                                operation_el = node_element.find('operation')
                                isin_el_sale = node_element.find('isin') 
                                isin_to_check_sale = isin_el_sale.text.strip() if isin_el_sale is not None and isin_el_sale.text and isin_el_sale.text.strip() != '-' else None

                                if (operation_el is not None and operation_el.text and operation_el.text.strip().lower() == 'sell' and
                                    isin_to_check_sale):
                                    instruments_with_sales_in_target_year.add(isin_to_check_sale)
            except Exception as e_sales_scan: _processing_had_error[0] = True; logger.error(f"Ошибка сканирования продаж: {e_sales_scan}", exc_info=True)

    final_instrument_event_history = defaultdict(list)
    conversion_map_old_to_new = {}
    conversion_map_new_to_old = {} 
    processed_conversion_ids_for_map = set()
    
    temp_conversion_events_for_map = sorted(
        [evt_wrapper for evt_wrapper in all_display_events if evt_wrapper.get('display_type') == 'conversion_info'],
        key=lambda x: x.get('datetime_obj') or date.min 
    )

    for event_wrapper in temp_conversion_events_for_map:
        event = event_wrapper.get('event_details')
        if event and event.get('corp_action_id') not in processed_conversion_ids_for_map:
            old_i = event.get('old_isin')
            new_i = event.get('new_isin')
            if old_i and new_i : 
                conversion_map_old_to_new[old_i] = new_i 
                conversion_map_new_to_old[new_i] = old_i
                processed_conversion_ids_for_map.add(event['corp_action_id'])

    relevant_isins_for_display = set()
    for sold_isin in instruments_with_sales_in_target_year:
        relevant_isins_for_display.add(sold_isin)
        temp_isin = sold_isin
        while temp_isin in conversion_map_new_to_old:
            prev_isin = conversion_map_new_to_old[temp_isin]
            if prev_isin == temp_isin: break 
            relevant_isins_for_display.add(prev_isin)
            temp_isin = prev_isin
        temp_isin = sold_isin
        while temp_isin in conversion_map_old_to_new:
            next_isin = conversion_map_old_to_new[temp_isin]
            if next_isin == temp_isin: break
            relevant_isins_for_display.add(next_isin)
            temp_isin = next_isin
            
    for event_data_wrapper in all_display_events: 
        details = event_data_wrapper.get('event_details')
        display_type = event_data_wrapper.get('display_type')
        
        current_event_isin = None
        if details: 
            if display_type == 'trade' or display_type == 'initial_holding':
                current_event_isin = details.get('isin')
            elif display_type == 'conversion_info':
                current_event_isin = details.get('new_isin')
        
        if not current_event_isin:
            log_id = "N/A"
            if details: log_id = details.get('trade_id', details.get('corp_action_id', 'Details available but no ID'))
            logger.warning(f"Пропуск события без основного ISIN при финальной группировке: {log_id}")
            continue

        grouping_key_isin = current_event_isin
        while grouping_key_isin in conversion_map_old_to_new and \
              conversion_map_old_to_new[grouping_key_isin] != grouping_key_isin:
             grouping_key_isin = conversion_map_old_to_new[grouping_key_isin]

        should_display_this_event = False
        if current_event_isin in relevant_isins_for_display: should_display_this_event = True
        elif grouping_key_isin in relevant_isins_for_display: should_display_this_event = True
        elif display_type == 'conversion_info' and details and details.get('old_isin') in relevant_isins_for_display:
            should_display_this_event = True
            
        if should_display_this_event: 
            final_instrument_event_history[grouping_key_isin].append(event_data_wrapper)

    final_instrument_event_history = {k: v for k, v in final_instrument_event_history.items() if v}
    if not final_instrument_event_history and (trade_and_holding_ops or conversion_events_for_display_accumulator) and instruments_with_sales_in_target_year:
         messages.info(request, f"Для инструментов с продажами в {target_report_year} году не найдено соответствующей истории сделок или конвертаций для отображения (после финальной фильтрации).")
    elif not final_instrument_event_history and instruments_with_sales_in_target_year:
         messages.warning(request, f"Найдены продажи в {target_report_year} для {list(instruments_with_sales_in_target_year)}, но не удалось собрать историю для них.")

    return final_instrument_event_history, _processing_had_error[0]

@login_required
def upload_xml_file(request):
    user = request.user
    context = {
        'target_report_year_for_title': None, 
        'instrument_event_history': {},
        'parsing_error_occurred': False,
        'previously_uploaded_files': UploadedXMLFile.objects.filter(user=user).order_by('-year', '-uploaded_at')
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
                            parsing_error_in_upload_phase = True; continue
                    if xml_string: file_year_from_xml = parse_year_from_date_end(xml_string)
                    if file_year_from_xml is None:
                        messages.error(request, f"Файл {original_name}: не удалось извлечь год из XML. Файл пропущен.")
                        parsing_error_in_upload_phase = True; continue
                    if UploadedXMLFile.objects.filter(user=user, original_filename=original_name, year=file_year_from_xml).exists():
                        messages.warning(request, f"Файл '{original_name}' для {file_year_from_xml} года уже был загружен. Пропуск.")
                        continue
                    instance = UploadedXMLFile(user=user, xml_file=uploaded_file_from_form, year=file_year_from_xml, original_filename=original_name)
                    instance.save()
                    messages.success(request, f"Файл {original_name} (отчет за {file_year_from_xml} год) успешно загружен.")
                except Exception as e:
                    messages.error(request, f"Ошибка при первичной обработке файла {original_name}: {e}. Файл пропущен.")
                    parsing_error_in_upload_phase = True; logger.error(f"Ошибка при первичной обработке файла {original_name}: {e}", exc_info=True)
            
            if parsing_error_in_upload_phase:
                messages.warning(request, "При загрузке некоторых файлов возникли ошибки.")
            return redirect('upload_xml_file')
        else:
            messages.error(request, "Неизвестное или отсутствующее действие в запросе.")
            return redirect('upload_xml_file')
            
    else: 
        year_to_process = request.session.pop('run_processing_for_year', None)

        if year_to_process is not None:
            context['target_report_year_for_title'] = year_to_process 
            instrument_event_history, parsing_error_current_run = _process_and_get_trade_data(request, user, year_to_process)
            context['instrument_event_history'] = instrument_event_history
            context['parsing_error_occurred'] = parsing_error_current_run
        
    return render(request, 'reports_to_ndfl/upload.html', context)