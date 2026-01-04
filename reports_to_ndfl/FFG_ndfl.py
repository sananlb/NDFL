# reports_to_ndfl/FFG_ndfl.py

from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict, deque
import re
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, Context


# Предполагается, что models и services доступны или будут импортированы
# Если models.py и services.py находятся в той же папке (reports_to_ndfl),
# то импорты будут выглядеть так:
from .models import UploadedXMLFile
from currency_CBRF.models import Currency, ExchangeRate
from currency_CBRF.services import fetch_daily_rates


decimal_context = Context(prec=36, rounding=ROUND_HALF_UP)

PARSING_ERROR_MARKER = "CA_PARSING_ERROR"
NOT_A_RELEVANT_CONVERSION_MARKER = "CA_NOT_RELEVANT_CONVERSION"
_processing_had_error = [False] # Общий флаг ошибки для всего процесса


def _get_report_file_field(file_instance):
    if hasattr(file_instance, 'xml_file') and file_instance.xml_file:
        return file_instance.xml_file
    if hasattr(file_instance, 'report_file') and file_instance.report_file:
        return file_instance.report_file
    return None

def _get_exchange_rate_for_date(request, currency_obj, target_date_obj, rate_purpose_message=""):
    if not isinstance(target_date_obj, date):
        return None, False, None

    exact_rate_obj = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
    if exact_rate_obj: return exact_rate_obj, True, exact_rate_obj.unit_rate

    cbr_date_str_to_fetch = target_date_obj.strftime('%d/%m/%Y')
    parsed_rates_list_from_service, actual_rates_date_from_cbr = fetch_daily_rates(cbr_date_str_to_fetch)
    if actual_rates_date_from_cbr:
        rate_on_target_date_after_fetch = ExchangeRate.objects.filter(currency=currency_obj, date=target_date_obj).first()
        if rate_on_target_date_after_fetch: return rate_on_target_date_after_fetch, True, rate_on_target_date_after_fetch.unit_rate
        if actual_rates_date_from_cbr != target_date_obj:
            rate_data_for_alias_creation = None
            if parsed_rates_list_from_service:
                for rate_info in parsed_rates_list_from_service:
                    if rate_info.get('char_code') == currency_obj.char_code:
                        rate_data_for_alias_creation = rate_info; break
            if rate_data_for_alias_creation:
                try:
                    aliased_rate, _ = ExchangeRate.objects.get_or_create(
                        currency=currency_obj, date=target_date_obj,
                        defaults={'value': rate_data_for_alias_creation['value'], 'nominal': rate_data_for_alias_creation['nominal']}
                    )
                    # Убрано уведомление об алиасе курса
                    return aliased_rate, True, aliased_rate.unit_rate
                except KeyError as e_key: pass
                except Exception as e_alias: pass

    final_fallback_rate = ExchangeRate.objects.filter(currency=currency_obj, date__lte=target_date_obj).order_by('-date').first()
    if final_fallback_rate:
        if final_fallback_rate.date != target_date_obj: messages.info(request, f"Для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} используется ближайший курс от {final_fallback_rate.date.strftime('%d.%m.%Y')}.")
        return final_fallback_rate, final_fallback_rate.date == target_date_obj, final_fallback_rate.unit_rate

    message_to_user = f"Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} не найден."
    if not actual_rates_date_from_cbr : message_to_user = f"Критическая ошибка при загрузке с ЦБ. {message_to_user}"; messages.error(request, message_to_user)
    else: messages.warning(request, message_to_user)
    return None, False, None

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
        file_field = _get_report_file_field(file_instance)
        if not file_field:
            return ca_nodes_in_file
        with file_field.open('rb') as xml_file_content_stream:
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
        pass
    return ca_nodes_in_file

def _parse_and_validate_ca_node_on_demand(request, raw_ca_node_data, ca_nodes_from_same_file, _processing_had_error):
    if not (raw_ca_node_data.get('type_id') == 'conversion' and \
            'Бумаги' in raw_ca_node_data.get('asset_type', '')):
        return NOT_A_RELEVANT_CONVERSION_MARKER

    ca_date_str = raw_ca_node_data.get('date')
    ca_datetime_obj = None
    if ca_date_str:
        try:
            ca_datetime_obj = datetime.strptime(ca_date_str, '%Y-%m-%d').date()
        except ValueError:
            _processing_had_error[0] = True; return PARSING_ERROR_MARKER
    if not ca_datetime_obj: _processing_had_error[0] = True; return PARSING_ERROR_MARKER

    amount_in_ca_node_str = raw_ca_node_data.get('amount', '0')
    try: quantity_in_node = Decimal(amount_in_ca_node_str)
    except InvalidOperation:
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
                pass

    if not found_removal_event:
        error_message = (f"Критическая ошибка (ON-DEMAND PARSE) для КД ID: {corp_action_id_from_node} в файле {raw_ca_node_data.get('file_source')}: "
                         f"Зачислено {quantity_in_node} шт. {isin_in_ca_node} (старый ISIN: {old_isin_from_comment}). "
                         f"Не найдено парное СПИСАНИЕ старых бумаг {old_isin_from_comment} в том же файле. Конвертация не будет применена.")
        messages.error(request, error_message)
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
                                file_ca_nodes_cache, _processing_had_error):
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
                parsed_ca_info = _parse_and_validate_ca_node_on_demand(request, raw_ca_item_data, current_file_raw_cas, _processing_had_error)
                memoized_parsed_ca_results[ca_id] = parsed_ca_info

            if parsed_ca_info in [PARSING_ERROR_MARKER, NOT_A_RELEVANT_CONVERSION_MARKER, None]: continue

            ca_event_fifo_data = parsed_ca_info['fifo_data']
            if ca_event_fifo_data['new_isin'] == target_isin and \
               ca_event_fifo_data['datetime_obj'] <= operation_date: # Конвертация должна произойти до или в день операции, которую она может затронуть
                old_isin = ca_event_fifo_data['old_isin']
                new_isin = ca_event_fifo_data['new_isin'] # Это target_isin
                new_quantity_from_ca = ca_event_fifo_data['new_quantity']
                conversion_date = ca_event_fifo_data['datetime_obj']

                total_cost_basis_of_old_shares_rub = Decimal(0)
                total_qty_of_old_shares_removed = Decimal(0)
                old_shares_queue = buy_lots_deques[old_isin]

                if not old_shares_queue:
                    pass
                
                temp_removed_lots = [] # Временно сохраняем списываемые лоты
                while old_shares_queue:
                    buy_lot = old_shares_queue.popleft() # Сразу извлекаем
                    temp_removed_lots.append(buy_lot)
                    # cost_per_share_rub в лоте уже включает комиссию на покупку этого лота
                    total_cost_basis_of_old_shares_rub += decimal_context.multiply(buy_lot['q_remaining'], buy_lot['cost_per_share_rub'])
                    total_qty_of_old_shares_removed += buy_lot['q_remaining']
                
                # ВАЖНО: Если конвертация происходит ДЛЯ target_isin (т.е. бумаги target_isin ПОЛУЧАЮТСЯ),
                # то мы не должны были ничего списывать из buy_lots_deques[target_isin].
                # Мы списываем из buy_lots_deques[old_isin].

                if total_qty_of_old_shares_removed > 0:
                     pass

                if new_quantity_from_ca > 0:
                    cost_per_new_share_rub = Decimal(0)
                    # Стоимость новых акций наследуется от старых, включая комиссии на покупку старых.
                    # Комиссии самой конвертации здесь не учитываются (они должны быть в other_commissions)
                    if total_qty_of_old_shares_removed > 0 and new_quantity_from_ca > 0: # Убедимся, что не делим на ноль
                        cost_per_new_share_rub = decimal_context.divide(total_cost_basis_of_old_shares_rub, new_quantity_from_ca)
                    
                    new_lot = {
                        'q_remaining': new_quantity_from_ca,
                        'cost_per_share_rub': cost_per_new_share_rub.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP), # Это полная стоимость за 1 шт. новых бумаг
                        'date': conversion_date,
                        'original_trade_id': f"CONV_IN_{ca_id}"
                    }
                    
                    # Вставляем новый лот в очередь для target_isin (new_isin) с сохранением хронологии
                    inserted = False; target_queue_for_new_shares = buy_lots_deques[new_isin] # Это buy_lots_deques[target_isin]
                    for i_idx in range(len(target_queue_for_new_shares)):
                        if conversion_date < target_queue_for_new_shares[i_idx]['date']:
                            target_queue_for_new_shares.insert(i_idx, new_lot); inserted = True; break
                    if not inserted: target_queue_for_new_shares.append(new_lot)
                    
                    # Добавляем информацию о конвертации для отображения в истории инструмента
                    if parsed_ca_info.get('display_data'): # Убедимся, что есть что добавлять
                         conversion_events_for_display_accumulator.append(parsed_ca_info['display_data'])
                    conversion_applied_this_call = True
                elif new_quantity_from_ca == 0 and total_qty_of_old_shares_removed > 0:
                     messages.warning(request, f"При конвертации (ID: {ca_id}) было списано {total_qty_of_old_shares_removed} шт. {old_isin}, но не получено новых акций {new_isin}.")

                applied_corp_action_ids.add(ca_id)
                # Если конвертация была для target_isin и она успешно применилась, можно вернуть True.
                # Это важно, т.к. _apply_conversion_on_demand вызывается в цикле, пока не покроется продажа ИЛИ не закончатся конверсии.
                if conversion_applied_this_call: return True 
    return False


def _process_all_operations_for_fifo(request, operations_to_process,
                                     full_trade_history_map_for_fifo, # Используется для обновления ссылок на словари сделок
                                     relevant_files_for_history,
                                     conversion_events_for_display_accumulator,
                                     _processing_had_error):
    buy_lots_deques = defaultdict(deque)
    pending_short_sales = defaultdict(deque) 
    applied_corp_action_ids = set()
    memoized_parsed_ca_results = {}
    file_ca_nodes_cache = {}

    for op in operations_to_process:
        op_type = op.get('op_type')
        op_isin = op.get('isin')
        op_datetime_obj = op.get('datetime_obj')
        op_date = op_datetime_obj.date() if op_datetime_obj else date.min
        trade_dict_ref = op.get('original_trade_dict_ref') if op_type == 'trade' else None
        
        if trade_dict_ref: # Инициализация для всех сделок (особенно продаж)
            trade_dict_ref['short_sale_status'] = None 
            trade_dict_ref.setdefault('fifo_cost_rub_decimal', Decimal(0))


        if op.get('operation_type') == 'buy' or op_type == 'initial_holding':
            if op['quantity'] <= 0: continue
            
            buy_quantity_original = op['quantity'] # Сохраняем исходное количество покупки
            buy_quantity_remaining_for_lot = op['quantity'] # Это количество пойдет в buy_lots_deques, если не уйдет на покрытие шортов

            # Расчет общей стоимости покупки и комиссии в RUB
            buy_price_per_share_orig_curr = op['price_per_share']
            buy_commission_orig_curr = op['commission']
            
            buy_total_cost_shares_rub = Decimal(0) # Стоимость только акций в RUB
            buy_total_commission_rub = Decimal(0)  # Комиссия покупки в RUB

            if op_type == 'initial_holding':
                # total_cost_rub в op для initial_holding уже должно быть полной стоимостью в RUB
                buy_total_cost_shares_rub = op['total_cost_rub'] 
                buy_total_commission_rub = Decimal(0) # Нет отдельной комиссии для НО в FIFO расчете
            else: # Обычная покупка
                if op['currency'] != 'RUB':
                    if op['cbr_rate_decimal'] is not None:
                        buy_total_cost_shares_rub = (buy_price_per_share_orig_curr * buy_quantity_original * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        buy_total_commission_rub = (buy_commission_orig_curr * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    else:
                        if trade_dict_ref: trade_dict_ref['fifo_cost_rub_str'] = "Ошибка курса покупки (FIFO)"
                        _processing_had_error[0] = True; continue
                else: # RUB trade
                    buy_total_cost_shares_rub = (buy_price_per_share_orig_curr * buy_quantity_original).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    buy_total_commission_rub = buy_commission_orig_curr.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # Полная стоимость одной акции этой покупки В РУБЛЯХ, включая ее комиссию
            cost_per_share_of_this_buy_rub_incl_comm = Decimal(0)
            if buy_quantity_original > 0:
                 cost_per_share_of_this_buy_rub_incl_comm = ((buy_total_cost_shares_rub + buy_total_commission_rub) / buy_quantity_original).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            
            # 1. Попытка покрыть ожидающие короткие продажи этой покупкой
            if op_isin in pending_short_sales and pending_short_sales[op_isin]:
                temp_pending_shorts_for_isin = list(pending_short_sales[op_isin]) # Работаем с копией для безопасного изменения deque
                
                for i in range(len(temp_pending_shorts_for_isin)):
                    if buy_quantity_remaining_for_lot <= Decimal('0.000001'): break # Покупка исчерпана

                    pending_short_entry = pending_short_sales[op_isin][0] # Берем самую старую
                    
                    if pending_short_entry['datetime_obj'].date() > op_date :
                        # Этот шорт был позже текущей покупки, нелогично, но может случиться при ошибках данных. Пропускаем.
                        # Или если мы хотим покрывать только шорты, которые были ДО покупки:
                        break 

                    original_short_trade_ref = pending_short_entry['original_trade_dict_ref']
                    qty_to_cover_short = min(buy_quantity_remaining_for_lot, pending_short_entry['q_uncovered'])

                    # Расходы на закрытие этой части шорта данной покупкой:
                    # Стоимость акций из текущей покупки + пропорциональная комиссия текущей покупки
                    cost_of_shares_for_closing_rub = (qty_to_cover_short * buy_price_per_share_orig_curr) # В исходной валюте
                    commission_for_closing_buy_rub = (buy_commission_orig_curr / buy_quantity_original * qty_to_cover_short) # В исходной валюте

                    if op['currency'] != 'RUB' and op['cbr_rate_decimal'] is not None:
                        cost_of_shares_for_closing_rub = (cost_of_shares_for_closing_rub * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        commission_for_closing_buy_rub = (commission_for_closing_buy_rub * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    elif op['currency'] == 'RUB':
                        cost_of_shares_for_closing_rub = cost_of_shares_for_closing_rub.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        commission_for_closing_buy_rub = commission_for_closing_buy_rub.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    
                    # Обновляем FIFO стоимость для короткой продажи:
                    # Изначально там была только комиссия самой продажи (sell_commission_rub).
                    # Добавляем стоимость акций для закрытия и комиссию покупки для закрытия.
                    original_short_trade_ref['fifo_cost_rub_decimal'] = \
                        (pending_short_entry['sell_commission_rub'] + \
                         cost_of_shares_for_closing_rub + \
                         commission_for_closing_buy_rub).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    
                    buy_quantity_remaining_for_lot -= qty_to_cover_short
                    pending_short_entry['q_uncovered'] -= qty_to_cover_short

                    # Добавляем ID текущей покупки в used_buy_ids продажи, которая была шортом
                    if 'used_buy_ids' not in original_short_trade_ref:
                        original_short_trade_ref['used_buy_ids'] = []
                    original_short_trade_ref['used_buy_ids'].append(op.get('trade_id'))

                    if pending_short_entry['q_uncovered'] <= Decimal('0.000001'):
                        original_short_trade_ref['short_sale_status'] = 'covered_by_future'
                        pending_short_sales[op_isin].popleft()
                    else:
                        original_short_trade_ref['short_sale_status'] = 'partially_covered_short'
            
            if buy_quantity_remaining_for_lot > Decimal('0.000001'):
                original_id = op.get('trade_id', 'INITIAL' if op_type == 'initial_holding' else 'BUY_NO_ID')
                buy_lots_deques[op_isin].append({
                    'q_remaining': buy_quantity_remaining_for_lot,
                    'cost_per_share_rub': cost_per_share_of_this_buy_rub_incl_comm, # Полная стоимость за 1 шт этой покупки
                    'date': op_date,
                    'original_trade_id': original_id
                })


        elif op.get('operation_type') == 'sell':
            if not trade_dict_ref: continue
            if op['quantity'] <= 0:
                trade_dict_ref['fifo_cost_rub_str'] = "0.00 (нулевое кол-во)"; trade_dict_ref['fifo_cost_rub_decimal'] = Decimal(0); continue

            sell_q_to_cover = op['quantity']
            cost_of_shares_from_past_buys_rub = Decimal(0) # Стоимость только самих акций из прошлых покупок/конвертаций
            final_q_covered_by_past_or_conv = Decimal(0)
            
            commission_sell_orig_curr = op['commission']
            commission_sell_rub = Decimal(0)
            if op['currency'] != 'RUB':
                if op['cbr_rate_decimal'] is not None:
                    commission_sell_rub = (commission_sell_orig_curr * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    messages.error(request, f"Нет курса для расчета комиссии продажи {op.get('trade_id','N/A')} ({op_isin}). Комиссия не учтена.")
                    _processing_had_error[0] = True # commission_sell_rub остается 0
            else: 
                commission_sell_rub = commission_sell_orig_curr.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # Начальные расходы для этой продажи = комиссия + стоимость опциона (если есть).
            # Стоимость акций будет добавляться по мере покрытия.
            option_cost_rub = Decimal(0)

            # Если эта продажа через исполнение опциона, добавляем стоимость опциона
            if trade_dict_ref.get('is_option_delivery') and trade_dict_ref.get('related_option_purchase'):
                option_data = trade_dict_ref['related_option_purchase']
                option_price = option_data.get('summ', Decimal(0))  # Стоимость опциона
                option_commission = option_data.get('commission', Decimal(0))  # Комиссия опциона
                option_currency = option_data.get('curr_c', '').strip().upper()
                option_cbr_rate = option_data.get('cbr_rate_decimal', Decimal(1))

                # Переводим стоимость опциона в рубли
                if option_currency != 'RUB' and option_cbr_rate:
                    option_cost_rub = ((option_price + option_commission) * option_cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    option_cost_rub = (option_price + option_commission).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            trade_dict_ref['fifo_cost_rub_decimal'] = (commission_sell_rub + option_cost_rub).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            trade_dict_ref['short_sale_status'] = 'covered_by_past' # Предположение
            # Добавляем список ID покупок, использованных для этой продажи
            trade_dict_ref['used_buy_ids'] = []

            # Этап 1: Попытка покрыть продажу из прошлых покупок (buy_lots_deques)
            current_buy_queue = buy_lots_deques[op_isin]
            while sell_q_to_cover > Decimal('0.000001') and current_buy_queue:
                buy_lot = current_buy_queue[0]
                q_to_take_from_lot = min(sell_q_to_cover, buy_lot['q_remaining'])
                # cost_per_share_rub в buy_lot уже включает комиссию НА ПОКУПКУ этого лота
                cost_for_this_portion = (q_to_take_from_lot * buy_lot['cost_per_share_rub']) 
                
                cost_of_shares_from_past_buys_rub += cost_for_this_portion 
                sell_q_to_cover -= q_to_take_from_lot
                final_q_covered_by_past_or_conv += q_to_take_from_lot
                buy_lot['q_remaining'] -= q_to_take_from_lot
                # Сохраняем ID использованной покупки
                if 'original_trade_id' in buy_lot:
                    trade_dict_ref['used_buy_ids'].append(buy_lot['original_trade_id'])
                if buy_lot['q_remaining'] <= Decimal('0.000001'): current_buy_queue.popleft()
            
            trade_dict_ref['fifo_cost_rub_decimal'] = (cost_of_shares_from_past_buys_rub + commission_sell_rub + option_cost_rub).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            # Этап 2: Если не покрыто, пытаемся применить конвертации
            if sell_q_to_cover > Decimal('0.000001'):
                max_conversion_attempts = 7; attempt_count = 0
                while attempt_count < max_conversion_attempts and sell_q_to_cover > Decimal('0.000001'):
                    attempt_count += 1
                    # _apply_conversion_on_demand теперь может добавлять новые лоты в buy_lots_deques[op_isin]
                    was_conversion_applied = _apply_conversion_on_demand(
                        request, op_isin, op_date, buy_lots_deques, # op_isin это new_isin для конвертации
                        relevant_files_for_history,
                        applied_corp_action_ids, memoized_parsed_ca_results,
                        conversion_events_for_display_accumulator, file_ca_nodes_cache, _processing_had_error
                    )
                    if was_conversion_applied:
                        current_buy_queue_after_conv = buy_lots_deques[op_isin]
                        cost_from_conversion_lots_rub_pass = Decimal(0)
                        while sell_q_to_cover > Decimal('0.000001') and current_buy_queue_after_conv:
                            buy_lot_conv = current_buy_queue_after_conv[0]
                            q_to_take_conv = min(sell_q_to_cover, buy_lot_conv['q_remaining'])
                            cost_for_this_portion_conv = (q_to_take_conv * buy_lot_conv['cost_per_share_rub'])
                            
                            cost_from_conversion_lots_rub_pass += cost_for_this_portion_conv
                            sell_q_to_cover -= q_to_take_conv
                            final_q_covered_by_past_or_conv += q_to_take_conv
                            buy_lot_conv['q_remaining'] -= q_to_take_conv
                            # Сохраняем ID использованной покупки из конвертации
                            if 'original_trade_id' in buy_lot_conv:
                                trade_dict_ref['used_buy_ids'].append(buy_lot_conv['original_trade_id'])
                            if buy_lot_conv['q_remaining'] <= Decimal('0.000001'): current_buy_queue_after_conv.popleft()
                        
                        cost_of_shares_from_past_buys_rub += cost_from_conversion_lots_rub_pass # Добавляем к общей стоимости акций
                        trade_dict_ref['fifo_cost_rub_decimal'] = (cost_of_shares_from_past_buys_rub + commission_sell_rub + option_cost_rub).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    else: 
                        break 
                
                if attempt_count >= max_conversion_attempts and sell_q_to_cover > Decimal('0.000001'):
                    pass

            # Этап 3: Если все еще не покрыто - это короткая продажа (или ее часть)
            if sell_q_to_cover > Decimal('0.000001'):
                pending_short_sales[op_isin].append({
                    'trade_id': op.get('trade_id'),
                    'datetime_obj': op_datetime_obj, # Сохраняем полный datetime для точного порядка
                    'q_uncovered': sell_q_to_cover,
                    'original_trade_dict_ref': trade_dict_ref,
                    'sell_commission_rub': commission_sell_rub 
                })
                trade_dict_ref['short_sale_status'] = 'pending_cover'
                # fifo_cost_rub_decimal уже содержит (стоимость_покрытой_части_акций + комиссия_продажи)

                # Убрано уведомление о непокрытых продажах (шортах)
                # msg_type = messages.info if final_q_covered_by_past_or_conv == 0 else messages.warning
                # message_text = (f"Продажа {op.get('trade_id','N/A')} ({op_isin}) "
                #                 f"{'не покрыта' if final_q_covered_by_past_or_conv == 0 else 'не полностью покрыта'} "
                #                 f"прошлыми покупками/конвертациями. "
                #                 f"Требовалось: {op['quantity']}, покрыто FIFO (до): {final_q_covered_by_past_or_conv}. "
                #                 f"Остаток {sell_q_to_cover} зарегистрирован как потенциальный шорт. "
                #                 f"Текущие расходы (по ранее покрытой части + комиссия продажи): {trade_dict_ref['fifo_cost_rub_decimal']:.2f} RUB.")
                # msg_type(request, message_text)

                # Строка fifo_cost_rub_str будет установлена позже
                if final_q_covered_by_past_or_conv > 0 : # Было частичное покрытие до шорта
                     trade_dict_ref['fifo_cost_rub_str'] = f"Частично: {trade_dict_ref['fifo_cost_rub_decimal']:.2f} (для {final_q_covered_by_past_or_conv} из {op['quantity']}) + шорт {sell_q_to_cover} шт."
                else: # Полностью ушла в шорт с самого начала
                     trade_dict_ref['fifo_cost_rub_str'] = f"Шорт {sell_q_to_cover} шт. (расходы: {commission_sell_rub:.2f} RUB ком.)"

            elif abs(final_q_covered_by_past_or_conv - op['quantity']) > Decimal('0.000001'): # Покрыто, но не точно (из-за округления и т.д.)
                 trade_dict_ref['fifo_cost_rub_str'] = f"Частично: {trade_dict_ref['fifo_cost_rub_decimal']:.2f} (для {final_q_covered_by_past_or_conv} из {op['quantity']})"
                 # trade_dict_ref['short_sale_status'] остается 'covered_by_past'
            else: # Полностью покрыто прошлыми/конвертациями
                 trade_dict_ref['fifo_cost_rub_str'] = f"{trade_dict_ref['fifo_cost_rub_decimal']:.2f}"
                 # trade_dict_ref['short_sale_status'] остается 'covered_by_past'

    # Пост-обработка: определение окончательного статуса для "pending_cover" и "partially_covered_short"
    for isin_key, shorts_deque in pending_short_sales.items():
        remaining_shorts_in_deque = list(shorts_deque) # Копируем, т.к. можем изменять оригинал внутри _apply_conversion...
        for short_entry in remaining_shorts_in_deque: 
            ref = short_entry['original_trade_dict_ref']
            current_status = ref.get('short_sale_status')
            
            if short_entry['q_uncovered'] > Decimal('0.000001'):
                 # Если он все еще в pending_short_sales и q_uncovered > 0, значит он не был полностью покрыт будущей покупкой.
                if current_status == 'pending_cover' or current_status == 'partially_covered_short':
                    ref['short_sale_status'] = 'open_short_sale'
                    q_originally_sold = ref.get('q',Decimal(0)) # Исходное количество продажи
                    q_covered_before_pending = q_originally_sold - short_entry['q_uncovered'] # Сколько было покрыто до того, как остаток ушел в pending

                    if q_covered_before_pending > Decimal('0.000001') : # Если часть была покрыта ДО того как остаток стал "open_short_sale"
                        # fifo_cost_rub_decimal уже содержит стоимость покрытой части + общую комиссию продажи.
                        # fifo_cost_rub_str будет обновлен в process_and_get_trade_data.
                         pass # Строка будет сформирована позже
                    else: # Полностью открытый шорт с самого начала (или после неуспешных конвертаций)
                        # Для полностью открытого шорта, расходы = только комиссия продажи
                        ref['fifo_cost_rub_decimal'] = short_entry['sell_commission_rub']
            # Если q_uncovered == 0, то он был полностью покрыт и уже удален из deque или его статус 'covered_by_future'


def _str_to_decimal_safe(val_str, field_name_for_log="", context_id_for_log="", _processing_had_error=None):
    if val_str is None: return Decimal(0)
    if isinstance(val_str, str) and not val_str.strip(): return Decimal(0) 
    try:
        # Проверяем, не является ли val_str уже Decimal
        if isinstance(val_str, Decimal):
            return val_str
        return Decimal(str(val_str)) 
    except InvalidOperation:
        if _processing_had_error is not None: 
            _processing_had_error[0] = True 
        return Decimal(0) 

def _parse_option_instr_name(option_name):
    if not option_name:
        return None
    match = re.match(
        r'^[\+\-]?(?P<underlying>[A-Z0-9]+)\.(?P<exp>\d{2}[A-Z]{3}\d{4})\.(?P<opt_type>[CP])(?P<strike>\d+(?:\.\d+)?)$',
        option_name.strip().upper()
    )
    if not match:
        return None

    exp = match.group('exp')
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    try:
        day = int(exp[:2])
        mon = month_map.get(exp[2:5])
        year = int(exp[5:9])
        if not mon:
            return None
        expiry_date = date(year, mon, day)
        strike = Decimal(match.group('strike'))
    except (ValueError, InvalidOperation):
        return None

    return {
        'underlying': match.group('underlying'),
        'expiry_date': expiry_date,
        'option_type': match.group('opt_type'),
        'strike': strike
    }

def _calculate_additional_commissions(request, user, target_report_year, target_year_files, _processing_had_error):
    dividend_commissions = defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal), 'amount_rub': Decimal(0), 'details': []})
    other_commissions_details = defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []})
    total_other_commissions_rub = Decimal(0)


    if not target_year_files.exists():
        messages.info(request, f"Нет файлов за {target_report_year} для расчета детализированных комиссий.")
        return dividend_commissions, other_commissions_details, total_other_commissions_rub

    for file_instance in target_year_files:
        try:
            file_field = _get_report_file_field(file_instance)
            if not file_field:
                continue
            with file_field.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read()
                xml_string = ""
                try:
                    xml_string = content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    xml_string = content_bytes.decode('windows-1251', errors='replace')

                if not xml_string:
                    continue

                root = ET.fromstring(xml_string)

                commissions_main_element = root.find('.//commissions')
                if commissions_main_element:
                    detailed_comm = commissions_main_element.find('detailed')
                    if detailed_comm:
                        for comm_node in detailed_comm.findall('node'):
                            sum_str = comm_node.findtext('sum', '0')
                            comm_id_for_log = comm_node.findtext('id', 'N/A_COMM') 
                            sum_val = _str_to_decimal_safe(sum_str, 'commission sum', f"type: {comm_node.findtext('type', 'N/A_COMM_TYPE')}, ID: {comm_id_for_log}, file: {file_instance.original_filename}", _processing_had_error)


                            currency = comm_node.findtext('currency', '').strip().upper()
                            comm_type_str = comm_node.findtext('type', '').strip()
                            comm_datetime_str = comm_node.findtext('datetime', '')
                            comm_comment = comm_node.findtext('comment', '').strip()


                            comm_date_obj = None
                            if comm_datetime_str:
                                try:
                                    comm_date_obj = datetime.strptime(comm_datetime_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    continue 
                            if not (comm_date_obj and comm_date_obj.year == target_report_year):
                                continue 

                            if not currency: 
                                continue

                            if sum_val == Decimal(0): 
                                continue
                            if comm_type_str.startswith("За сделку: "):
                                continue 


                            category_key = None
                            if comm_type_str.startswith("Проценты за использование денежных средств"):
                                category_key = comm_type_str
                            elif comm_type_str == "Прочие комиссии":
                                if "Возмещение комиссии ЦДЦБ за хранение ценных бумаг" in comm_comment:
                                    category_key = "Возмещение комиссии ЦДЦБ за хранение ценных бумаг"
                                elif comm_comment: 
                                     category_key = f"Прочие комиссии: {comm_comment[:50]}{'...' if len(comm_comment) > 50 else ''}"
                                else:
                                    category_key = "Прочие комиссии (без детализации)"
                            elif comm_type_str: 
                                category_key = f"Другие виды комиссий: {comm_type_str}"
                            else: 
                                category_key = "Комиссия без указания типа"


                            amount_rub_comm = sum_val
                            if currency != 'RUB':
                                currency_model_comm = Currency.objects.filter(char_code=currency).first()
                                if currency_model_comm:
                                    _, _, rate_val_comm = _get_exchange_rate_for_date(request, currency_model_comm, comm_date_obj, f"для комиссии '{category_key}'")
                                    if rate_val_comm is not None:
                                        amount_rub_comm = (sum_val * rate_val_comm).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                    else:
                                        messages.warning(request, f"Курс {currency} не найден для комиссии '{category_key}' на {comm_date_obj.strftime('%d.%m.%Y')}.")
                                        _processing_had_error[0] = True
                                else:
                                    messages.warning(request, f"Валюта {currency} для комиссии '{category_key}' не найдена в системе.")
                                    _processing_had_error[0] = True
                            
                            other_commissions_details[category_key]['currencies'][currency] += sum_val
                            other_commissions_details[category_key]['total_rub'] += amount_rub_comm
                            other_commissions_details[category_key]['raw_events'].append({
                                'amount': sum_val,
                                'currency': currency,
                                'date': comm_date_obj, 
                                'amount_rub': amount_rub_comm,
                                'source': f"Comm Type: {comm_type_str}, {file_instance.original_filename}"
                            })
                            total_other_commissions_rub += amount_rub_comm


                corporate_actions_element = root.find('.//corporate_actions')

                if corporate_actions_element:
                    detailed_corp_actions = corporate_actions_element.find('detailed')
                    if detailed_corp_actions:
                        for ca_node in detailed_corp_actions.findall('node'):
                            ca_type = ca_node.findtext('type', '').strip() 
                            ca_type_id = ca_node.findtext('type_id', '').strip().lower()
                            asset_type = ca_node.findtext('asset_type', '').strip()
                            ca_amount_str = ca_node.findtext('amount', '0')
                            ca_currency = ca_node.findtext('currency', '').strip().upper()
                            ca_date_str = ca_node.findtext('date', '') 
                            ca_comment = ca_node.findtext('comment', '').strip()
                            ca_id_for_log = ca_node.findtext('corporate_action_id', 'N/A_CA_COMM')


                            ca_date_obj = None
                            if ca_date_str: 
                                try:        
                                    ca_date_obj = datetime.strptime(ca_date_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    continue
                            
                            if not (ca_date_obj and ca_date_obj.year == target_report_year):
                                continue
                            if asset_type == "Деньги" and ca_type_id not in ['dividend', 'dividend_reverted']:
                                amount_val_ca = _str_to_decimal_safe(ca_amount_str, 'corporate_action amount for commission', ca_id_for_log, _processing_had_error)
                                
                                if amount_val_ca < 0: 
                                    if ca_type_id == 'agent_fee' and "дивиденд" in ca_comment.lower():
                                        continue
                                    
                                    if ca_type_id == 'tax' or ca_type_id == 'tax_reverted':
                                        continue

                                    category_key_ca = None
                                    if "Компенсация при проведении корпоративного действия с бумагами" in ca_comment:
                                        category_key_ca = "Комиссия за корпоративное действие (Компенсация)"
                                    elif ca_type_id == 'conversion' and "компенсация" in ca_comment.lower():
                                        category_key_ca = "Комиссия за корпоративное действие (Конвертация)"
                                    elif ca_type_id == 'intercompany' and "перевод собственных денежных средств" in ca_comment.lower(): 
                                        category_key_ca = "Перевод внутри компании (Комиссия)"
                                    elif ca_type: 
                                        category_key_ca = f"Денежное списание по КД: {ca_type}"
                                    else:
                                        category_key_ca = "Денежное списание по КД (без типа)"


                                    actual_expense_amount_ca = abs(amount_val_ca) 
                                    amount_rub_ca = actual_expense_amount_ca
                                    if ca_currency != 'RUB':
                                        currency_model_ca = Currency.objects.filter(char_code=ca_currency).first()
                                        if currency_model_ca:
                                            _, _, rate_val_ca = _get_exchange_rate_for_date(request, currency_model_ca, ca_date_obj, f"для списания по КД '{category_key_ca}'")
                                            if rate_val_ca is not None:
                                                amount_rub_ca = (actual_expense_amount_ca * rate_val_ca).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                            else:
                                                messages.warning(request, f"Курс {ca_currency} не найден для списания по КД '{category_key_ca}' на {ca_date_obj.strftime('%d.%m.%Y')}.")
                                                _processing_had_error[0] = True
                                        else:
                                            messages.warning(request, f"Валюта {ca_currency} для списания по КД '{category_key_ca}' не найдена в системе.")
                                            _processing_had_error[0] = True

                                    other_commissions_details[category_key_ca]['currencies'][ca_currency] += actual_expense_amount_ca
                                    other_commissions_details[category_key_ca]['total_rub'] += amount_rub_ca
                                    other_commissions_details[category_key_ca]['raw_events'].append({
                                        'amount': actual_expense_amount_ca,
                                        'currency': ca_currency,
                                        'date': ca_date_obj,
                                        'amount_rub': amount_rub_ca,
                                        'source': f"CA ID: {ca_id_for_log}, {file_instance.original_filename}"
                                    })
                                    total_other_commissions_rub += amount_rub_ca
                
                cash_in_outs_element = root.find('.//cash_in_outs') 
                
                if cash_in_outs_element:
                    for node_cio in cash_in_outs_element.findall('node'):
                        cio_type = node_cio.findtext('type', '').strip().lower()
                        cio_comment_original = node_cio.findtext('comment', '').strip() 
                        cio_comment_lower = cio_comment_original.lower() 
                        cio_id_for_log = node_cio.findtext('id', 'N/A_CIO_AGENT_FEE_DIV')

                        if cio_type == 'agent_fee' and "дивиденд" in cio_comment_lower:
                            cio_amount_str = node_cio.findtext('amount', '0')
                            cio_currency = node_cio.findtext('currency', '').strip().upper()
                            
                            cio_datetime_str = node_cio.findtext('datetime', '')
                            if not cio_datetime_str: 
                                cio_datetime_str = node_cio.findtext('pay_d', '') 

                            cio_date_obj = None
                            if cio_datetime_str:
                                try:
                                    cio_date_obj = datetime.strptime(cio_datetime_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    continue
                            
                            if not cio_date_obj:
                                continue

                            if cio_date_obj.year != target_report_year:
                                continue

                            amount_val_cio = _str_to_decimal_safe(cio_amount_str, 'agent_fee amount from cash_in_outs', cio_id_for_log, _processing_had_error)
                            
                            if amount_val_cio < Decimal(0): 
                                actual_commission_amount = abs(amount_val_cio)
                                
                                if not cio_currency:
                                    continue
                                
                                ticker_match = re.search(r'\(([^)]+?\.US|[A-Z]{2,6}\.(?:KZ|HK)|[A-Z0-9]{1,6})\)', cio_comment_original)
                                ticker_key = ticker_match.group(1).strip().upper() if ticker_match else "Неизвестный тикер"
                                
                                category_key_div_comm = f"Агентская комиссия по дивидендам ({ticker_key})"

                                amount_rub_cio = actual_commission_amount
                                if cio_currency != 'RUB':
                                    currency_model_cio = Currency.objects.filter(char_code=cio_currency).first()
                                    if currency_model_cio:
                                        _, _, rate_val_cio = _get_exchange_rate_for_date(request, currency_model_cio, cio_date_obj, f"агентской комиссии по дивидендам {ticker_key}")
                                        if rate_val_cio is not None:
                                            amount_rub_cio = (actual_commission_amount * rate_val_cio).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                        else:
                                            messages.warning(request, f"Курс {cio_currency} не найден для агентской комиссии по дивидендам ({ticker_key}) на {cio_date_obj.strftime('%d.%m.%Y')}.")
                                            _processing_had_error[0] = True
                                    else:
                                        messages.warning(request, f"Валюта {cio_currency} для агентской комиссии по дивидендам ({ticker_key}) не найдена в системе.")
                                        _processing_had_error[0] = True
                                
                                if 'amount_by_currency' not in dividend_commissions[category_key_div_comm]:
                                    dividend_commissions[category_key_div_comm]['amount_by_currency'] = defaultdict(Decimal)
                                
                                dividend_commissions[category_key_div_comm]['amount_by_currency'][cio_currency] += actual_commission_amount
                                dividend_commissions[category_key_div_comm]['amount_rub'] += amount_rub_cio
                                dividend_commissions[category_key_div_comm]['details'].append({
                                    'date': cio_date_obj.strftime('%d.%m.%Y'),
                                    'amount': actual_commission_amount,
                                    'currency': cio_currency,
                                    'amount_rub': amount_rub_cio,
                                    'comment': cio_comment_original,
                                    'source_file': file_instance.original_filename,
                                    'transaction_id': node_cio.findtext('transaction_id', node_cio.findtext('id', 'N/A')) 
                                })
                            elif amount_val_cio > Decimal(0):
                                 pass

        except ET.ParseError as e_parse:
            _processing_had_error[0] = True
            messages.error(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} при расчете детализированных комиссий.")
        except Exception as e:
            _processing_had_error[0] = True
            messages.error(request, f"Неожиданная ошибка при обработке файла {file_instance.original_filename} для детализированных комиссий.")

    return dividend_commissions, other_commissions_details, total_other_commissions_rub


def process_and_get_trade_data(request, user, target_report_year, files_queryset=None):
    _processing_had_error_local_flag = [False] 

    full_instrument_trade_history_for_fifo = defaultdict(list)
    trade_and_holding_ops = [] 
    all_dividend_events_final_list = [] 
    total_dividends_rub_for_year = Decimal(0) 
    total_sales_profit_rub_for_year = Decimal(0) 

    # ИЗМЕНЕНО: Загружаем ВСЕ файлы пользователя для полной истории, включая покрытие шортов будущими покупками
    if files_queryset is None:
        relevant_files_for_history = UploadedXMLFile.objects.filter(user=user).order_by('year', 'uploaded_at')
    else:
        relevant_files_for_history = files_queryset.order_by('year', 'uploaded_at')
    if not relevant_files_for_history.exists():
        messages.info(request, f"У вас нет загруженных файлов для анализа истории.") # Сообщение изменено
        return {}, [], Decimal(0), Decimal(0), _processing_had_error_local_flag[0], defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal),'amount_rub': Decimal(0), 'details': []}), defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []}), Decimal(0)


    trade_detail_tags = ['trade_id', 'date', 'operation', 'instr_nm', 'instr_type', 'instr_kind', 'p', 'curr_c', 'q', 'summ', 'commission', 'issue_nb', 'isin', 'trade_nb']

    # Словарь для хранения опционных сделок: {trade_id: option_purchase_data}
    option_purchases_by_delivery = {}
    used_option_trade_ids = set()

    earliest_report_start_datetime = None
    if relevant_files_for_history: # earliest_report_start_datetime определяется из самого первого файла по дате
        first_file_instance = relevant_files_for_history.first() 
        try:
            file_field = _get_report_file_field(first_file_instance)
            if file_field:
                with file_field.open('rb') as xml_file_content_stream:
                    content_bytes = xml_file_content_stream.read(); xml_string_temp = ""
                    try: xml_string_temp = content_bytes.decode('utf-8')
                    except UnicodeDecodeError: xml_string_temp = content_bytes.decode('windows-1251', errors='replace')
                    if xml_string_temp:
                        root_temp = ET.fromstring(xml_string_temp)
                        date_start_el_temp = root_temp.find('.//date_start')
                        if date_start_el_temp is not None and date_start_el_temp.text:
                            earliest_report_start_datetime = datetime.strptime(date_start_el_temp.text.strip(), '%Y-%m-%d %H:%M:%S')
        except Exception as e_early_date:
            _processing_had_error_local_flag[0] = True 

    processed_initial_holdings_file_ids = set() 
    dividend_events_in_current_file = {} 

    for file_instance in relevant_files_for_history:
        dividend_events_in_current_file.clear() 
        is_target_year_file_for_dividends = (file_instance.year == target_report_year) 

        try:
            file_field = _get_report_file_field(file_instance)
            if not file_field:
                continue
            with file_field.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read(); xml_string_loop = ""
                try: xml_string_loop = content_bytes.decode('utf-8')
                except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
                if not xml_string_loop:
                    continue
                root = ET.fromstring(xml_string_loop)

                current_file_date_start_str = root.findtext('.//date_start', default='').strip()
                current_file_start_dt = None
                if current_file_date_start_str:
                    try: current_file_start_dt = datetime.strptime(current_file_date_start_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError: pass

                if earliest_report_start_datetime and current_file_start_dt == earliest_report_start_datetime and file_instance.id not in processed_initial_holdings_file_ids:
                    account_at_start_el = root.find('.//account_at_start')
                    if account_at_start_el is not None:
                        positions_el_path = './/positions_from_ts/ps/pos' # Стандартный путь
                        positions_el = account_at_start_el.find(positions_el_path)
                        if positions_el is None: # Альтернативный путь, если бумаги напрямую в ps
                            positions_el_path_alt = './/positions_from_ts/ps'
                            positions_el = account_at_start_el.find(positions_el_path_alt)


                        if positions_el is not None: 
                            for pos_node in positions_el.findall('node'): 
                                try:
                                    isin_el = pos_node.find('issue_nb'); isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                    if not isin: 
                                        isin_el_fallback = pos_node.find('isin')
                                        isin = isin_el_fallback.text.strip() if isin_el_fallback is not None and isin_el_fallback.text and isin_el_fallback.text.strip() != '-' else None

                                    if not isin: instr_nm_log = pos_node.findtext('name', 'N/A').strip(); continue
                                    quantity = _str_to_decimal_safe(pos_node.findtext('q', '0'), 'q НО', isin, _processing_had_error_local_flag)
                                    if quantity <= 0: continue 
                                    bal_price_per_share_curr = _str_to_decimal_safe(pos_node.findtext('bal_price_a', '0'), 'bal_price_a НО', isin, _processing_had_error_local_flag)
                                    currency_code = pos_node.findtext('curr', 'RUB').strip().upper()
                                    
                                    total_cost_rub_init = (quantity * bal_price_per_share_curr) # В валюте позиции
                                    rate_decimal_init = Decimal("1.0")
                                    
                                    if currency_code != 'RUB':
                                        currency_model_init = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model_init and earliest_report_start_datetime: 
                                            _ , _, rate_val_init = _get_exchange_rate_for_date(request, currency_model_init, earliest_report_start_datetime.date(), f"для НО {isin}")
                                            if rate_val_init is not None:
                                                rate_decimal_init = rate_val_init
                                                total_cost_rub_init = (total_cost_rub_init * rate_decimal_init).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Теперь в RUB
                                            else: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Не найден курс для НО {isin} ({currency_code}) на {earliest_report_start_datetime.date().strftime('%d.%m.%Y') if earliest_report_start_datetime else 'N/A'}. Стоимость НО может быть неверной."); total_cost_rub_init = Decimal(0) # Обнуляем, если нет курса
                                        else: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Валюта {currency_code} для НО {isin} не найдена. Стоимость НО может быть неверной."); total_cost_rub_init = Decimal(0)
                                    else: # RUB
                                        total_cost_rub_init = total_cost_rub_init.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                                    op_details_dict_for_ref = { 
                                        'date': earliest_report_start_datetime.strftime('%Y-%m-%d %H:%M:%S') if earliest_report_start_datetime else "N/A",
                                        'trade_id': f'INITIAL_{isin}_{earliest_report_start_datetime.strftime("%Y%m%d") if earliest_report_start_datetime else "NODATE"}', # Более уникальный ID
                                        'operation': 'initial_holding',
                                        'instr_nm': pos_node.findtext('name', isin).strip(), 
                                        'isin': isin, 'p': bal_price_per_share_curr, 'curr_c': currency_code, 'q': quantity,
                                        'summ': quantity * bal_price_per_share_curr, 'commission': Decimal(0), 
                                        'transaction_cbr_rate_str': f"{rate_decimal_init:.4f}" if rate_decimal_init else "-",
                                        'file_source': f"Нач. остаток из {file_instance.original_filename}", 'total_cost_rub_str': f"{total_cost_rub_init:.2f}"
                                    }
                                    trade_and_holding_ops.append({
                                        'op_type': 'initial_holding', 'datetime_obj': earliest_report_start_datetime,
                                        'isin': isin, 'quantity': quantity, 
                                        'price_per_share': bal_price_per_share_curr, # Цена в валюте позиции
                                        'total_cost_rub': total_cost_rub_init, # Полная стоимость УЖЕ В РУБЛЯХ для FIFO
                                        'commission': Decimal(0), 'currency': currency_code, # Валюта позиции
                                        'cbr_rate_decimal': rate_decimal_init, 
                                        'original_trade_dict_ref': op_details_dict_for_ref, 
                                        'operation_type': 'buy', 
                                        'file_source': op_details_dict_for_ref['file_source'] 
                                    })
                                except (AttributeError, ValueError) as e_init: 
                                     _processing_had_error_local_flag[0] = True
                        processed_initial_holdings_file_ids.add(file_instance.id) 

                trades_element = root.find('.//trades')
                if trades_element:
                    detailed_element = trades_element.find('detailed')
                    if detailed_element:
                        for node_element in detailed_element.findall('node'):
                            trade_data_dict = {'file_source': f"{file_instance.original_filename} (за {file_instance.year})"}
                            current_trade_id_for_log = node_element.findtext('trade_id', 'N/A')
                            try:
                                instr_type_el = node_element.find('instr_type'); instr_type_val = instr_type_el.text.strip() if instr_type_el is not None and instr_type_el.text else None

                                # Обрабатываем опционы (instr_type='4') отдельно
                                if instr_type_val == '4':
                                    # Парсим данные опциона
                                    for tag in trade_detail_tags:
                                        data_el = node_element.find(tag)
                                        trade_data_dict[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)

                                    # Обрабатываем только покупки опционов
                                    operation = trade_data_dict.get('operation', '').strip().lower()
                                    if operation == 'buy':
                                        # Сохраняем опцион для последующей привязки к поставке
                                        option_trade_id = trade_data_dict.get('trade_id')
                                        trade_data_dict['p'] = _str_to_decimal_safe(trade_data_dict.get('p'), 'p', current_trade_id_for_log, _processing_had_error_local_flag)
                                        trade_data_dict['q'] = _str_to_decimal_safe(trade_data_dict.get('q'), 'q', current_trade_id_for_log, _processing_had_error_local_flag)
                                        trade_data_dict['summ'] = _str_to_decimal_safe(trade_data_dict.get('summ'), 'summ', current_trade_id_for_log, _processing_had_error_local_flag)
                                        trade_data_dict['commission'] = _str_to_decimal_safe(trade_data_dict.get('commission'), 'commission', current_trade_id_for_log, _processing_had_error_local_flag)

                                        # Парсим дату
                                        op_datetime_obj_opt = None
                                        if trade_data_dict.get('date'):
                                            try:
                                                op_datetime_obj_opt = datetime.strptime(trade_data_dict['date'], '%Y-%m-%d %H:%M:%S')
                                            except ValueError:
                                                pass

                                        # Получаем курс валюты
                                        currency_code_opt = trade_data_dict.get('curr_c', '').strip().upper()
                                        rate_decimal_opt = Decimal("1.0000")
                                        if currency_code_opt and currency_code_opt not in ['RUB', 'РУБ', 'РУБ.'] and op_datetime_obj_opt:
                                            currency_model_opt = Currency.objects.filter(char_code=currency_code_opt).first()
                                            if currency_model_opt:
                                                _, _, rate_val_opt = _get_exchange_rate_for_date(request, currency_model_opt, op_datetime_obj_opt.date(), f"для опциона {current_trade_id_for_log}")
                                                if rate_val_opt is not None:
                                                    rate_decimal_opt = rate_val_opt

                                        trade_data_dict['transaction_cbr_rate_str'] = f"{rate_decimal_opt:.4f}"
                                        trade_data_dict['datetime_obj'] = op_datetime_obj_opt
                                        trade_data_dict['cbr_rate_decimal'] = rate_decimal_opt

                                        # Парсим структуру опциона из названия
                                        option_name = trade_data_dict.get('instr_nm', '')
                                        parsed_option_info = _parse_option_instr_name(option_name)
                                        if parsed_option_info:
                                            trade_data_dict['option_underlying'] = parsed_option_info['underlying']
                                            trade_data_dict['option_expiry'] = parsed_option_info['expiry_date']
                                            trade_data_dict['option_type'] = parsed_option_info['option_type']
                                            trade_data_dict['option_strike'] = parsed_option_info['strike']

                                        # Сохраняем по trade_id опциона для последующего поиска
                                        if option_trade_id:
                                            trade_data_dict['option_internal_id'] = option_trade_id
                                            option_purchases_by_delivery[option_trade_id] = trade_data_dict
                                        else:
                                            synthetic_id = f"NO_ID_{len(option_purchases_by_delivery) + 1}"
                                            trade_data_dict['option_internal_id'] = synthetic_id
                                            option_purchases_by_delivery[synthetic_id] = trade_data_dict
                                    continue  # Пропускаем дальнейшую обработку опционов

                                if instr_type_val != '1': continue 

                                isin_el = node_element.find('isin'); current_isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                if not current_isin:
                                    isin_el_issue_nb = node_element.find('issue_nb')
                                    current_isin = isin_el_issue_nb.text.strip() if isin_el_issue_nb is not None and isin_el_issue_nb.text and isin_el_issue_nb.text.strip() != '-' else None
                                
                                if not current_isin: _processing_had_error_local_flag[0] = True; continue
                                trade_data_dict['isin'] = current_isin 

                                for tag in trade_detail_tags: 
                                    data_el = node_element.find(tag)
                                    trade_data_dict[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)
                                if not trade_data_dict.get('isin') and current_isin : trade_data_dict['isin'] = current_isin

                                trade_data_dict['p'] = _str_to_decimal_safe(trade_data_dict.get('p'), 'p', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['q'] = _str_to_decimal_safe(trade_data_dict.get('q'), 'q', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['summ'] = _str_to_decimal_safe(trade_data_dict.get('summ'), 'summ', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['commission'] = _str_to_decimal_safe(trade_data_dict.get('commission'), 'commission', current_trade_id_for_log, _processing_had_error_local_flag)

                                op_datetime_obj = None
                                if trade_data_dict.get('date'):
                                    try: op_datetime_obj = datetime.strptime(trade_data_dict['date'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Некорректная дата сделки {current_trade_id_for_log} ({current_isin})."); continue
                                if not op_datetime_obj: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Отсутствует дата для сделки {current_trade_id_for_log} ({current_isin})."); continue

                                rate_decimal, rate_str = None, "-"; currency_code = trade_data_dict.get('curr_c', '').strip().upper()
                                if currency_code: 
                                    if currency_code in ['RUB', 'РУБ', 'РУБ.']: rate_decimal, rate_str = Decimal("1.0000"), "1.0000"
                                    else:
                                        currency_model = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model:
                                            _ , fetched_exactly, rate_val_trade = _get_exchange_rate_for_date(request, currency_model, op_datetime_obj.date(), f"для сделки {current_trade_id_for_log}")
                                            if rate_val_trade is not None:
                                                rate_decimal = rate_val_trade; rate_str = f"{rate_decimal:.4f}"
                                                if not fetched_exactly: rate_str += " (ближ.)" 
                                            else: _processing_had_error_local_flag[0] = True; rate_str = "не найден"; messages.error(request, f"Курс {currency_code} не найден для сделки {current_trade_id_for_log} на {op_datetime_obj.date().strftime('%d.%m.%Y')}.")
                                        else: _processing_had_error_local_flag[0] = True; rate_str = "валюта не найдена"; messages.error(request, f"Валюта {currency_code} не найдена для сделки {current_trade_id_for_log}.")
                                trade_data_dict['transaction_cbr_rate_str'] = rate_str 

                                if currency_code != 'RUB' and rate_decimal is None: _processing_had_error_local_flag[0] = True; continue

                                # Проверяем, является ли эта сделка результатом исполнения опциона
                                trade_nb = trade_data_dict.get('trade_nb', '')
                                if trade_nb and 'option_delivery' in trade_nb.lower():
                                    trade_data_dict['is_option_delivery'] = True
                                else:
                                    trade_data_dict['is_option_delivery'] = False

                                full_instrument_trade_history_for_fifo[current_isin].append(trade_data_dict) 

                                op_for_processing = {
                                    'op_type': 'trade', 'datetime_obj': op_datetime_obj, 'isin': current_isin,
                                    'trade_id': trade_data_dict.get('trade_id'), 'operation_type': trade_data_dict.get('operation', '').strip().lower(),
                                    'quantity': trade_data_dict['q'], 'price_per_share': trade_data_dict['p'],
                                    'commission': trade_data_dict['commission'], 'currency': currency_code,
                                    'cbr_rate_decimal': rate_decimal, 
                                    'original_trade_dict_ref': trade_data_dict, 
                                    'file_source': trade_data_dict['file_source']
                                }
                                trade_and_holding_ops.append(op_for_processing)
                            except Exception as e_node: 
                                _processing_had_error_local_flag[0] = True
                                messages.error(request, f"Ошибка данных для сделки ID: {current_trade_id_for_log} в файле {file_instance.original_filename}."); continue
                
                if is_target_year_file_for_dividends:
                    cash_in_outs_element = root.find('.//cash_in_outs')
                    if cash_in_outs_element:
                        for node_cio in cash_in_outs_element.findall('node'):
                            try:
                                cio_type = node_cio.findtext('type', '').strip().lower()
                                cio_comment = node_cio.findtext('comment', '').strip()
                                cio_id_for_log = node_cio.findtext('id', 'N/A_CIO_DIV') 
                                
                                details_json_str_cio = node_cio.findtext('details')
                                ca_id_from_details_cio = None
                                if details_json_str_cio:
                                    try: details_data_cio = json.loads(details_json_str_cio); ca_id_from_details_cio = details_data_cio.get('corporate_action_id')
                                    except json.JSONDecodeError: pass 
                                if not ca_id_from_details_cio: 
                                    ca_id_from_details_cio = node_cio.findtext('corporate_action_id', '').strip()

                                if cio_type == 'dividend':
                                    amount_val = _str_to_decimal_safe(node_cio.findtext('amount', '0'), 'dividend amount', cio_id_for_log, _processing_had_error_local_flag)
                                    if amount_val <= 0: continue 

                                    payment_date_str = node_cio.findtext('pay_d', node_cio.findtext('datetime', ''))
                                    payment_date_obj = None
                                    if payment_date_str:
                                        try: 
                                            dt_part = payment_date_str.split(' ')[0]; payment_date_obj = datetime.strptime(dt_part, '%Y-%m-%d').date()
                                        except ValueError: continue
                                    
                                    if not payment_date_obj or payment_date_obj.year != target_report_year: continue

                                    ticker_cio = node_cio.findtext('ticker', '').strip() 
                                    currency_cio = node_cio.findtext('currency', 'RUB').strip().upper() 
                                    
                                    instr_name_cio = ticker_cio if ticker_cio else "Неизвестный инструмент" 
                                    match_comment_instr = re.search(r'Дивиденды по бумаге \((.*?)\s*\(([^)]+)\)\)', cio_comment)
                                    if match_comment_instr:
                                        instr_name_cio = match_comment_instr.group(1).strip()
                                        if not ticker_cio: ticker_cio = match_comment_instr.group(2).strip() 

                                    div_event_key = f"{ca_id_from_details_cio}_{payment_date_obj.isoformat()}" if ca_id_from_details_cio else f"{instr_name_cio}_{ticker_cio}_{payment_date_obj.isoformat()}_{amount_val}" # Более уникальный ключ
                                    
                                    if div_event_key not in dividend_events_in_current_file:
                                        dividend_events_in_current_file[div_event_key] = {
                                            'date': payment_date_obj, 'instrument_name': instr_name_cio, 'ticker': ticker_cio,
                                            'amount': amount_val, 'tax_amount': Decimal(0), 
                                            'currency': currency_cio, 'cbr_rate_str': "-", 
                                            'amount_rub': Decimal(0), 
                                            'file_source': f"{file_instance.original_filename} (за {file_instance.year})",
                                            'corporate_action_id': ca_id_from_details_cio 
                                        }
                                    else: 
                                        dividend_events_in_current_file[div_event_key]['amount'] += amount_val
                                    # Processed dividend event
                            except Exception as e_div_pre_parse: 
                                pass
                        
                        for node_cio in cash_in_outs_element.findall('node'):
                            try:
                                cio_type = node_cio.findtext('type', '').strip().lower()
                                cio_comment = node_cio.findtext('comment', '').strip()
                                cio_id_for_log_tax = node_cio.findtext('id', 'N/A_CIO_Tax') 
                                
                                details_json_str_cio = node_cio.findtext('details')
                                ca_id_from_details_cio = None
                                if details_json_str_cio:
                                    try: details_data_cio = json.loads(details_json_str_cio); ca_id_from_details_cio = details_data_cio.get('corporate_action_id')
                                    except json.JSONDecodeError: pass
                                if not ca_id_from_details_cio:
                                    ca_id_from_details_cio = node_cio.findtext('corporate_action_id', '').strip()

                                if cio_type == 'tax' and ("налог за корпоративное действие" in cio_comment.lower() or "tax for corporate action" in cio_comment.lower()):
                                    tax_date_str = node_cio.findtext('pay_d', node_cio.findtext('datetime', ''))
                                    tax_date_obj = None
                                    if tax_date_str:
                                        try: dt_part = tax_date_str.split(' ')[0]; tax_date_obj = datetime.strptime(dt_part, '%Y-%m-%d').date()
                                        except ValueError: pass 
                                    
                                    if not tax_date_obj or tax_date_obj.year != target_report_year: continue

                                    tax_amount_val = _str_to_decimal_safe(node_cio.findtext('amount', '0'), 'сумма налога', cio_id_for_log_tax, _processing_had_error_local_flag)
                                    
                                    target_dividend_event = None
                                    if ca_id_from_details_cio:
                                        # Ищем по CA_ID и дате, т.к. ключ мог быть сгенерирован без CA_ID если его не было в 'dividend' событии
                                        for key, div_event_entry in dividend_events_in_current_file.items():
                                            if div_event_entry.get('corporate_action_id') == ca_id_from_details_cio:
                                                # Проверка на разумную близость дат налога и дивиденда
                                                if tax_date_obj and div_event_entry.get('date') and \
                                                   tax_date_obj >= div_event_entry.get('date') and \
                                                   (tax_date_obj - div_event_entry.get('date')).days < 90 : # Увеличил дельту
                                                    target_dividend_event = div_event_entry; break
                                    
                                    if target_dividend_event:
                                        target_dividend_event['tax_amount'] += abs(tax_amount_val) 
                                        # Added tax to dividend event
                                    else: pass
                            except Exception as e_tax_parse: 
                                pass

                    all_dividend_events_final_list.extend(dividend_events_in_current_file.values()) 

        except ET.ParseError: _processing_had_error_local_flag[0] = True; messages.error(request, f"Ошибка парсинга XML в файле {file_instance.original_filename}.")
        except Exception as e: _processing_had_error_local_flag[0] = True; messages.error(request, f"Неожиданная ошибка при обработке файла {file_instance.original_filename}.")

    for div_event in all_dividend_events_final_list:
        currency_code_final = div_event['currency']; payment_date_final = div_event['date']; ticker_final = div_event['ticker']
        rate_val_div = Decimal('1.0') 
        cbr_rate_str_for_event = "1.0000"
        if currency_code_final != 'RUB':
            currency_model_f = Currency.objects.filter(char_code=currency_code_final).first()
            if currency_model_f:
                _, fetched_f, rate_val_fetched = _get_exchange_rate_for_date(request, currency_model_f, payment_date_final, f"дивиденд {ticker_final}")
                if rate_val_fetched is not None:
                    rate_val_div = rate_val_fetched
                    cbr_rate_str_for_event = f"{rate_val_div:.4f}"
                    if not fetched_f: cbr_rate_str_for_event += " (ближ.)"
                else: cbr_rate_str_for_event = "не найден" 
            else: cbr_rate_str_for_event = "валюта?" 
        div_event['cbr_rate_str'] = cbr_rate_str_for_event
        div_event['amount_rub'] = (div_event['amount'] * rate_val_div).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        # Налог также нужно перевести в рубли, если он не в рублях и есть курс
        # Это не сделано в исходном коде, но для корректности НДФЛ это важно.
        # Пока оставляем tax_amount как есть (предполагая, что он уже в нужной валюте или его не нужно переводить для целей этого отчета)
        total_dividends_rub_for_year += div_event['amount_rub']


    conversion_events_for_display_accumulator = []
    trade_and_holding_ops.sort(key=lambda x: x.get('datetime_obj') or datetime.min)
    if trade_and_holding_ops:
        pass

    # --- Связывание опционов с поставками ---
    # Для каждой сделки OPTION_DELIVERY находим соответствующую покупку опциона
    for isin_key, trades_list in full_instrument_trade_history_for_fifo.items():
        for trade_dict in trades_list:
            if trade_dict.get('is_option_delivery'):
                delivery_date = trade_dict.get('date')
                if not delivery_date:
                    continue
                try:
                    delivery_dt = datetime.strptime(delivery_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue

                # Извлекаем тикер из названия акции (например, PBR.US -> PBR)
                instr_nm = trade_dict.get('instr_nm', '')
                ticker_match = None
                if '.' in instr_nm:
                    ticker_match = instr_nm.split('.')[0]

                if not ticker_match:
                    continue

                delivery_operation = trade_dict.get('operation', '').strip().lower()
                expected_option_type = 'P' if delivery_operation == 'sell' else ('C' if delivery_operation == 'buy' else None)
                delivery_price = trade_dict.get('p')
                delivery_qty = trade_dict.get('q')

                matching_candidates = []
                for opt_id, opt_data in option_purchases_by_delivery.items():
                    opt_trade_id = opt_data.get('option_internal_id') or opt_data.get('trade_id') or opt_id
                    if opt_trade_id in used_option_trade_ids:
                        continue

                    if opt_data.get('option_underlying') != ticker_match:
                        continue
                    if expected_option_type and opt_data.get('option_type') and opt_data.get('option_type') != expected_option_type:
                        continue

                    opt_expiry = opt_data.get('option_expiry')
                    if opt_expiry:
                        if opt_expiry > delivery_dt.date():
                            continue
                        if (delivery_dt.date() - opt_expiry).days > 7:
                            continue

                    opt_strike = opt_data.get('option_strike')
                    if opt_strike is not None and delivery_price is not None:
                        try:
                            if abs(opt_strike - delivery_price) > Decimal('0.01'):
                                continue
                        except InvalidOperation:
                            continue

                    opt_qty = opt_data.get('q')
                    if opt_qty is not None and delivery_qty is not None:
                        try:
                            expected_shares = opt_qty * Decimal('100')
                            if expected_shares != delivery_qty:
                                continue
                        except InvalidOperation:
                            continue

                    opt_dt = opt_data.get('datetime_obj')
                    if opt_dt and opt_dt >= delivery_dt:
                        continue

                    matching_candidates.append(opt_data)

                if len(matching_candidates) == 1:
                    matching_option = matching_candidates[0]
                    trade_dict['related_option_purchase'] = matching_option
                    opt_trade_id = matching_option.get('option_internal_id') or matching_option.get('trade_id')
                    if opt_trade_id:
                        used_option_trade_ids.add(opt_trade_id)
                elif len(matching_candidates) > 1:
                    matching_option = sorted(
                        matching_candidates,
                        key=lambda x: x.get('datetime_obj') or datetime.min,
                        reverse=True
                    )[0]
                    trade_dict['related_option_purchase'] = matching_option
                    opt_trade_id = matching_option.get('option_internal_id') or matching_option.get('trade_id')
                    if opt_trade_id:
                        used_option_trade_ids.add(opt_trade_id)

    _process_all_operations_for_fifo(request, trade_and_holding_ops, full_instrument_trade_history_for_fifo, relevant_files_for_history, conversion_events_for_display_accumulator, _processing_had_error_local_flag)


    all_display_events = []
    for isin_key, trades_list_for_isin in full_instrument_trade_history_for_fifo.items():
        for trade_dict_updated_with_fifo in trades_list_for_isin:
            dt_obj = datetime.min 
            if trade_dict_updated_with_fifo.get('date'): 
                try: dt_obj = datetime.strptime(trade_dict_updated_with_fifo['date'], '%Y-%m-%d %H:%M:%S')
                except ValueError: pass
            
            # Добавляем is_aggregated по умолчанию false, если его нет
            trade_dict_updated_with_fifo.setdefault('is_aggregated', False)
            trade_dict_updated_with_fifo.setdefault('short_sale_status', None) # Убедимся, что поле есть
            
            all_display_events.append({'display_type': 'trade', 'datetime_obj': dt_obj, 'event_details': trade_dict_updated_with_fifo, 'isin_group_key': trade_dict_updated_with_fifo.get('isin')})

    processed_no_refs_ids = set() 
    for op in trade_and_holding_ops: 
        if op.get('op_type') == 'initial_holding' and op.get('original_trade_dict_ref'):
            ref_id_check = id(op['original_trade_dict_ref']) 
            if ref_id_check not in processed_no_refs_ids:
                op['original_trade_dict_ref']['fifo_cost_rub_str'] = None; 
                op['original_trade_dict_ref']['fifo_cost_rub_decimal'] = None;
                op['original_trade_dict_ref'].setdefault('is_aggregated', False)
                all_display_events.append({'display_type': 'initial_holding', 'datetime_obj': op['datetime_obj'], 'event_details': op['original_trade_dict_ref'], 'isin_group_key': op.get('isin')})
                processed_no_refs_ids.add(ref_id_check)

    for conv_event_data in conversion_events_for_display_accumulator: 
        conv_event_data.setdefault('is_aggregated', False) # Конверсии не агрегируются
        all_display_events.append({'display_type': 'conversion_info', 'datetime_obj': conv_event_data['datetime_obj'], 'event_details': conv_event_data, 'isin_group_key': conv_event_data.get('new_isin')})

    # --- Агрегация сделок ---
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
        current_event_wrapper = all_display_events[loop_idx]; details = current_event_wrapper.get('event_details'); display_type = current_event_wrapper.get('display_type')
        if display_type == 'trade' and details and current_event_wrapper.get('datetime_obj') and not details.get('is_aggregated'): # Только неагрегированные сделки
            key_date_obj = current_event_wrapper.get('datetime_obj'); key_date_for_agg = key_date_obj.date() if isinstance(key_date_obj, datetime) else key_date_obj
            key_isin = details.get('isin'); key_price = details.get('p'); key_operation = details.get('operation','').lower(); key_currency = details.get('curr_c')
            
            trades_to_potentially_aggregate = [current_event_wrapper]; next_idx = loop_idx + 1
            while next_idx < len(all_display_events):
                next_event_wrapper = all_display_events[next_idx]; next_details = next_event_wrapper.get('event_details'); next_display_type = next_event_wrapper.get('display_type')
                next_datetime_obj = next_event_wrapper.get('datetime_obj'); next_date_for_agg = None
                if next_datetime_obj: next_date_for_agg = next_datetime_obj.date() if isinstance(next_datetime_obj, datetime) else next_datetime_obj

                if (next_display_type == 'trade' and next_details and not next_details.get('is_aggregated') and 
                    next_date_for_agg == key_date_for_agg and
                    next_details.get('isin') == key_isin and next_details.get('p') == key_price and
                    next_details.get('operation','').lower() == key_operation and next_details.get('curr_c') == key_currency):
                    trades_to_potentially_aggregate.append(next_event_wrapper); next_idx += 1
                else: break 
            
            if len(trades_to_potentially_aggregate) > 1: 
                first_trade_wrapper = trades_to_potentially_aggregate[0]; combined_details = first_trade_wrapper['event_details'].copy()
                total_q, total_summ, total_commission, total_fifo_cost_rub = Decimal(0), Decimal(0), Decimal(0), Decimal(0)
                trade_ids_list = []
                
                is_sell_operation = combined_details.get('operation','').lower() == 'sell'
                
                for trade_wrapper_item in trades_to_potentially_aggregate:
                    detail_item = trade_wrapper_item['event_details']
                    total_q += detail_item.get('q', Decimal(0)); total_summ += detail_item.get('summ', Decimal(0)); total_commission += detail_item.get('commission', Decimal(0))
                    
                    # Суммируем fifo_cost_rub_decimal только если это не "чистый" шорт, где это только комиссия.
                    # Или если это покупка - для покупок fifo_cost_rub_decimal нерелевантен.
                    # Для продаж fifo_cost_rub_decimal = стоимость акций + комиссия продажи.
                    if is_sell_operation: # Для продаж
                        fifo_cost_val = detail_item.get('fifo_cost_rub_decimal', Decimal(0)) 
                        if not isinstance(fifo_cost_val, Decimal): 
                            fifo_cost_val = _str_to_decimal_safe(fifo_cost_val, 'fifo_cost_rub_decimal aggregation', detail_item.get('trade_id'), _processing_had_error_local_flag)
                        total_fifo_cost_rub += fifo_cost_val
                    
                    trade_ids_list.append(str(detail_item.get('trade_id', ''))) 

                combined_details['q'] = total_q; combined_details['summ'] = total_summ; combined_details['commission'] = total_commission;
                
                if is_sell_operation: # Только для продаж обновляем FIFO стоимость
                    combined_details['fifo_cost_rub_decimal'] = total_fifo_cost_rub
                    combined_details['fifo_cost_rub_str'] = f"{total_fifo_cost_rub:.2f}"
                    # Статус шорта не агрегируем, т.к. это может быть сложно. Агрегированная сделка не будет иметь статуса шорта.
                    combined_details['short_sale_status'] = None 
                    # Объединяем used_buy_ids для агрегированных продаж
                    all_used_buy_ids = []
                    for trade_wrapper_item in trades_to_potentially_aggregate:
                        detail_item = trade_wrapper_item['event_details']
                        if 'used_buy_ids' in detail_item:
                            all_used_buy_ids.extend(detail_item['used_buy_ids'])
                    combined_details['used_buy_ids'] = all_used_buy_ids
                else: # Для покупок
                    combined_details['fifo_cost_rub_decimal'] = None
                    combined_details['fifo_cost_rub_str'] = None


                aggregated_id_display_count = 3; aggregated_id_str = ", ".join(filter(None, trade_ids_list[:aggregated_id_display_count]))
                if len(trade_ids_list) > aggregated_id_display_count: aggregated_id_str += f"... (еще {len(trade_ids_list) - aggregated_id_display_count})";
                combined_details['trade_id'] = f"Агрегировано ({len(trade_ids_list)}): {aggregated_id_str}" 
                combined_details['is_aggregated'] = True
                # Сохраняем оригинальные ID для проверки релевантности
                combined_details['original_trade_ids'] = trade_ids_list
                
                aggregated_wrapper = {'display_type': 'trade', 'datetime_obj': first_trade_wrapper['datetime_obj'], 'event_details': combined_details, 'isin_group_key': combined_details.get('isin')}
                processed_events_for_aggregation.append(aggregated_wrapper); loop_idx = next_idx
            else: 
                if current_event_wrapper.get('event_details'): 
                    current_event_wrapper['event_details']['is_aggregated'] = False
                processed_events_for_aggregation.append(current_event_wrapper); loop_idx += 1
        else: 
            if details: details.setdefault('is_aggregated', False) 
            processed_events_for_aggregation.append(current_event_wrapper); loop_idx += 1
    all_display_events = processed_events_for_aggregation

    # Обновление fifo_cost_rub_str для неагрегированных сделок, особенно шортов, ПОСЛЕ агрегации
    for event_wrapper in all_display_events:
        details = event_wrapper.get('event_details')
        if event_wrapper.get('display_type') == 'trade' and details and not details.get('is_aggregated'):
            if details.get('operation','').lower() == 'sell':
                status = details.get('short_sale_status')
                fifo_cost_decimal = details.get('fifo_cost_rub_decimal', Decimal(0))
                q_sold = details.get('q', Decimal(0))
                
                # Находим исходную запись в pending_short_sales, если она там была, чтобы получить q_uncovered
                # Это сложно сделать здесь без доступа к pending_short_sales.
                # Предположим, что short_sale_status уже содержит всю информацию.

                if status == 'open_short_sale':
                    # Если это полностью открытый шорт, fifo_cost_decimal = sell_commission_rub
                    # Если он был частично покрыт до этого, fifo_cost_decimal уже содержит (стоимость_покр_части + sell_comm)
                    # Для отображения нужен текст
                    initial_q_sold = details.get('q', Decimal(0))
                    # Если мы не знаем, сколько было q_uncovered, сложно сформировать точную строку для частично открытого шорта.
                    # Проще всего выводить статус.
                    details['fifo_cost_rub_str'] = f"Открытый шорт (расх.: {fifo_cost_decimal:.2f} RUB)"
                elif status == 'covered_by_future':
                    details['fifo_cost_rub_str'] = f"{fifo_cost_decimal:.2f} (шорт, покр.)"
                elif status == 'partially_covered_short': # Остался в этом статусе, значит часть не покрыта = open_short_sale для остатка
                     details['short_sale_status'] = 'open_short_sale' # Финализируем статус
                     details['fifo_cost_rub_str'] = f"Частично открытый шорт (тек. расх.: {fifo_cost_decimal:.2f} RUB)"
                elif status == 'covered_by_past' and details.get('fifo_cost_rub_str') is None: # Обычная продажа, если строка не была сформирована
                     details['fifo_cost_rub_str'] = f"{fifo_cost_decimal:.2f}"
                elif details.get('fifo_cost_rub_str') is None and fifo_cost_decimal is not None and status is None : # Покупка или неопределенная продажа
                     details['fifo_cost_rub_str'] = f"{fifo_cost_decimal:.2f}" if details.get('operation','').lower() == 'sell' else None


            elif details.get('operation','').lower() == 'buy': # Для покупок
                 details['fifo_cost_rub_str'] = None
                 details['fifo_cost_rub_decimal'] = None


    all_display_events.sort(key=lambda x: (
        (datetime.combine(x.get('datetime_obj'), datetime.min.time()) if isinstance(x.get('datetime_obj'), date) and not isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) or datetime.min, 
        0 if x.get('display_type') == 'initial_holding' else \
        (1 if x.get('display_type') == 'trade' and x.get('event_details') and x['event_details'].get('operation','').lower() == 'buy' else \
        (2 if x.get('display_type') == 'conversion_info' else \
        (3 if x.get('display_type') == 'trade' and x.get('event_details') and x['event_details'].get('operation','').lower() == 'sell' else 4))) 
    ))

    instruments_with_sales_in_target_year = set()
    # Сканируем продажи ТОЛЬКО в файлах целевого года для определения, чью историю показывать.
    if files_queryset is None:
        files_for_sales_scan_target_year_only = UploadedXMLFile.objects.filter(user=user, year=target_report_year)
    else:
        files_for_sales_scan_target_year_only = files_queryset.filter(year=target_report_year)

    if files_for_sales_scan_target_year_only.exists():
        for file_instance_scan in files_for_sales_scan_target_year_only:
            try:
                file_field = _get_report_file_field(file_instance_scan)
                if not file_field:
                    continue
                with file_field.open('rb') as xml_file_content_stream:
                    content_bytes = xml_file_content_stream.read(); xml_string_loop_scan = ""
                    try: xml_string_loop_scan = content_bytes.decode('utf-8')
                    except UnicodeDecodeError: xml_string_loop_scan = content_bytes.decode('windows-1251', errors='replace')
                    if not xml_string_loop_scan: continue 
                    root_scan = ET.fromstring(xml_string_loop_scan); trades_element_scan = root_scan.find('.//trades')
                    if trades_element_scan:
                        detailed_element_scan = trades_element_scan.find('detailed')
                        if detailed_element_scan:
                            for node_element_scan in detailed_element_scan.findall('node'):
                                instr_type_el_sale = node_element_scan.find('instr_type')
                                if instr_type_el_sale is None or instr_type_el_sale.text != '1': continue 
                                operation_el = node_element_scan.find('operation'); 
                                isin_el_sale = node_element_scan.find('isin')
                                isin_to_check_sale = isin_el_sale.text.strip() if isin_el_sale is not None and isin_el_sale.text and isin_el_sale.text.strip() != '-' else None
                                if not isin_to_check_sale : 
                                    isin_el_sale_nb = node_element_scan.find('issue_nb')
                                    isin_to_check_sale = isin_el_sale_nb.text.strip() if isin_el_sale_nb is not None and isin_el_sale_nb.text and isin_el_sale_nb.text.strip() != '-' else None
                                
                                if (operation_el is not None and operation_el.text and operation_el.text.strip().lower() == 'sell' and isin_to_check_sale):
                                    # Проверяем дату продажи, чтобы она была в целевом году
                                    date_str_sale_scan = node_element_scan.findtext('date')
                                    if date_str_sale_scan:
                                        try:
                                            sale_datetime_scan = datetime.strptime(date_str_sale_scan, '%Y-%m-%d %H:%M:%S')
                                            if sale_datetime_scan.year == target_report_year:
                                                instruments_with_sales_in_target_year.add(isin_to_check_sale)
                                        except ValueError:
                                            pass


            except Exception as e_sales_scan: _processing_had_error_local_flag[0] = True

    final_instrument_event_history = defaultdict(list)
    conversion_map_old_to_new = {}; conversion_map_new_to_old = {}; processed_conversion_ids_for_map = set()
    temp_conversion_events_for_map = sorted(
        [evt_wrapper for evt_wrapper in all_display_events if evt_wrapper.get('display_type') == 'conversion_info'],
        key=lambda x: x.get('datetime_obj') or date.min 
    )
    for event_wrapper in temp_conversion_events_for_map: 
        event = event_wrapper.get('event_details')
        if event and event.get('corp_action_id') not in processed_conversion_ids_for_map: 
            old_i = event.get('old_isin'); new_i = event.get('new_isin')
            if old_i and new_i and old_i != new_i : # Добавил проверку old_i != new_i
                 conversion_map_old_to_new[old_i] = new_i; conversion_map_new_to_old[new_i] = old_i; processed_conversion_ids_for_map.add(event['corp_action_id'])

    relevant_isins_for_display = set() 
    for sold_isin in instruments_with_sales_in_target_year: 
        relevant_isins_for_display.add(sold_isin); temp_isin_chain = sold_isin
        # Назад по цепочке
        while temp_isin_chain in conversion_map_new_to_old:
            prev_isin = conversion_map_new_to_old[temp_isin_chain]
            if prev_isin == temp_isin_chain or prev_isin in relevant_isins_for_display : break 
            relevant_isins_for_display.add(prev_isin); temp_isin_chain = prev_isin
        # Вперед по цепочке от исходно проданного ISIN
        temp_isin_chain = sold_isin 
        while temp_isin_chain in conversion_map_old_to_new:
            next_isin = conversion_map_old_to_new[temp_isin_chain]
            if next_isin == temp_isin_chain or next_isin in relevant_isins_for_display : break
            relevant_isins_for_display.add(next_isin); temp_isin_chain = next_isin
            
    for event_data_wrapper in all_display_events:
        details = event_data_wrapper.get('event_details'); display_type = event_data_wrapper.get('display_type')
        current_event_isin = None 
        if details:
            if display_type == 'trade' or display_type == 'initial_holding': current_event_isin = details.get('isin')
            elif display_type == 'conversion_info': current_event_isin = details.get('new_isin') 
        
        if not current_event_isin: 
            log_id = "N/A"; 
            if details: log_id = details.get('trade_id', details.get('corp_action_id', 'Details available but no ID'))
            continue

        grouping_key_isin = current_event_isin
        # Проходим по цепочке до самого "нового" ISIN для группировки
        visited_for_grouping_key = {grouping_key_isin}
        while grouping_key_isin in conversion_map_old_to_new:
            next_in_chain = conversion_map_old_to_new[grouping_key_isin]
            if next_in_chain == grouping_key_isin or next_in_chain in visited_for_grouping_key: break # Предотвращаем цикл
            grouping_key_isin = next_in_chain
            visited_for_grouping_key.add(grouping_key_isin)
        
        should_display_this_event = False
        # Проверяем релевантность всей цепочки, к которой принадлежит current_event_isin
        temp_check_isin = current_event_isin
        chain_to_check = {temp_check_isin}
        # Назад
        temp_prev = temp_check_isin
        visited_prev = {temp_prev}
        while temp_prev in conversion_map_new_to_old:
            p = conversion_map_new_to_old[temp_prev]
            if p == temp_prev or p in visited_prev: break
            chain_to_check.add(p); temp_prev = p; visited_prev.add(p)
        # Вперед
        temp_next = temp_check_isin
        visited_next = {temp_next}
        while temp_next in conversion_map_old_to_new:
            n = conversion_map_old_to_new[temp_next]
            if n == temp_next or n in visited_next: break
            chain_to_check.add(n); temp_next = n; visited_next.add(n)
        
        if not relevant_isins_for_display.isdisjoint(chain_to_check): # Если есть пересечение
            should_display_this_event = True
        
        if should_display_this_event: final_instrument_event_history[grouping_key_isin].append(event_data_wrapper)

    final_instrument_event_history = {k: v for k, v in final_instrument_event_history.items() if v} 
    all_dividend_events_final_list.sort(key=lambda x: (x.get('date') or date.min, x.get('instrument_name', ''))) 

    # Расчет total_sales_profit_rub_for_year
    if final_instrument_event_history: 
        for isin_key, event_list_for_isin in final_instrument_event_history.items():
            for event_wrapper in event_list_for_isin:
                if event_wrapper.get('display_type') == 'trade':
                    details = event_wrapper.get('event_details')
                    if details and details.get('operation','').lower() == 'sell': 
                        event_datetime_obj = event_wrapper.get('datetime_obj')
                        
                        if event_datetime_obj and event_datetime_obj.year == target_report_year:
                            sale_amount_curr = details.get('summ', Decimal(0)) 
                            currency_code = details.get('curr_c', 'RUB')

                            cbr_rate_for_sale = Decimal('1.0') 
                            if currency_code != 'RUB':
                                rate_str_from_event = details.get('transaction_cbr_rate_str', "0") 
                                match_rate = re.search(r"(\d+(\.\d+)?)", rate_str_from_event) 
                                if match_rate:
                                    try: cbr_rate_for_sale = Decimal(match_rate.group(1))
                                    except InvalidOperation: 
                                        if currency_code != 'RUB': cbr_rate_for_sale = Decimal(0) 
                                elif currency_code != 'RUB': 
                                    cbr_rate_for_sale = Decimal(0)

                            sale_amount_curr = _str_to_decimal_safe(sale_amount_curr, 'sale_amount_for_total_profit_calc', details.get('trade_id'), _processing_had_error_local_flag)
                            income_from_sale_gross_rub = decimal_context.multiply(sale_amount_curr, cbr_rate_for_sale)
                            
                            total_expenses_for_sale_rub = details.get('fifo_cost_rub_decimal', Decimal(0))
                            if total_expenses_for_sale_rub is None: total_expenses_for_sale_rub = Decimal(0) 
                            elif not isinstance(total_expenses_for_sale_rub, Decimal): 
                                total_expenses_for_sale_rub = _str_to_decimal_safe(total_expenses_for_sale_rub, 'total_expenses_for_profit_calc', details.get('trade_id'), _processing_had_error_local_flag)

                            # Для open_short_sale, fifo_cost_rub_decimal = комиссия продажи. Это корректно для НДФЛ.
                            profit_for_this_sale_rub = income_from_sale_gross_rub - total_expenses_for_sale_rub
                            total_sales_profit_rub_for_year += profit_for_this_sale_rub


    if not final_instrument_event_history and not all_dividend_events_final_list and instruments_with_sales_in_target_year:
         messages.warning(request, f"Найдены продажи в {target_report_year} для {list(instruments_with_sales_in_target_year)}, но не удалось собрать историю операций или дивидендов для них.")
    elif not final_instrument_event_history and not all_dividend_events_final_list and not instruments_with_sales_in_target_year and UploadedXMLFile.objects.filter(user=user, year=target_report_year).exists(): 
        messages.info(request, f"В отчетах за {target_report_year} год не найдено продаж по ценным бумагам и не найдено дивидендов для отображения.")
    
    # Помечаем операции, связанные с продажами целевого года
    # Сначала собираем все ID покупок, использованных для продаж целевого года
    used_buy_ids_for_target_year = set()
    target_year_sales = []
    
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details and details.get('operation','').lower() == 'sell':
                    event_datetime_obj = event_wrapper.get('datetime_obj')
                    if event_datetime_obj and event_datetime_obj.year == target_report_year:
                        target_year_sales.append(event_wrapper)
                        # Помечаем саму продажу целевого года
                        details['is_relevant_for_target_year'] = True
                        # Собираем ID использованных покупок
                        if 'used_buy_ids' in details:
                            used_buy_ids_for_target_year.update(details['used_buy_ids'])
                            # Отладка
                            if details['used_buy_ids']:
                                pass
    
    # Помечаем операции на основе их участия в продажах целевого года
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            details = event_wrapper.get('event_details')
            if details:
                # Если это покупка, проверяем, использовалась ли она
                if event_wrapper.get('display_type') == 'trade' and details.get('operation','').lower() == 'buy':
                    trade_id = details.get('trade_id')
                    # Проверяем, агрегированная ли это сделка
                    is_aggregated = details.get('is_aggregated', False)
                    
                    if is_aggregated:
                        # Для агрегированных сделок проверяем исходные ID
                        original_ids = details.get('original_trade_ids', [])
                        
                        if original_ids:
                            # Проверяем каждый оригинальный ID
                            is_relevant = False
                            for orig_id in original_ids:
                                if orig_id in used_buy_ids_for_target_year:
                                    is_relevant = True
                                    break
                            
                            details['is_relevant_for_target_year'] = is_relevant
                            if not is_relevant:
                                pass
                        else:
                            details.setdefault('is_relevant_for_target_year', False)
                    else:
                        # Для обычных покупок
                        if trade_id and trade_id in used_buy_ids_for_target_year:
                            details['is_relevant_for_target_year'] = True
                        else:
                            details.setdefault('is_relevant_for_target_year', False)
                # Для начальных остатков проверяем их ID
                elif event_wrapper.get('display_type') == 'initial_holding':
                    trade_id = details.get('trade_id')
                    if trade_id and trade_id in used_buy_ids_for_target_year:
                        details['is_relevant_for_target_year'] = True
                    else:
                        details.setdefault('is_relevant_for_target_year', False)
                # Для конвертаций проверяем, участвовали ли они в цепочке для продаж целевого года
                elif event_wrapper.get('display_type') == 'conversion_info':
                    # Упрощенно помечаем конвертации как релевантные, если они связаны с инструментами продаж
                    # В идеале нужно отслеживать конвертации в процессе FIFO
                    corp_action_id = details.get('corp_action_id')
                    if corp_action_id and corp_action_id.startswith('CONV_IN_'):
                        # Это ID из конвертации, проверяем
                        if corp_action_id in used_buy_ids_for_target_year:
                            details['is_relevant_for_target_year'] = True
                        else:
                            details.setdefault('is_relevant_for_target_year', False)
                    else:
                        # Пока помечаем все конвертации как потенциально релевантные
                        details.setdefault('is_relevant_for_target_year', True)
                # Продажи не целевого года по умолчанию не релевантны
                elif not details.get('is_relevant_for_target_year'):
                    details.setdefault('is_relevant_for_target_year', False)

    # --- Цветовая маркировка связей покупка-продажа ---
    # Генерируем цвета для связанных сделок ПОСЛЕ установки is_relevant_for_target_year
    available_colors = ['#4FC3F7', '#FF9800', '#66BB6A', '#AB47BC', '#EF5350', '#FFEB3B', '#26C6DA', '#FF7043']
    color_index = 0
    pair_to_color = {}  # {(buy_id, sell_id): color} - каждая пара покупка-продажа получает уникальный цвет
    trade_id_to_colors = {}  # {trade_id: [color1, color2, ...]}

    # Создаем словарь trade_id -> is_relevant для быстрого поиска
    # Включаем как обычные trade_id, так и original_trade_ids для агрегированных сделок
    trade_id_to_relevant = {}
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details:
                    trade_id = details.get('trade_id')
                    is_relevant = details.get('is_relevant_for_target_year', False)
                    if trade_id:
                        trade_id_to_relevant[trade_id] = is_relevant
                    # Также добавляем original_trade_ids для агрегированных сделок
                    if details.get('is_aggregated') and 'original_trade_ids' in details:
                        for orig_id in details['original_trade_ids']:
                            # Для оригинальных ID используем релевантность агрегированной сделки
                            trade_id_to_relevant[orig_id] = is_relevant

    # Шаг 1: Создаем уникальный цвет для каждой пары (покупка, продажа)
    # НО только если ОБЕ сделки релевантны для целевого года
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details and details.get('operation', '').lower() == 'sell':
                    sell_id = details.get('trade_id')
                    sell_is_relevant = details.get('is_relevant_for_target_year', False)

                    # Пропускаем продажи, не относящиеся к целевому году
                    if not sell_is_relevant:
                        continue

                    used_buy_ids = details.get('used_buy_ids', [])
                    # Убираем дубликаты buy_id, сохраняя порядок
                    seen_buy_ids = set()
                    unique_buy_ids = []
                    for buy_id in used_buy_ids:
                        if buy_id not in seen_buy_ids:
                            seen_buy_ids.add(buy_id)
                            unique_buy_ids.append(buy_id)

                    for buy_id in unique_buy_ids:
                        # Проверяем, релевантна ли покупка
                        buy_is_relevant = trade_id_to_relevant.get(buy_id, False)

                        # Создаем пару только если ОБЕ сделки релевантны
                        if buy_is_relevant and sell_is_relevant:
                            pair_key = (buy_id, sell_id)
                            if pair_key not in pair_to_color:
                                pair_to_color[pair_key] = available_colors[color_index % len(available_colors)]
                                color_index += 1

    # Шаг 2: Присваиваем цвета покупкам на основе их связей с продажами
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details and details.get('operation', '').lower() == 'buy':
                    # Для агрегированных покупок используем оригинальные ID
                    if details.get('is_aggregated') and 'original_trade_ids' in details:
                        original_ids = details['original_trade_ids']
                        colors = []
                        for orig_id in original_ids:
                            # Ищем все пары (orig_id, *)
                            for (buy_id, sell_id), color in pair_to_color.items():
                                if buy_id == orig_id and color not in colors:
                                    colors.append(color)
                        if colors:
                            trade_id_to_colors[details.get('trade_id')] = colors
                    else:
                        # Обычная покупка
                        buy_id = details.get('trade_id')
                        colors = []
                        # Ищем все пары (buy_id, *)
                        for (pair_buy_id, sell_id), color in pair_to_color.items():
                            if pair_buy_id == buy_id:
                                colors.append(color)
                        if colors:
                            trade_id_to_colors[buy_id] = colors

    # Шаг 3: Присваиваем цвета продажам на основе их связей с покупками
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details and details.get('operation', '').lower() == 'sell':
                    sell_id = details.get('trade_id')
                    colors = []
                    # Ищем все пары (*, sell_id)
                    for (buy_id, pair_sell_id), color in pair_to_color.items():
                        if pair_sell_id == sell_id:
                            colors.append(color)
                    if colors:
                        trade_id_to_colors[sell_id] = colors

    # Присваиваем цвета сделкам
    for isin_key, event_list_for_isin in final_instrument_event_history.items():
        for event_wrapper in event_list_for_isin:
            if event_wrapper.get('display_type') == 'trade':
                details = event_wrapper.get('event_details')
                if details:
                    trade_id = details.get('trade_id')
                    details['link_colors'] = trade_id_to_colors.get(trade_id, [])

    # Присваиваем цвета опционам (те же, что у связанной поставки)
    for isin_key, trades_list in full_instrument_trade_history_for_fifo.items():
        for trade_dict in trades_list:
            if trade_dict.get('is_option_delivery') and trade_dict.get('related_option_purchase'):
                # Получаем цвета поставки
                delivery_colors = trade_dict.get('link_colors', [])
                # Присваиваем те же цвета опциону
                option_data = trade_dict['related_option_purchase']
                option_data['link_colors'] = delivery_colors

    # Используем files_for_sales_scan_target_year_only для расчета комиссий за целевой год
    dividend_commissions_details, other_commissions_details, total_other_commissions_rub_val = _calculate_additional_commissions(request, user, target_report_year, files_for_sales_scan_target_year_only, _processing_had_error_local_flag)


    if _processing_had_error_local_flag[0]: 
        pass
    
    # Добавляем опционы в final_instrument_event_history как отдельные группы
    # Они будут отображаться вместе с акциями, но перед ними (благодаря сортировке)
    for _, opt_data in option_purchases_by_delivery.items():
        opt_name = opt_data.get('instr_nm', 'Неизвестный опцион')
        # Используем название опциона как ключ группировки
        grouping_key = f"OPTION_{opt_name}"

        # Формируем структуру, похожую на trade events
        option_event = {
            'display_type': 'option',
            'datetime_obj': opt_data.get('datetime_obj'),
            'event_details': opt_data
        }

        if grouping_key not in final_instrument_event_history:
            final_instrument_event_history[grouping_key] = []
        final_instrument_event_history[grouping_key].append(option_event)

    return (
        final_instrument_event_history,
        all_dividend_events_final_list,
        total_dividends_rub_for_year,
        total_sales_profit_rub_for_year,
        _processing_had_error_local_flag[0],
        dividend_commissions_details,
        other_commissions_details,
        total_other_commissions_rub_val
    )
