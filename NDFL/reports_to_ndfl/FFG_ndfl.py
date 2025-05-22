# reports_to_ndfl/FFG_ndfl.py

from django.contrib import messages
import xml.etree.ElementTree as ET
from datetime import datetime, date
from collections import defaultdict, deque
import re
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, Context
import logging


# Предполагается, что models и services доступны или будут импортированы
# Если models.py и services.py находятся в той же папке (reports_to_ndfl),
# то импорты будут выглядеть так:
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
_processing_had_error = [False] # Общий флаг ошибки для всего процесса

def _get_exchange_rate_for_date(request, currency_obj, target_date_obj, rate_purpose_message=""):
    if not isinstance(target_date_obj, date):
        logger.error(f"FFG_NDFL: Передана не дата в _get_exchange_rate_for_date: {target_date_obj} ({type(target_date_obj)}) для {currency_obj.char_code} {rate_purpose_message}")
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
                    aliased_rate, created = ExchangeRate.objects.get_or_create(
                        currency=currency_obj, date=target_date_obj,
                        defaults={'value': rate_data_for_alias_creation['value'], 'nominal': rate_data_for_alias_creation['nominal']}
                    )
                    if created: messages.info(request, f"Создан 'алиас' курса для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} исп. данные от {actual_rates_date_from_cbr.strftime('%d.%m.%Y')}.")
                    return aliased_rate, True, aliased_rate.unit_rate
                except KeyError as e_key: logger.error(f"FFG_NDFL: KeyError при создании 'алиаса' ({e_key}) для {currency_obj.char_code} на {target_date_obj}. Данные: {rate_data_for_alias_creation}", exc_info=True)
                except Exception as e_alias: logger.error(f"FFG_NDFL: Ошибка при создании 'алиаса' курса для {currency_obj.char_code} на {target_date_obj}: {e_alias}", exc_info=True)

    final_fallback_rate = ExchangeRate.objects.filter(currency=currency_obj, date__lte=target_date_obj).order_by('-date').first()
    if final_fallback_rate:
        if final_fallback_rate.date != target_date_obj: messages.info(request, f"Для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} используется ближайший курс от {final_fallback_rate.date.strftime('%d.%m.%Y')}.")
        return final_fallback_rate, final_fallback_rate.date == target_date_obj, final_fallback_rate.unit_rate

    message_to_user = f"Курс для {currency_obj.char_code} на {target_date_obj.strftime('%d.%m.%Y')} {rate_purpose_message} не найден."
    if not actual_rates_date_from_cbr : message_to_user = f"Критическая ошибка при загрузке с ЦБ. {message_to_user}"; messages.error(request, message_to_user)
    else: messages.warning(request, message_to_user)
    logger.warning(message_to_user); return None, False, None

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
        logger.error(f"FFG_NDFL: Ошибка при извлечении КД из файла {file_instance.original_filename}: {e}")
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
            logger.warning(f"FFG_NDFL: ON-DEMAND PARSE: Некорректная дата '{ca_date_str}' для КД {raw_ca_node_data.get('corporate_action_id')}")
            _processing_had_error[0] = True; return PARSING_ERROR_MARKER
    if not ca_datetime_obj: _processing_had_error[0] = True; return PARSING_ERROR_MARKER

    amount_in_ca_node_str = raw_ca_node_data.get('amount', '0')
    try: quantity_in_node = Decimal(amount_in_ca_node_str)
    except InvalidOperation:
        logger.warning(f"FFG_NDFL: ON-DEMAND PARSE: Некорректное количество '{amount_in_ca_node_str}' для КД {raw_ca_node_data.get('corporate_action_id')}")
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
                logger.warning(f"FFG_NDFL: ON-DEMAND PARSE: Не удалось преобразовать кол-во '{removal_ca_data.get('amount')}' для списания КД {corp_action_id_from_node}")

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
               ca_event_fifo_data['datetime_obj'] <= operation_date:
                old_isin = ca_event_fifo_data['old_isin']
                new_isin = ca_event_fifo_data['new_isin']
                new_quantity_from_ca = ca_event_fifo_data['new_quantity']
                conversion_date = ca_event_fifo_data['datetime_obj']

                logger.info(f"FFG_NDFL: FIFO (ON-DEMAND): Применяется конвертация (ID: {ca_id}): {old_isin} -> {new_quantity_from_ca} {new_isin} на {conversion_date} из файла {raw_ca_item_data.get('file_source')}.")
                total_cost_basis_of_old_shares_rub = Decimal(0)
                total_qty_of_old_shares_removed = Decimal(0)
                old_shares_queue = buy_lots_deques[old_isin]

                if not old_shares_queue:
                    logger.warning(f"FFG_NDFL: FIFO (ON-DEMAND): Нет акций {old_isin} для списания при конвертации в {new_isin} (ID: {ca_id}).")

                while old_shares_queue:
                    buy_lot = old_shares_queue.popleft()
                    total_cost_basis_of_old_shares_rub += decimal_context.multiply(buy_lot['q_remaining'], buy_lot['cost_per_share_rub'])
                    total_qty_of_old_shares_removed += buy_lot['q_remaining']

                if total_qty_of_old_shares_removed > 0:
                     logger.info(f"FFG_NDFL: FIFO (ON-DEMAND): Для конвертации (ID: {ca_id}) списано ВСЕГО {total_qty_of_old_shares_removed} шт. {old_isin} общей стоимостью {total_cost_basis_of_old_shares_rub.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP):.2f} RUB.")

                if new_quantity_from_ca > 0:
                    cost_per_new_share_rub = Decimal(0)
                    if total_qty_of_old_shares_removed > 0 : # Ensure division by zero is not attempted if no old shares were present.
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
                    logger.info(f"FFG_NDFL: FIFO (ON-DEMAND): В результате конвертации (ID: {ca_id}) зачислено {new_quantity_from_ca} шт. {new_isin} по ~{cost_per_new_share_rub:.6f} RUB/шт.")

                    conversion_events_for_display_accumulator.append(parsed_ca_info['display_data'])
                    conversion_applied_this_call = True
                elif new_quantity_from_ca == 0 and total_qty_of_old_shares_removed > 0: # Handles cases like delisting where new shares quantity might be zero
                     messages.warning(request, f"При конвертации (ID: {ca_id}) было списано {total_qty_of_old_shares_removed} шт. {old_isin}, но не получено новых акций {new_isin}.")


                applied_corp_action_ids.add(ca_id)
                if conversion_applied_this_call: return True # Return immediately if a relevant conversion was applied for the target_isin
    return False # No relevant conversion was applied in this call for the target_isin


def _process_all_operations_for_fifo(request, operations_to_process,
                                     full_trade_history_map_for_fifo, # Changed parameter name
                                     relevant_files_for_history,
                                     conversion_events_for_display_accumulator,
                                     _processing_had_error):
    buy_lots_deques = defaultdict(deque)
    applied_corp_action_ids = set() # Tracks IDs of CA already applied to avoid re-processing.
    memoized_parsed_ca_results = {} # Caches parsing results for CA to avoid re-parsing.
    file_ca_nodes_cache = {} # Caches raw CA nodes per file_id.


    for op in operations_to_process:
        op_type = op.get('op_type')
        op_isin = op.get('isin')
        op_date = op.get('datetime_obj').date() if op.get('datetime_obj') else date.min
        trade_dict_ref = op.get('original_trade_dict_ref') if op_type == 'trade' else None

        if op.get('operation_type') == 'buy' or op_type == 'initial_holding':
            if op['quantity'] <= 0: continue # Skip if quantity is zero or negative for buys/initial.
            if op_type == 'initial_holding':
                cost_in_rub = op['total_cost_rub']
            else: # Standard buy trade
                cost_in_currency = (op['price_per_share'] * op['quantity']) + op['commission']
                cost_in_rub = cost_in_currency
                if op['currency'] != 'RUB':
                    if op['cbr_rate_decimal'] is not None: # Ensure rate is available
                        cost_in_rub = (cost_in_currency * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    else:
                        # Critical error if no rate for non-RUB buy. Log, set error, and skip.
                        if trade_dict_ref: trade_dict_ref['fifo_cost_rub_str'] = "Ошибка курса покупки (FIFO)"
                        logger.error(f"FFG_NDFL: FIFO: Непредвиденная ошибка курса для покупки {op.get('trade_id','N/A')}")
                        _processing_had_error[0] = True; continue
            cost_per_share_rub = (cost_in_rub / op['quantity']).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            buy_lots_deques[op_isin].append({
                'q_remaining': op['quantity'], 'cost_per_share_rub': cost_per_share_rub,
                'date': op_date,
                'original_trade_id': op.get('trade_id', 'INITIAL' if op_type == 'initial_holding' else 'BUY_NO_ID')
            })
            if op_type == 'initial_holding': logger.info(f"FFG_NDFL: FIFO: Добавлен нач. остаток {op_isin}: {op['quantity']} @ {cost_per_share_rub:.6f} RUB")


        elif op.get('operation_type') == 'sell':
            if not trade_dict_ref: logger.warning(f"FFG_NDFL: FIFO: Пропуск продажи без trade_dict_ref: {op}"); continue
            if op['quantity'] <= 0: # Handle zero quantity sales if they occur
                trade_dict_ref['fifo_cost_rub_str'] = "0.00 (нулевое кол-во)"; trade_dict_ref['fifo_cost_rub_decimal'] = Decimal(0); continue

            trade_dict_ref['fifo_cost_rub_str'] = None; trade_dict_ref['fifo_cost_rub_decimal'] = None # Initialize for this sale
            sell_q_to_cover = op['quantity']
            final_cost_of_shares_sold_rub = Decimal(0)
            final_q_covered_by_fifo = Decimal(0)
            max_conversion_attempts = 7 # Max depth for recursive-like conversion application
            attempt_count = 0

            while sell_q_to_cover > Decimal('0.000001') and attempt_count < max_conversion_attempts:
                attempt_count += 1 # Increment attempt counter
                current_buy_queue = buy_lots_deques[op_isin]
                qty_covered_in_this_pass = Decimal(0); cost_in_this_pass = Decimal(0)

                while sell_q_to_cover > Decimal('0.000001') and current_buy_queue:
                    buy_lot = current_buy_queue[0] # Peek at the first lot
                    q_to_take_from_lot = min(sell_q_to_cover, buy_lot['q_remaining'])
                    cost_for_this_portion = (q_to_take_from_lot * buy_lot['cost_per_share_rub'])
                    cost_in_this_pass += cost_for_this_portion
                    sell_q_to_cover -= q_to_take_from_lot
                    qty_covered_in_this_pass += q_to_take_from_lot
                    buy_lot['q_remaining'] -= q_to_take_from_lot
                    if buy_lot['q_remaining'] <= Decimal('0.000001'): current_buy_queue.popleft() # Remove lot if fully consumed

                final_cost_of_shares_sold_rub += cost_in_this_pass
                final_q_covered_by_fifo += qty_covered_in_this_pass

                if sell_q_to_cover <= Decimal('0.000001'): break # Sale fully covered

                # If sale not fully covered, try to apply conversions that might create new lots for op_isin
                was_conversion_applied = _apply_conversion_on_demand(
                    request, op_isin, op_date, buy_lots_deques,
                    relevant_files_for_history,
                    applied_corp_action_ids, memoized_parsed_ca_results,
                    conversion_events_for_display_accumulator, file_ca_nodes_cache, _processing_had_error
                )
                if not was_conversion_applied: break # No relevant conversion found or applied, stop attempts for this sale

            if attempt_count >= max_conversion_attempts and sell_q_to_cover > Decimal('0.000001'):
                logger.warning(f"FFG_NDFL: FIFO: Достигнуто макс. число попыток ({max_conversion_attempts}) применения конвертаций для продажи {op.get('trade_id','N/A')} ({op_isin}). Непокрытое кол-во: {sell_q_to_cover}")

            # Calculate total expenses in RUB including commission for the sell operation
            commission_sell_rub = op['commission'] # Assume commission is in RUB if not specified otherwise
            if op['currency'] != 'RUB':
                if op['cbr_rate_decimal'] is not None:
                    commission_sell_rub = (op['commission'] * op['cbr_rate_decimal']).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                else:
                    messages.error(request, f"Нет курса для расчета комиссии продажи {op.get('trade_id','N/A')} ({op_isin}).")
                    _processing_had_error[0] = True; commission_sell_rub = Decimal(0) # Default to 0 if rate error

            total_fifo_expenses_rub = final_cost_of_shares_sold_rub + commission_sell_rub
            total_fifo_expenses_rub = total_fifo_expenses_rub.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            trade_dict_ref['fifo_cost_rub_decimal'] = total_fifo_expenses_rub

            if sell_q_to_cover <= Decimal('0.000001'): # Sale fully covered by FIFO
                trade_dict_ref['fifo_cost_rub_str'] = f"{total_fifo_expenses_rub:.2f}"
            else: # Sale partially covered or not covered
                uncovered_qty = op['quantity'] - final_q_covered_by_fifo
                msg = (f"Недостаточно покупок/конвертаций для продажи {op.get('trade_id','N/A')} ({op_isin}). "
                       f"Требовалось: {op['quantity']}, покрыто FIFO: {final_q_covered_by_fifo} (не покрыто: {uncovered_qty}). "
                       f"FIFO затраты (по покрытой части + полная комиссия): {total_fifo_expenses_rub:.2f} RUB.")
                messages.warning(request, msg); logger.warning(msg)
                trade_dict_ref['fifo_cost_rub_str'] = f"Частично: {total_fifo_expenses_rub:.2f} (для {final_q_covered_by_fifo} из {op['quantity']} шт.)"


def _str_to_decimal_safe(val_str, field_name_for_log="", context_id_for_log="", _processing_had_error=None):
    if val_str is None: return Decimal(0)
    if isinstance(val_str, str) and not val_str.strip(): return Decimal(0) # Handle empty strings
    try:
        return Decimal(str(val_str)) # Ensure val_str is stringified before Decimal conversion
    except InvalidOperation:
        logger.error(f"FFG_NDFL: Ошибка преобразования '{field_name_for_log}' в Decimal: '{val_str}' для ID/контекста: {context_id_for_log}")
        if _processing_had_error is not None: # Check if the error flag list is provided
            _processing_had_error[0] = True # Set the error flag
        return Decimal(0) # Return default Decimal(0) on error

def _calculate_additional_commissions(request, user, target_report_year, target_year_files, _processing_had_error):
    dividend_commissions = defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal), 'amount_rub': Decimal(0), 'details': []})
    other_commissions_details = defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []})
    total_other_commissions_rub = Decimal(0)

    sell_trade_ids = set() # Still useful for debugging or future logic, even if not directly used for exclusion now.
    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: _calculate_additional_commissions called for year {target_report_year}.")
    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: target_year_files contains {target_year_files.count()} file(s).")


    if not target_year_files.exists():
        messages.info(request, f"Нет файлов за {target_report_year} для расчета детализированных комиссий.")
        logger.info(f"FFG_NDFL: COMMISSION_DEBUG: No files for target year {target_report_year}. Returning empty commissions.")
        return dividend_commissions, other_commissions_details, total_other_commissions_rub

    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Starting Pass to process commissions.")
    for file_instance in target_year_files:
        logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Processing file for commissions: {file_instance.original_filename}")
        try:
            with file_instance.xml_file.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read()
                xml_string = ""
                try:
                    xml_string = content_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    xml_string = content_bytes.decode('windows-1251', errors='replace')

                if not xml_string:
                    logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: XML string is empty for file {file_instance.original_filename}")
                    continue

                root = ET.fromstring(xml_string)

                # Process commissions from <commissions> section
                commissions_main_element = root.find('.//commissions')
                logger.info(f"FFG_NDFL: COMMISSION_DEBUG: File {file_instance.original_filename} - commissions_main_element found: {commissions_main_element is not None}")
                if commissions_main_element:
                    detailed_comm = commissions_main_element.find('detailed')
                    if detailed_comm:
                        for comm_node in detailed_comm.findall('node'):
                            sum_str = comm_node.findtext('sum', '0')
                            comm_id_for_log = comm_node.findtext('id', 'N/A_COMM') # Use a default if 'id' is missing
                            sum_val = _str_to_decimal_safe(sum_str, 'commission sum', f"type: {comm_node.findtext('type', 'N/A_COMM_TYPE')}, ID: {comm_id_for_log}, file: {file_instance.original_filename}", _processing_had_error)


                            currency = comm_node.findtext('currency', '').strip().upper()
                            comm_type_str = comm_node.findtext('type', '').strip()
                            comm_datetime_str = comm_node.findtext('datetime', '')
                            comm_comment = comm_node.findtext('comment', '').strip()
                            logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Comm Node: type='{comm_type_str}', sum='{sum_str}', currency='{currency}', datetime='{comm_datetime_str}', comment='{comm_comment[:60]}...'")


                            comm_date_obj = None
                            if comm_datetime_str:
                                try:
                                    comm_date_obj = datetime.strptime(comm_datetime_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Некорректная дата комиссии '{comm_datetime_str}' для типа '{comm_type_str}' в файле {file_instance.original_filename}")
                                    continue # Skip this commission if date is invalid

                            # Filter by target_report_year
                            if not (comm_date_obj and comm_date_obj.year == target_report_year):
                                continue # Skip if not in the target year

                            if not currency: # Commissions must have a currency
                                logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Пропуск комиссии без валюты: Тип '{comm_type_str}', Сумма '{sum_str}' в файле {file_instance.original_filename}")
                                continue

                            if sum_val == Decimal(0): # Skip zero-value commissions from this section
                                continue

                            # New logic: Exclude "За сделку" commissions from 'other_commissions_details'
                            if comm_type_str.startswith("За сделку: "):
                                logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Excluding commission '{comm_type_str}' as it is a 'За сделку' type.")
                                continue # Skip adding this commission to other_commissions_details


                            # Categorize other commissions
                            category_key = None
                            if comm_type_str.startswith("Проценты за использование денежных средств"):
                                category_key = comm_type_str
                            elif comm_type_str == "Прочие комиссии":
                                if "Возмещение комиссии ЦДЦБ за хранение ценных бумаг" in comm_comment:
                                    category_key = "Возмещение комиссии ЦДЦБ за хранение ценных бумаг"
                                elif comm_comment: # Use comment if available for "Прочие комиссии"
                                     category_key = f"Прочие комиссии: {comm_comment[:50]}{'...' if len(comm_comment) > 50 else ''}"
                                else:
                                    category_key = "Прочие комиссии (без детализации)"
                            elif comm_type_str: # Other specific commission types
                                category_key = f"Другие виды комиссий: {comm_type_str}"
                            else: # Fallback for commissions without a specified type
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
                                'date': comm_date_obj, # Store as date object for consistency
                                'amount_rub': amount_rub_comm,
                                'source': f"Comm Type: {comm_type_str}, {file_instance.original_filename}"
                            })
                            total_other_commissions_rub += amount_rub_comm
                            logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Adding to other_commissions (from <commissions>): Category='{category_key}', Currency='{currency}', Amount='{sum_val}', RUB='{amount_rub_comm}'. Total now for this cat/curr: {other_commissions_details[category_key]['currencies'][currency]}")


                # Process commissions from <corporate_actions> section where asset_type is "Деньги"
                corporate_actions_element = root.find('.//corporate_actions')
                logger.info(f"FFG_NDFL: COMMISSION_DEBUG: File {file_instance.original_filename} - corporate_actions_element found: {corporate_actions_element is not None}")

                if corporate_actions_element:
                    detailed_corp_actions = corporate_actions_element.find('detailed')
                    if detailed_corp_actions:
                        for ca_node in detailed_corp_actions.findall('node'):
                            ca_type = ca_node.findtext('type', '').strip() # Keep original case for display if needed, use .lower() for comparisons
                            ca_type_id = ca_node.findtext('type_id', '').strip().lower()
                            asset_type = ca_node.findtext('asset_type', '').strip()
                            ca_amount_str = ca_node.findtext('amount', '0')
                            ca_currency = ca_node.findtext('currency', '').strip().upper()
                            ca_date_str = ca_node.findtext('date', '') # This is ex_date for some, payment date for others.
                            ca_comment = ca_node.findtext('comment', '').strip()
                            ca_id_for_log = ca_node.findtext('corporate_action_id', 'N/A_CA_COMM')


                            ca_date_obj = None
                            if ca_date_str: # Date from <corporate_actions> is usually the record/ex-date, not payment date.
                                try:        # For commissions, this date should be the actual charge date.
                                    ca_date_obj = datetime.strptime(ca_date_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Некорректная дата корпоративного действия '{ca_date_str}' для CA ID {ca_id_for_log}")
                                    continue
                            
                            if not (ca_date_obj and ca_date_obj.year == target_report_year):
                                continue

                            # Only consider 'Деньги' asset_type and negative amounts (commissions/expenses)
                            # EXCLUDE dividend types from corporate actions as they are handled separately
                            if asset_type == "Деньги" and ca_type_id not in ['dividend', 'dividend_reverted']:
                                amount_val_ca = _str_to_decimal_safe(ca_amount_str, 'corporate_action amount for commission', ca_id_for_log, _processing_had_error)
                                
                                if amount_val_ca < 0: # Only negative amounts represent expenses/commissions here
                                    # Exclude agent_fee related to dividends, as they are now handled via <cash_in_outs>
                                    if ca_type_id == 'agent_fee' and "дивиденд" in ca_comment.lower():
                                        logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Excluding agent_fee for dividend from 'other_commissions' (via <corporate_actions>): {ca_id_for_log} as it should be caught by <cash_in_outs> processing.")
                                        continue
                                    
                                    # Explicitly check for taxes and exclude them from other_commissions if they are actual tax withholdings.
                                    if ca_type_id == 'tax' or ca_type_id == 'tax_reverted':
                                        logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Excluding tax from other_commissions (via <corporate_actions>): {ca_id_for_log}")
                                        continue

                                    category_key_ca = None
                                    # More specific categorization for CA-related monetary adjustments
                                    if "Компенсация при проведении корпоративного действия с бумагами" in ca_comment:
                                        category_key_ca = "Комиссия за корпоративное действие (Компенсация)"
                                    elif ca_type_id == 'conversion' and "компенсация" in ca_comment.lower():
                                        category_key_ca = "Комиссия за корпоративное действие (Конвертация)"
                                    elif ca_type_id == 'intercompany' and "перевод собственных денежных средств" in ca_comment.lower(): # Example
                                        category_key_ca = "Перевод внутри компании (Комиссия)"
                                    elif ca_type: # Use the original type string for categorization if specific
                                        category_key_ca = f"Денежное списание по КД: {ca_type}"
                                    else:
                                        category_key_ca = "Денежное списание по КД (без типа)"


                                    actual_expense_amount_ca = abs(amount_val_ca) # Store as positive for commission value
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
                                    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Adding to other_commissions (from <corporate_actions>): Category='{category_key_ca}', Currency='{ca_currency}', Amount='{actual_expense_amount_ca}', RUB='{amount_rub_ca}'. Total now for this cat/curr: {other_commissions_details[category_key_ca]['currencies'][ca_currency]}")
                
                # --- START OF NEW BLOCK FOR DIVIDEND AGENT FEES FROM CASH_IN_OUTS ---
                logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Starting processing <cash_in_outs> for dividend agent fees in file: {file_instance.original_filename}")
                cash_in_outs_element = root.find('.//cash_in_outs') # Re-find or use existing 'root'
                
                if cash_in_outs_element:
                    for node_cio in cash_in_outs_element.findall('node'):
                        cio_type = node_cio.findtext('type', '').strip().lower()
                        cio_comment_original = node_cio.findtext('comment', '').strip() # Keep original for display
                        cio_comment_lower = cio_comment_original.lower() # For case-insensitive search
                        cio_id_for_log = node_cio.findtext('id', 'N/A_CIO_AGENT_FEE_DIV')

                        if cio_type == 'agent_fee' and "дивиденд" in cio_comment_lower:
                            cio_amount_str = node_cio.findtext('amount', '0')
                            cio_currency = node_cio.findtext('currency', '').strip().upper()
                            
                            cio_datetime_str = node_cio.findtext('datetime', '')
                            if not cio_datetime_str: 
                                cio_datetime_str = node_cio.findtext('pay_d', '') # Fallback to pay_d

                            cio_date_obj = None
                            if cio_datetime_str:
                                try:
                                    cio_date_obj = datetime.strptime(cio_datetime_str.split(' ')[0], '%Y-%m-%d').date()
                                except ValueError:
                                    logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Некорректная дата '{cio_datetime_str}' для CIO ID {cio_id_for_log} (агентская комиссия по дивидендам).")
                                    continue
                            
                            if not cio_date_obj:
                                logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Отсутствует дата для CIO ID {cio_id_for_log} (агентская комиссия по дивидендам).")
                                continue

                            if cio_date_obj.year != target_report_year:
                                continue

                            amount_val_cio = _str_to_decimal_safe(cio_amount_str, 'agent_fee amount from cash_in_outs', cio_id_for_log, _processing_had_error)
                            
                            if amount_val_cio < Decimal(0): # Process only if it's an expense
                                actual_commission_amount = abs(amount_val_cio)
                                
                                if not cio_currency:
                                    logger.warning(f"FFG_NDFL: COMMISSION_DEBUG: Пропуск агентской комиссии по дивидендам (ID: {cio_id_for_log}) без указания валюты.")
                                    continue
                                
                                # Regex to extract ticker: looks for content within parentheses, prioritizing .US, then .KZ/.HK type, then general uppercase.
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
                                    'transaction_id': node_cio.findtext('transaction_id', 'N/A') # From cash_in_outs, it's called 'id' or 'transaction_id'
                                })
                                logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Добавлена агентская комиссия по дивидендам (из <cash_in_outs>): Категория='{category_key_div_comm}', Валюта='{cio_currency}', Сумма='{actual_commission_amount}', Сумма RUB='{amount_rub_cio}'")
                            elif amount_val_cio > Decimal(0):
                                 logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Найдена агентская комиссия по дивидендам (ID: {cio_id_for_log} в <cash_in_outs>) с положительной суммой '{amount_val_cio}'. Пропускается, так как ожидается расход.")
                # --- END OF NEW BLOCK ---

        except ET.ParseError as e_parse:
            _processing_had_error[0] = True
            logger.error(f"FFG_NDFL: COMMISSION_DEBUG: Ошибка парсинга XML в {file_instance.original_filename} при расчете комиссий: {e_parse}", exc_info=True)
            messages.error(request, f"Ошибка парсинга XML в файле {file_instance.original_filename} при расчете детализированных комиссий.")
        except Exception as e:
            _processing_had_error[0] = True
            logger.error(f"FFG_NDFL: COMMISSION_DEBUG: Неожиданная ошибка при обработке файла {file_instance.original_filename} для комиссий: {e}", exc_info=True)
            messages.error(request, f"Неожиданная ошибка при обработке файла {file_instance.original_filename} для детализированных комиссий.")

    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Finished _calculate_additional_commissions.")
    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Returning dividend_commissions: {json.dumps({k: {'amount_by_currency': dict(v['amount_by_currency']), 'amount_rub': str(v['amount_rub'])} for k, v in dividend_commissions.items()}, default=str)}")
    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Returning other_commissions_details: {json.dumps({k: {kc: str(vc) for kc, vc in v['currencies'].items()} for k, v in other_commissions_details.items()}, default=str)}")
    logger.info(f"FFG_NDFL: COMMISSION_DEBUG: Total other commissions RUB: {total_other_commissions_rub:.2f}")
    return dividend_commissions, other_commissions_details, total_other_commissions_rub


def process_and_get_trade_data(request, user, target_report_year):
    _processing_had_error_local_flag = [False] # Local flag for this processing run

    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Starting process_and_get_trade_data for year {target_report_year} for user {user.username}")

    full_instrument_trade_history_for_fifo = defaultdict(list)
    trade_and_holding_ops = [] # Will store dicts for trades and initial holdings for FIFO
    all_dividend_events_final_list = [] # Will store final processed dividend events
    total_dividends_rub_for_year = Decimal(0) # Sum of all dividends in RUB for the target year
    total_sales_profit_rub_for_year = Decimal(0) # Sum of profits from sales in RUB for the target year


    # Ensure UploadedXMLFile is accessible via import
    relevant_files_for_history = UploadedXMLFile.objects.filter(user=user, year__lte=target_report_year).order_by('year', 'uploaded_at')
    if not relevant_files_for_history.exists():
        messages.info(request, f"У вас нет загруженных файлов с годом отчета {target_report_year} или ранее для анализа истории.")
        logger.info(f"FFG_NDFL: PROCESS_DEBUG: No relevant files for history (year <= {target_report_year}). Returning empty data.")
        # Ensure the return matches the 8 values expected by views.py
        return {}, [], Decimal(0), Decimal(0), _processing_had_error_local_flag[0], defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal),'amount_rub': Decimal(0), 'details': []}), defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []}), Decimal(0)


    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Found {relevant_files_for_history.count()} files for history (year <= {target_report_year}).")

    trade_detail_tags = ['trade_id', 'date', 'operation', 'instr_nm', 'instr_type', 'instr_kind', 'p', 'curr_c', 'q', 'summ', 'commission', 'issue_nb', 'isin']

    # Determine the earliest report start date from the historical files for initial holdings
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
                        logger.info(f"FFG_NDFL: PROCESS_DEBUG: Earliest report start datetime set to {earliest_report_start_datetime} from file {first_file_instance.original_filename}")
        except Exception as e_early_date:
            logger.error(f"FFG_NDFL: PROCESS_DEBUG: Не удалось определить самую раннюю дату начала отчета из {first_file_instance.original_filename}: {e_early_date}")
            _processing_had_error_local_flag[0] = True # Set error flag

    processed_initial_holdings_file_ids = set() # To process initial holdings only once from the earliest file(s)
    dividend_events_in_current_file = {} # Temp dict for processing dividends within a single file

    for file_instance in relevant_files_for_history:
        logger.info(f"FFG_NDFL: PROCESS_DEBUG: Processing file for main data: {file_instance.original_filename} (Year: {file_instance.year})")
        dividend_events_in_current_file.clear() # Reset for current file
        is_target_year_file_for_dividends = (file_instance.year == target_report_year) # Dividends only from target year files

        try:
            with file_instance.xml_file.open('rb') as xml_file_content_stream:
                content_bytes = xml_file_content_stream.read(); xml_string_loop = ""
                try: xml_string_loop = content_bytes.decode('utf-8')
                except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
                if not xml_string_loop:
                    logger.warning(f"FFG_NDFL: PROCESS_DEBUG: XML string is empty for file {file_instance.original_filename}. Skipping.")
                    continue
                root = ET.fromstring(xml_string_loop)

                # Process initial holdings only from the file(s) matching the earliest_report_start_datetime
                current_file_date_start_str = root.findtext('.//date_start', default='').strip()
                current_file_start_dt = None
                if current_file_date_start_str:
                    try: current_file_start_dt = datetime.strptime(current_file_date_start_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError: logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Некорректная date_start {current_file_date_start_str} в {file_instance.original_filename}")

                if earliest_report_start_datetime and current_file_start_dt == earliest_report_start_datetime and file_instance.id not in processed_initial_holdings_file_ids:
                    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Processing initial holdings for file {file_instance.original_filename}")
                    account_at_start_el = root.find('.//account_at_start')
                    if account_at_start_el is not None:
                        positions_el = account_at_start_el.find('.//positions_from_ts/ps/pos')
                        if positions_el: # Check if 'pos' element exists
                            for pos_node in positions_el.findall('node'): # Iterate through each 'node' under 'pos'
                                try:
                                    # Attempt to get ISIN from 'issue_nb' first, then 'isin' as fallback for initial holdings if structure varies.
                                    isin_el = pos_node.find('issue_nb'); isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                    if not isin: # Fallback to 'isin' tag if 'issue_nb' is not suitable or missing
                                        isin_el_fallback = pos_node.find('isin')
                                        isin = isin_el_fallback.text.strip() if isin_el_fallback is not None and isin_el_fallback.text and isin_el_fallback.text.strip() != '-' else None

                                    if not isin: instr_nm_log = pos_node.findtext('name', 'N/A').strip(); logger.info(f"FFG_NDFL: PROCESS_DEBUG: Пропуск НО в {file_instance.original_filename} для '{instr_nm_log}': отсутствует ISIN."); continue
                                    quantity = _str_to_decimal_safe(pos_node.findtext('q', '0'), 'q НО', isin, _processing_had_error_local_flag)
                                    if quantity <= 0: continue # Skip if no quantity
                                    bal_price_per_share_curr = _str_to_decimal_safe(pos_node.findtext('bal_price_a', '0'), 'bal_price_a НО', isin, _processing_had_error_local_flag)
                                    currency_code = pos_node.findtext('curr', 'RUB').strip().upper()
                                    rate_decimal_init = Decimal("1.0"); total_cost_rub_init = (quantity * bal_price_per_share_curr)
                                    if currency_code != 'RUB':
                                        currency_model_init = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model_init and earliest_report_start_datetime: # Ensure earliest_report_start_datetime is valid
                                            _ , _, rate_val_init = _get_exchange_rate_for_date(request, currency_model_init, earliest_report_start_datetime.date(), f"для НО {isin}")
                                            if rate_val_init is not None:
                                                rate_decimal_init = rate_val_init
                                                total_cost_rub_init = (quantity * bal_price_per_share_curr * rate_decimal_init).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                                            else: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Не найден курс для НО {isin} ({currency_code}) на {earliest_report_start_datetime.date().strftime('%d.%m.%Y') if earliest_report_start_datetime else 'N/A'}.")
                                        else: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Валюта {currency_code} для НО {isin} не найдена.")
                                    
                                    # Create a dictionary for this initial holding to be added to trade_and_holding_ops
                                    op_details_dict_for_ref = { # This dict will be referenced by the FIFO op
                                        'date': earliest_report_start_datetime.strftime('%Y-%m-%d %H:%M:%S') if earliest_report_start_datetime else "N/A",
                                        'trade_id': f'INITIAL_{isin}', 'operation': 'initial_holding',
                                        'instr_nm': pos_node.findtext('name', isin).strip(), # Use ISIN as fallback name
                                        'isin': isin, 'p': bal_price_per_share_curr, 'curr_c': currency_code, 'q': quantity,
                                        'summ': quantity * bal_price_per_share_curr, 'commission': Decimal(0), # No commission for initial holdings
                                        'transaction_cbr_rate_str': f"{rate_decimal_init:.4f}" if rate_decimal_init else "-",
                                        'file_source': f"Нач. остаток из {file_instance.original_filename}", 'total_cost_rub_str': f"{total_cost_rub_init:.2f}"
                                    }
                                    trade_and_holding_ops.append({
                                        'op_type': 'initial_holding', 'datetime_obj': earliest_report_start_datetime,
                                        'isin': isin, 'quantity': quantity, 'price_per_share': bal_price_per_share_curr,
                                        'total_cost_rub': total_cost_rub_init, # Cost in RUB for FIFO
                                        'commission': Decimal(0), 'currency': currency_code,
                                        'cbr_rate_decimal': rate_decimal_init, # Rate used for conversion
                                        'original_trade_dict_ref': op_details_dict_for_ref, # Reference to display dict
                                        'operation_type': 'buy', # Treated as a buy for FIFO
                                        'file_source': op_details_dict_for_ref['file_source'] # For traceability
                                    })
                                    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Added initial holding: {op_details_dict_for_ref}")
                                except (AttributeError, ValueError) as e_init: # Catch potential errors during parsing of a pos_node
                                     _processing_had_error_local_flag[0] = True; logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка парсинга НО в {file_instance.original_filename}: {e_init}", exc_info=True)
                        processed_initial_holdings_file_ids.add(file_instance.id) # Mark file as processed for initial holdings

                # Process trades
                trades_element = root.find('.//trades')
                if trades_element:
                    detailed_element = trades_element.find('detailed')
                    if detailed_element:
                        for node_element in detailed_element.findall('node'):
                            trade_data_dict = {'file_source': f"{file_instance.original_filename} (за {file_instance.year})"}
                            current_trade_id_for_log = node_element.findtext('trade_id', 'N/A') # For logging
                            try:
                                instr_type_el = node_element.find('instr_type'); instr_type_val = instr_type_el.text.strip() if instr_type_el is not None and instr_type_el.text else None
                                if instr_type_val != '1': continue # Process only type '1' (stocks, bonds, etc.)

                                # ISIN is crucial. Try 'isin' first, then 'issue_nb' as fallback.
                                isin_el = node_element.find('isin'); current_isin = isin_el.text.strip() if isin_el is not None and isin_el.text and isin_el.text.strip() != '-' else None
                                if not current_isin:
                                    isin_el_issue_nb = node_element.find('issue_nb')
                                    current_isin = isin_el_issue_nb.text.strip() if isin_el_issue_nb is not None and isin_el_issue_nb.text and isin_el_issue_nb.text.strip() != '-' else None
                                
                                if not current_isin: logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Пропуск сделки (ID: {current_trade_id_for_log}): нет ISIN/ISSUE_NB."); _processing_had_error_local_flag[0] = True; continue
                                trade_data_dict['isin'] = current_isin # Ensure ISIN is in the dict

                                for tag in trade_detail_tags: # Populate dict with all relevant tags
                                    data_el = node_element.find(tag)
                                    trade_data_dict[tag] = (data_el.text.strip() if data_el is not None and data_el.text is not None else None)
                                if not trade_data_dict.get('isin') and current_isin : trade_data_dict['isin'] = current_isin # Redundant check, but safe

                                # Convert numerical fields to Decimal, handling potential errors
                                trade_data_dict['p'] = _str_to_decimal_safe(trade_data_dict.get('p'), 'p', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['q'] = _str_to_decimal_safe(trade_data_dict.get('q'), 'q', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['summ'] = _str_to_decimal_safe(trade_data_dict.get('summ'), 'summ', current_trade_id_for_log, _processing_had_error_local_flag)
                                trade_data_dict['commission'] = _str_to_decimal_safe(trade_data_dict.get('commission'), 'commission', current_trade_id_for_log, _processing_had_error_local_flag)

                                # Date and time are crucial for ordering and rate fetching
                                op_datetime_obj = None
                                if trade_data_dict.get('date'):
                                    try: op_datetime_obj = datetime.strptime(trade_data_dict['date'], '%Y-%m-%d %H:%M:%S')
                                    except ValueError: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Некорректная дата сделки {current_trade_id_for_log} ({current_isin})."); continue
                                if not op_datetime_obj: _processing_had_error_local_flag[0] = True; messages.warning(request, f"Отсутствует дата для сделки {current_trade_id_for_log} ({current_isin})."); continue

                                # Get CBRF exchange rate for the operation date
                                rate_decimal, rate_str = None, "-"; currency_code = trade_data_dict.get('curr_c', '').strip().upper()
                                if currency_code: # Ensure currency code exists
                                    if currency_code in ['RUB', 'РУБ', 'РУБ.']: rate_decimal, rate_str = Decimal("1.0000"), "1.0000"
                                    else:
                                        currency_model = Currency.objects.filter(char_code=currency_code).first()
                                        if currency_model:
                                            _ , fetched_exactly, rate_val_trade = _get_exchange_rate_for_date(request, currency_model, op_datetime_obj.date(), f"для сделки {current_trade_id_for_log}")
                                            if rate_val_trade is not None:
                                                rate_decimal = rate_val_trade; rate_str = f"{rate_decimal:.4f}"
                                                if not fetched_exactly: rate_str += " (ближ.)" # Indicate if rate is not for exact date
                                            else: _processing_had_error_local_flag[0] = True; rate_str = "не найден"; messages.error(request, f"Курс {currency_code} не найден для сделки {current_trade_id_for_log} на {op_datetime_obj.date().strftime('%d.%m.%Y')}.")
                                        else: _processing_had_error_local_flag[0] = True; rate_str = "валюта не найдена"; messages.error(request, f"Валюта {currency_code} не найдена для сделки {current_trade_id_for_log}.")
                                trade_data_dict['transaction_cbr_rate_str'] = rate_str # For display

                                # If non-RUB trade and rate couldn't be found, it's a critical error for FIFO.
                                if currency_code != 'RUB' and rate_decimal is None: _processing_had_error_local_flag[0] = True; logger.error(f"FFG_NDFL: PROCESS_DEBUG: Пропуск сделки {current_trade_id_for_log} в FIFO (нет курса {currency_code})."); continue

                                full_instrument_trade_history_for_fifo[current_isin].append(trade_data_dict) # Add to detailed history for display

                                # Prepare operation for FIFO processing list
                                op_for_processing = {
                                    'op_type': 'trade', 'datetime_obj': op_datetime_obj, 'isin': current_isin,
                                    'trade_id': trade_data_dict.get('trade_id'), 'operation_type': trade_data_dict.get('operation', '').strip().lower(),
                                    'quantity': trade_data_dict['q'], 'price_per_share': trade_data_dict['p'],
                                    'commission': trade_data_dict['commission'], 'currency': currency_code,
                                    'cbr_rate_decimal': rate_decimal, # Decimal rate for calculations
                                    'original_trade_dict_ref': trade_data_dict, # Reference to display dict
                                    'file_source': trade_data_dict['file_source']
                                }
                                trade_and_holding_ops.append(op_for_processing)
                            except Exception as e_node: # Catch any other error during node processing
                                _processing_had_error_local_flag[0] = True; logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка обработки узла сделки (ID: {current_trade_id_for_log}) в {file_instance.original_filename}: {e_node}", exc_info=True)
                                messages.error(request, f"Ошибка данных для сделки ID: {current_trade_id_for_log} в файле {file_instance.original_filename}."); continue
                
                # Process dividends only if the current file is for the target report year
                if is_target_year_file_for_dividends:
                    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Processing dividends for target year file {file_instance.original_filename}")
                    cash_in_outs_element = root.find('.//cash_in_outs')
                    if cash_in_outs_element:
                        # First pass for dividend payments
                        for node_cio in cash_in_outs_element.findall('node'):
                            try:
                                cio_type = node_cio.findtext('type', '').strip().lower()
                                cio_comment = node_cio.findtext('comment', '').strip()
                                cio_id_for_log = node_cio.findtext('id', 'N/A_CIO_DIV') # For logging
                                
                                # Try to get corporate_action_id from 'details' JSON or 'corporate_action_id' tag
                                details_json_str_cio = node_cio.findtext('details')
                                ca_id_from_details_cio = None
                                if details_json_str_cio:
                                    try: details_data_cio = json.loads(details_json_str_cio); ca_id_from_details_cio = details_data_cio.get('corporate_action_id')
                                    except json.JSONDecodeError: pass # Ignore if 'details' is not valid JSON
                                if not ca_id_from_details_cio: # Fallback to direct tag
                                    ca_id_from_details_cio = node_cio.findtext('corporate_action_id', '').strip()

                                if cio_type == 'dividend':
                                    amount_val = _str_to_decimal_safe(node_cio.findtext('amount', '0'), 'dividend amount', cio_id_for_log, _processing_had_error_local_flag)
                                    if amount_val <= 0: continue # Dividends should be positive amounts

                                    # Determine payment date (prefer 'pay_d', fallback to 'datetime')
                                    payment_date_str = node_cio.findtext('pay_d', node_cio.findtext('datetime', ''))
                                    payment_date_obj = None
                                    if payment_date_str:
                                        try: # Parse only the date part
                                            dt_part = payment_date_str.split(' ')[0]; payment_date_obj = datetime.strptime(dt_part, '%Y-%m-%d').date()
                                        except ValueError: logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Некорректная дата выплаты дивиденда '{payment_date_str}' (ID {cio_id_for_log})"); continue
                                    
                                    # Ensure payment date is valid and within the target report year
                                    if not payment_date_obj or payment_date_obj.year != target_report_year: continue

                                    ticker_cio = node_cio.findtext('ticker', '').strip() # Get ticker if available
                                    currency_cio = node_cio.findtext('currency', 'RUB').strip().upper() # Default to RUB if not specified
                                    
                                    # Attempt to extract instrument name and improve ticker from comment if needed
                                    instr_name_cio = ticker_cio if ticker_cio else "Неизвестный инструмент" # Default name
                                    match_comment_instr = re.search(r'Дивиденды по бумаге \((.*?)\s*\(([^)]+)\)\)', cio_comment)
                                    if match_comment_instr:
                                        instr_name_cio = match_comment_instr.group(1).strip()
                                        if not ticker_cio: ticker_cio = match_comment_instr.group(2).strip() # Update ticker from comment if missing

                                    # Create a unique key for the dividend event to handle multiple entries for the same CA
                                    div_event_key = f"{ca_id_from_details_cio}_{payment_date_obj.isoformat()}" if ca_id_from_details_cio else f"{ticker_cio}_{payment_date_obj.isoformat()}_{amount_val}"
                                    
                                    if div_event_key not in dividend_events_in_current_file:
                                        dividend_events_in_current_file[div_event_key] = {
                                            'date': payment_date_obj, 'instrument_name': instr_name_cio, 'ticker': ticker_cio,
                                            'amount': amount_val, 'tax_amount': Decimal(0), # Initialize tax as 0
                                            'currency': currency_cio, 'cbr_rate_str': "-", # Rate to be filled later
                                            'amount_rub': Decimal(0), # RUB amount to be filled later
                                            'file_source': f"{file_instance.original_filename} (за {file_instance.year})",
                                            'corporate_action_id': ca_id_from_details_cio # Store CA ID for linking tax
                                        }
                                    else: # If event already exists (e.g. split payments), sum amounts
                                        dividend_events_in_current_file[div_event_key]['amount'] += amount_val
                                    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Processed dividend event: {dividend_events_in_current_file[div_event_key]}")
                            except Exception as e_div_pre_parse: # Catch errors during this specific node processing
                                logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка предварительного парсинга дивиденда (ID: {cio_id_for_log}) в {file_instance.original_filename}: {e_div_pre_parse}", exc_info=True)
                        
                        # Second pass for taxes related to dividends
                        for node_cio in cash_in_outs_element.findall('node'):
                            try:
                                cio_type = node_cio.findtext('type', '').strip().lower()
                                cio_comment = node_cio.findtext('comment', '').strip()
                                cio_id_for_log = node_cio.findtext('id', 'N/A_CIO_Tax') # For logging
                                
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
                                        except ValueError: pass # Ignore if date is invalid
                                    
                                    if not tax_date_obj or tax_date_obj.year != target_report_year: continue

                                    tax_amount_val = _str_to_decimal_safe(node_cio.findtext('amount', '0'), 'сумма налога', cio_id_for_log, _processing_had_error_local_flag)
                                    
                                    # Try to link tax to a previously found dividend event using CA ID
                                    target_dividend_event = None
                                    if ca_id_from_details_cio:
                                        for key, div_event_entry in dividend_events_in_current_file.items():
                                            # Match by corporate_action_id and ensure tax date is reasonable (e.g., on or after dividend payment)
                                            if div_event_entry.get('corporate_action_id') == ca_id_from_details_cio:
                                                if tax_date_obj and div_event_entry.get('date') and tax_date_obj >= div_event_entry.get('date') and (tax_date_obj - div_event_entry.get('date')).days < 30 : # Tax usually close to payment
                                                    target_dividend_event = div_event_entry; break
                                    
                                    if target_dividend_event:
                                        target_dividend_event['tax_amount'] += abs(tax_amount_val) # Tax is usually negative, store as positive deduction
                                        logger.info(f"FFG_NDFL: PROCESS_DEBUG: Added tax {abs(tax_amount_val)} to dividend event {target_dividend_event.get('corporate_action_id')}")
                                    else: logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Не удалось связать налог (ID {cio_id_for_log}, CA_ID: {ca_id_from_details_cio}, Дата: {tax_date_obj}) с дивидендом в файле {file_instance.original_filename}.")
                            except Exception as e_tax_parse: # Catch errors during tax node processing
                                logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка парсинга узла налога (ID: {cio_id_for_log}) в {file_instance.original_filename}: {e_tax_parse}", exc_info=True)

                    all_dividend_events_final_list.extend(dividend_events_in_current_file.values()) # Add processed dividends from this file

        except ET.ParseError: _processing_had_error_local_flag[0] = True; logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Ошибка парсинга XML: {file_instance.original_filename}", exc_info=True); messages.error(request, f"Ошибка парсинга XML в файле {file_instance.original_filename}.")
        except Exception as e: _processing_had_error_local_flag[0] = True; logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка обработки файла {file_instance.original_filename}: {e}", exc_info=True); messages.error(request, f"Неожиданная ошибка при обработке файла {file_instance.original_filename}.")

    # Finalize dividend amounts in RUB and total for the year
    for div_event in all_dividend_events_final_list:
        currency_code_final = div_event['currency']; payment_date_final = div_event['date']; ticker_final = div_event['ticker']
        rate_val_div = Decimal('1.0') # Default for RUB
        cbr_rate_str_for_event = "1.0000"
        if currency_code_final != 'RUB':
            currency_model_f = Currency.objects.filter(char_code=currency_code_final).first()
            if currency_model_f:
                _, fetched_f, rate_val_fetched = _get_exchange_rate_for_date(request, currency_model_f, payment_date_final, f"дивиденд {ticker_final}")
                if rate_val_fetched is not None:
                    rate_val_div = rate_val_fetched
                    cbr_rate_str_for_event = f"{rate_val_div:.4f}"
                    if not fetched_f: cbr_rate_str_for_event += " (ближ.)"
                else: cbr_rate_str_for_event = "не найден" # Error fetching rate
            else: cbr_rate_str_for_event = "валюта?" # Currency not in DB
        div_event['cbr_rate_str'] = cbr_rate_str_for_event
        div_event['amount_rub'] = (div_event['amount'] * rate_val_div).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        # Note: tax_amount is already in the dividend currency, it also needs conversion to RUB if not RUB.
        # This part is missing and should be added if tax_amount_rub is needed for display or calculation.
        total_dividends_rub_for_year += div_event['amount_rub']


    # Sort all operations (trades and initial holdings) by date and time for FIFO processing
    conversion_events_for_display_accumulator = [] # Initialize list to hold conversion display data
    trade_and_holding_ops.sort(key=lambda x: x.get('datetime_obj') or datetime.min) # Sort by datetime
    if trade_and_holding_ops:
        logger.info(f"FFG_NDFL: PROCESS_DEBUG: Starting FIFO processing for {len(trade_and_holding_ops)} operations.")
    _process_all_operations_for_fifo(request, trade_and_holding_ops, full_instrument_trade_history_for_fifo, relevant_files_for_history, conversion_events_for_display_accumulator, _processing_had_error_local_flag)


    # Prepare data for display: combine trades, initial holdings, and conversion events
    all_display_events = []
    # Add trades to display list
    for isin_key, trades_list_for_isin in full_instrument_trade_history_for_fifo.items():
        for trade_dict_updated_with_fifo in trades_list_for_isin:
            dt_obj = datetime.min # Default datetime
            if trade_dict_updated_with_fifo.get('date'): # Parse date string to datetime
                try: dt_obj = datetime.strptime(trade_dict_updated_with_fifo['date'], '%Y-%m-%d %H:%M:%S')
                except ValueError: logger.warning(f"FFG_NDFL: Некорректная дата в trade_dict_updated_with_fifo: {trade_dict_updated_with_fifo.get('date')}")
            # For buy operations, if FIFO cost is not set (or zero), set display string to None (or handle as needed)
            if trade_dict_updated_with_fifo.get('operation','').lower() == 'buy' and \
               (trade_dict_updated_with_fifo.get('fifo_cost_rub_decimal') is None or trade_dict_updated_with_fifo.get('fifo_cost_rub_decimal') == Decimal(0)):
                trade_dict_updated_with_fifo['fifo_cost_rub_str'] = None # Or suitable placeholder
            all_display_events.append({'display_type': 'trade', 'datetime_obj': dt_obj, 'event_details': trade_dict_updated_with_fifo, 'isin_group_key': trade_dict_updated_with_fifo.get('isin')})

    # Add initial holdings to display list (ensure they are not duplicated if referenced multiple times)
    processed_no_refs_ids = set() # To avoid duplicating initial holdings in display
    for op in trade_and_holding_ops: # Iterate through the original ops list that went into FIFO
        if op.get('op_type') == 'initial_holding' and op.get('original_trade_dict_ref'):
            ref_id_check = id(op['original_trade_dict_ref']) # Use id of the dict as a unique key
            if ref_id_check not in processed_no_refs_ids:
                # Initial holdings don't have a FIFO cost themselves; their cost is used by sales.
                op['original_trade_dict_ref']['fifo_cost_rub_str'] = None; 
                op['original_trade_dict_ref']['fifo_cost_rub_decimal'] = None;
                all_display_events.append({'display_type': 'initial_holding', 'datetime_obj': op['datetime_obj'], 'event_details': op['original_trade_dict_ref'], 'isin_group_key': op.get('isin')})
                processed_no_refs_ids.add(ref_id_check)

    # Add conversion events to display list
    for conv_event_data in conversion_events_for_display_accumulator: # This list is populated by _apply_conversion_on_demand
        all_display_events.append({'display_type': 'conversion_info', 'datetime_obj': conv_event_data['datetime_obj'], 'event_details': conv_event_data, 'isin_group_key': conv_event_data.get('new_isin')})

    # Aggregate trades that occurred on the same date, for the same ISIN, price, operation, and currency
    processed_events_for_aggregation = []
    loop_idx = 0
    # Pre-sort for aggregation: ISIN, then Date (ignoring time for date-grouping), then Price, Operation, Currency, then full datetime for tie-breaking
    all_display_events.sort(key=lambda x: (
        x['event_details'].get('isin') if x.get('display_type') == 'trade' and x.get('event_details') else x.get('isin_group_key', ''), # Primary sort by ISIN
        (x.get('datetime_obj').date() if isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) if x.get('display_type') == 'trade' and x.get('datetime_obj') else (x.get('datetime_obj') or date.min), # Then by date part
        x['event_details'].get('p') if x.get('display_type') == 'trade' and x.get('event_details') else None, # Then price
        x['event_details'].get('operation','').lower() if x.get('display_type') == 'trade' and x.get('event_details') else '', # Then operation
        x['event_details'].get('curr_c') if x.get('display_type') == 'trade' and x.get('event_details') else '', # Then currency
        (datetime.combine(x.get('datetime_obj'), datetime.min.time()) if isinstance(x.get('datetime_obj'), date) and not isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) or datetime.min # Finally, full datetime
    ))

    while loop_idx < len(all_display_events):
        current_event_wrapper = all_display_events[loop_idx]; details = current_event_wrapper.get('event_details'); display_type = current_event_wrapper.get('display_type')
        if display_type == 'trade' and details and current_event_wrapper.get('datetime_obj'): # Only aggregate trades
            key_date_obj = current_event_wrapper.get('datetime_obj'); key_date_for_agg = key_date_obj.date() if isinstance(key_date_obj, datetime) else key_date_obj
            key_isin = details.get('isin'); key_price = details.get('p'); key_operation = details.get('operation','').lower(); key_currency = details.get('curr_c')
            
            trades_to_potentially_aggregate = [current_event_wrapper]; next_idx = loop_idx + 1
            while next_idx < len(all_display_events):
                next_event_wrapper = all_display_events[next_idx]; next_details = next_event_wrapper.get('event_details'); next_display_type = next_event_wrapper.get('display_type')
                next_datetime_obj = next_event_wrapper.get('datetime_obj'); next_date_for_agg = None
                if next_datetime_obj: next_date_for_agg = next_datetime_obj.date() if isinstance(next_datetime_obj, datetime) else next_datetime_obj

                # Check conditions for aggregation
                if (next_display_type == 'trade' and next_details and next_date_for_agg == key_date_for_agg and
                    next_details.get('isin') == key_isin and next_details.get('p') == key_price and
                    next_details.get('operation','').lower() == key_operation and next_details.get('curr_c') == key_currency):
                    trades_to_potentially_aggregate.append(next_event_wrapper); next_idx += 1
                else: break # Conditions not met, stop collecting for this group
            
            if len(trades_to_potentially_aggregate) > 1: # Aggregate if more than one trade in group
                first_trade_wrapper = trades_to_potentially_aggregate[0]; combined_details = first_trade_wrapper['event_details'].copy()
                total_q, total_summ, total_commission, total_fifo_cost_rub = Decimal(0), Decimal(0), Decimal(0), Decimal(0)
                trade_ids_list = []
                
                for trade_wrapper_item in trades_to_potentially_aggregate:
                    detail_item = trade_wrapper_item['event_details']
                    total_q += detail_item.get('q', Decimal(0)); total_summ += detail_item.get('summ', Decimal(0)); total_commission += detail_item.get('commission', Decimal(0))
                    
                    fifo_cost_val = detail_item.get('fifo_cost_rub_decimal', Decimal(0)) # Default to 0 if missing
                    if not isinstance(fifo_cost_val, Decimal): # Ensure it's Decimal
                        fifo_cost_val = _str_to_decimal_safe(fifo_cost_val, 'fifo_cost_rub_decimal aggregation', detail_item.get('trade_id'), _processing_had_error_local_flag)
                    total_fifo_cost_rub += fifo_cost_val
                    trade_ids_list.append(str(detail_item.get('trade_id', ''))) # Collect trade IDs

                combined_details['q'] = total_q; combined_details['summ'] = total_summ; combined_details['commission'] = total_commission;
                combined_details['fifo_cost_rub_decimal'] = total_fifo_cost_rub
                if combined_details.get('operation','').lower() == 'sell': # Update string representation for sell
                    combined_details['fifo_cost_rub_str'] = f"{total_fifo_cost_rub:.2f}"
                elif combined_details.get('operation','').lower() == 'buy': # Buys don't have a direct "FIFO cost" in this context
                    combined_details['fifo_cost_rub_str'] = None 
                    combined_details['fifo_cost_rub_decimal'] = None


                aggregated_id_display_count = 3; aggregated_id_str = ", ".join(filter(None, trade_ids_list[:aggregated_id_display_count]))
                if len(trade_ids_list) > aggregated_id_display_count: aggregated_id_str += "...";
                combined_details['trade_id'] = f"Агрегировано ({len(trade_ids_list)} сделок): {aggregated_id_str}" # Updated ID
                combined_details['is_aggregated'] = True
                
                aggregated_wrapper = {'display_type': 'trade', 'datetime_obj': first_trade_wrapper['datetime_obj'], 'event_details': combined_details, 'isin_group_key': combined_details.get('isin')}
                processed_events_for_aggregation.append(aggregated_wrapper); loop_idx = next_idx
            else: # Not aggregated, add current event as is
                if current_event_wrapper.get('event_details'): 
                    current_event_wrapper['event_details']['is_aggregated'] = False
                    # Ensure buy ops without FIFO cost have None for string display
                    if current_event_wrapper['event_details'].get('operation','').lower() == 'buy' and \
                       (current_event_wrapper['event_details'].get('fifo_cost_rub_decimal') is None or current_event_wrapper['event_details'].get('fifo_cost_rub_decimal') == Decimal(0)):
                        current_event_wrapper['event_details']['fifo_cost_rub_str'] = None
                processed_events_for_aggregation.append(current_event_wrapper); loop_idx += 1
        else: # Non-trade events, pass through
            if details: details['is_aggregated'] = False # Ensure flag is set for non-aggregated items
            processed_events_for_aggregation.append(current_event_wrapper); loop_idx += 1
    all_display_events = processed_events_for_aggregation # Replace with processed list


    # Final sort for display: by datetime, then by type (initial, buy, conversion, sell)
    all_display_events.sort(key=lambda x: (
        (datetime.combine(x.get('datetime_obj'), datetime.min.time()) if isinstance(x.get('datetime_obj'), date) and not isinstance(x.get('datetime_obj'), datetime) else x.get('datetime_obj')) or datetime.min, # Primary sort by full datetime
        0 if x.get('display_type') == 'initial_holding' else \
        (1 if x.get('display_type') == 'trade' and x.get('event_details') and x['event_details'].get('operation','').lower() == 'buy' else \
        (2 if x.get('display_type') == 'conversion_info' else 3)) # Secondary sort by operation type order
    ))

    # Determine instruments with sales in the target year to filter history display
    instruments_with_sales_in_target_year = set()
    files_for_sales_scan = UploadedXMLFile.objects.filter(user=user, year=target_report_year) # Only files of the target year
    logger.info(f"FFG_NDFL: PROCESS_DEBUG: files_for_sales_scan for commission calculation (year {target_report_year}): {[f.original_filename for f in files_for_sales_scan]}")

    if files_for_sales_scan.exists():
        for file_instance_scan in files_for_sales_scan:
            try:
                with file_instance_scan.xml_file.open('rb') as xml_file_content_stream:
                    content_bytes = xml_file_content_stream.read(); xml_string_loop = ""
                    try: xml_string_loop = content_bytes.decode('utf-8')
                    except UnicodeDecodeError: xml_string_loop = content_bytes.decode('windows-1251', errors='replace')
                    if not xml_string_loop: continue # Skip empty files
                    root = ET.fromstring(xml_string_loop); trades_element = root.find('.//trades')
                    if trades_element:
                        detailed_element = trades_element.find('detailed')
                        if detailed_element:
                            for node_element in detailed_element.findall('node'):
                                instr_type_el_sale = node_element.find('instr_type')
                                if instr_type_el_sale is None or instr_type_el_sale.text != '1': continue # Only stock-like instruments
                                operation_el = node_element.find('operation'); 
                                # Prioritize 'isin' then 'issue_nb' for ISIN
                                isin_el_sale = node_element.find('isin')
                                isin_to_check_sale = isin_el_sale.text.strip() if isin_el_sale is not None and isin_el_sale.text and isin_el_sale.text.strip() != '-' else None
                                if not isin_to_check_sale : 
                                    isin_el_sale_nb = node_element.find('issue_nb')
                                    isin_to_check_sale = isin_el_sale_nb.text.strip() if isin_el_sale_nb is not None and isin_el_sale_nb.text and isin_el_sale_nb.text.strip() != '-' else None
                                
                                if (operation_el is not None and operation_el.text and operation_el.text.strip().lower() == 'sell' and isin_to_check_sale):
                                    instruments_with_sales_in_target_year.add(isin_to_check_sale)
            except Exception as e_sales_scan: _processing_had_error_local_flag[0] = True; logger.error(f"FFG_NDFL: PROCESS_DEBUG: Ошибка сканирования продаж: {e_sales_scan}", exc_info=True)

    # Group events by the "final" ISIN after all conversions for display
    final_instrument_event_history = defaultdict(list)
    conversion_map_old_to_new = {}; conversion_map_new_to_old = {}; processed_conversion_ids_for_map = set()
    # Build conversion map from sorted conversion events
    temp_conversion_events_for_map = sorted(
        [evt_wrapper for evt_wrapper in all_display_events if evt_wrapper.get('display_type') == 'conversion_info'],
        key=lambda x: x.get('datetime_obj') or date.min # Sort by date
    )
    for event_wrapper in temp_conversion_events_for_map: # Build conversion chain map
        event = event_wrapper.get('event_details')
        if event and event.get('corp_action_id') not in processed_conversion_ids_for_map: # Process each CA ID once
            old_i = event.get('old_isin'); new_i = event.get('new_isin')
            if old_i and new_i : conversion_map_old_to_new[old_i] = new_i; conversion_map_new_to_old[new_i] = old_i; processed_conversion_ids_for_map.add(event['corp_action_id'])

    relevant_isins_for_display = set() # ISINs whose history should be shown
    for sold_isin in instruments_with_sales_in_target_year: # Start with ISINs sold in target year
        relevant_isins_for_display.add(sold_isin); temp_isin = sold_isin
        # Trace back through conversions (new to old)
        while temp_isin in conversion_map_new_to_old:
            prev_isin = conversion_map_new_to_old[temp_isin]
            if prev_isin == temp_isin: break # Avoid infinite loop on self-conversion if data error
            relevant_isins_for_display.add(prev_isin); temp_isin = prev_isin
        # Trace forward through conversions (old to new) - less common for historical display but good for completeness
        temp_isin = sold_isin
        while temp_isin in conversion_map_old_to_new:
            next_isin = conversion_map_old_to_new[temp_isin]
            if next_isin == temp_isin: break
            relevant_isins_for_display.add(next_isin); temp_isin = next_isin
            
    # Filter and group events for display
    for event_data_wrapper in all_display_events:
        details = event_data_wrapper.get('event_details'); display_type = event_data_wrapper.get('display_type')
        current_event_isin = None # The ISIN directly associated with this event
        if details:
            if display_type == 'trade' or display_type == 'initial_holding': current_event_isin = details.get('isin')
            elif display_type == 'conversion_info': current_event_isin = details.get('new_isin') # For conversions, the "new" ISIN is primary for this event
        
        if not current_event_isin: # Should not happen if data is clean
            log_id = "N/A"; 
            if details: log_id = details.get('trade_id', details.get('corp_action_id', 'Details available but no ID'))
            logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Пропуск события без основного ISIN при финальной группировке: {log_id}"); continue

        # Determine the "root" or "final" ISIN for grouping (trace forward)
        grouping_key_isin = current_event_isin
        while grouping_key_isin in conversion_map_old_to_new and conversion_map_old_to_new[grouping_key_isin] != grouping_key_isin: # Follow chain to the end
             grouping_key_isin = conversion_map_old_to_new[grouping_key_isin]
        
        # Check if this event's chain is relevant for display
        should_display_this_event = False
        if current_event_isin in relevant_isins_for_display: should_display_this_event = True
        elif grouping_key_isin in relevant_isins_for_display: should_display_this_event = True
        # For conversions, also check if the "old" ISIN is relevant
        elif display_type == 'conversion_info' and details and details.get('old_isin') in relevant_isins_for_display: should_display_this_event = True
        
        if should_display_this_event: final_instrument_event_history[grouping_key_isin].append(event_data_wrapper)

    final_instrument_event_history = {k: v for k, v in final_instrument_event_history.items() if v} # Remove empty groups
    all_dividend_events_final_list.sort(key=lambda x: (x.get('date') or date.min, x.get('instrument_name', ''))) # Sort dividends for display

    # Calculate total sales profit for the target year
    if final_instrument_event_history: # Only if there's history to process
        for isin_key, event_list_for_isin in final_instrument_event_history.items():
            for event_wrapper in event_list_for_isin:
                if event_wrapper.get('display_type') == 'trade':
                    details = event_wrapper.get('event_details')
                    if details and details.get('operation','').lower() == 'sell': # Process only sales
                        event_datetime_obj = event_wrapper.get('datetime_obj')
                        
                        # Ensure sale occurred in the target report year
                        if event_datetime_obj and event_datetime_obj.year == target_report_year:
                            sale_amount_curr = details.get('summ', Decimal(0)) # Gross sale amount in currency
                            # commission_curr = details.get('commission', Decimal(0)) # Commission in currency - already part of fifo_cost_rub_decimal for sells
                            currency_code = details.get('curr_c', 'RUB')

                            cbr_rate_for_sale = Decimal('1.0') # Default for RUB
                            if currency_code != 'RUB':
                                rate_str_from_event = details.get('transaction_cbr_rate_str', "0") # Get rate string from event
                                match_rate = re.search(r"(\d+(\.\d+)?)", rate_str_from_event) # Extract numeric part
                                if match_rate:
                                    try: cbr_rate_for_sale = Decimal(match_rate.group(1))
                                    except InvalidOperation: 
                                        logger.error(f"FFG_NDFL: PROCESS_DEBUG: Failed to parse CBR rate: '{rate_str_from_event}' for sale ID {details.get('trade_id')}. Using 0 for non-RUB.")
                                        if currency_code != 'RUB': cbr_rate_for_sale = Decimal(0) # Critical if rate cannot be parsed for non-RUB
                                elif currency_code != 'RUB': # If no rate string or unmatchable for non-RUB
                                    logger.error(f"FFG_NDFL: PROCESS_DEBUG: CBR rate string not found or unmatchable: '{rate_str_from_event}' for currency {currency_code}, sale ID {details.get('trade_id')}. Using 0.")
                                    cbr_rate_for_sale = Decimal(0)

                            # Ensure sale_amount_curr is Decimal
                            sale_amount_curr = _str_to_decimal_safe(sale_amount_curr, 'sale_amount_for_total_profit_calc', details.get('trade_id'), _processing_had_error_local_flag)

                            income_from_sale_gross_rub = decimal_context.multiply(sale_amount_curr, cbr_rate_for_sale)
                            
                            # FIFO cost already includes buy cost + sell commission in RUB
                            total_expenses_for_sale_rub = details.get('fifo_cost_rub_decimal', Decimal(0))
                            if total_expenses_for_sale_rub is None: total_expenses_for_sale_rub = Decimal(0) # Handle None
                            elif not isinstance(total_expenses_for_sale_rub, Decimal): # Ensure it's Decimal
                                total_expenses_for_sale_rub = _str_to_decimal_safe(total_expenses_for_sale_rub, 'total_expenses_for_profit_calc', details.get('trade_id'), _processing_had_error_local_flag)

                            profit_for_this_sale_rub = income_from_sale_gross_rub - total_expenses_for_sale_rub
                            total_sales_profit_rub_for_year += profit_for_this_sale_rub


    # User messages based on processing results
    if not final_instrument_event_history and not all_dividend_events_final_list and instruments_with_sales_in_target_year:
         messages.warning(request, f"Найдены продажи в {target_report_year} для {list(instruments_with_sales_in_target_year)}, но не удалось собрать историю операций или дивидендов для них.")
    elif not final_instrument_event_history and not all_dividend_events_final_list and not instruments_with_sales_in_target_year and UploadedXMLFile.objects.filter(user=user, year=target_report_year).exists(): # Check if files for target year even exist
        messages.info(request, f"В отчетах за {target_report_year} год не найдено продаж по ценным бумагам и не найдено дивидендов для отображения.")
    
    # Call _calculate_additional_commissions to get all commission details
    # files_for_sales_scan are the files from target_report_year, suitable for commission calculation for that year.
    dividend_commissions_details, other_commissions_details, total_other_commissions_rub_val = _calculate_additional_commissions(request, user, target_report_year, files_for_sales_scan, _processing_had_error_local_flag)


    logger.info(f"FFG_NDFL: PROCESS_DEBUG: Finished process_and_get_trade_data for year {target_report_year}.")
    if _processing_had_error_local_flag[0]: # Check the local flag
        logger.warning(f"FFG_NDFL: PROCESS_DEBUG: Processing for year {target_report_year} had one or more errors.")
        # messages.error(request, "При обработке данных возникли ошибки. Проверьте логи или сообщения на странице для деталей.") # Generic error message if needed

    # Ensure all 8 values are returned
    return final_instrument_event_history, all_dividend_events_final_list, total_dividends_rub_for_year, total_sales_profit_rub_for_year, _processing_had_error_local_flag[0], dividend_commissions_details, other_commissions_details, total_other_commissions_rub_val