# currency_CBRF/services.py
import requests
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from datetime import datetime
from django.conf import settings 
import logging

# Импортируем модели для сохранения данных
from .models import Currency, ExchangeRate # <--- ДОБАВЛЕНО

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler() 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def fetch_daily_rates(date_str=None):
    """
    Получает ежедневные курсы валют с сайта ЦБ РФ и сохраняет их в БД.
    date_str: дата в формате 'dd/mm/yyyy'. Если None, запрашиваются последние доступные курсы.
    Возвращает кортеж (parsed_rates_list, rates_date_object) или (None, None) в случае ошибки.
    parsed_rates_list: список словарей с данными курсов, которые были успешно обработаны (не обязательно все, что вернул ЦБ).
    rates_date_object: объект date, на которую ЦБ вернул курсы.
    """
    base_url = getattr(settings, 'CBRF_API_BASE_URL', "http://www.cbr.ru/scripts/")
    url = base_url + "XML_daily.asp"
    timeout_daily = getattr(settings, 'CBRF_API_TIMEOUT_DAILY', 10)
    
    params = {}
    if date_str:
        try:
            datetime.strptime(date_str, '%d/%m/%Y')
            params['date_req'] = date_str
        except ValueError:
            logger.error(f"Неверный формат даты для XML_daily.asp: {date_str}. Используйте dd/mm/yyyy.")
            return None, None
            
    raw_parsed_rates_from_xml = [] # Список для данных, как они пришли из XML
    successfully_saved_rate_char_codes = [] # Для логирования, какие курсы сохранены

    try:
        logger.info(f"Запрос курсов ЦБ: URL={url}, Params={params}")
        response = requests.get(url, params=params, timeout=timeout_daily)
        response.raise_for_status() 
        response.encoding = 'windows-1251' 
        xml_data = response.text
        root = ET.fromstring(xml_data)
        rates_date_str_from_xml = root.get('Date')

        if not rates_date_str_from_xml:
            logger.error("Не удалось получить дату курсов ('Date' attribute) из ответа XML_daily.asp.")
            return None, None
        
        try:
            rates_date_obj_from_xml = datetime.strptime(rates_date_str_from_xml, '%d.%m.%Y').date()
        except ValueError:
            logger.error(f"Некорректный формат даты '{rates_date_str_from_xml}' в атрибуте 'Date' XML_daily.asp.")
            return None, None

        for valute_node in root.findall('Valute'):
            cbr_id = valute_node.get('ID')
            num_code_node = valute_node.find('NumCode')
            char_code_node = valute_node.find('CharCode')
            nominal_node = valute_node.find('Nominal')
            name_node = valute_node.find('Name')
            value_node = valute_node.find('Value')

            num_code_text = num_code_node.text.strip() if num_code_node is not None and num_code_node.text else None
            char_code_text = char_code_node.text.strip() if char_code_node is not None and char_code_node.text else None
            nominal_text = nominal_node.text.strip() if nominal_node is not None and nominal_node.text else None
            name_text = name_node.text.strip() if name_node is not None and name_node.text else None
            value_text = value_node.text.strip() if value_node is not None and value_node.text else None

            if not all([cbr_id, num_code_text, char_code_text, nominal_text, name_text, value_text]):
                logger.warning(f"Пропущены неполные данные для одной из валют в XML_daily.asp. Valute ID: {cbr_id}.")
                continue

            try:
                value_decimal = Decimal(value_text.replace(',', '.'))
                nominal_int = int(nominal_text)
                
                # Собираем данные, как они пришли из XML
                rate_data_from_xml = {
                    'cbr_id': cbr_id, 'num_code': num_code_text, 'char_code': char_code_text,
                    'nominal': nominal_int, 'name': name_text, 'value': value_decimal,
                    'date': rates_date_obj_from_xml # Важно: это дата, на которую ЦБ дал курсы
                }
                raw_parsed_rates_from_xml.append(rate_data_from_xml)

                # Пытаемся сохранить в БД
                currency_model_instance = Currency.objects.filter(char_code=char_code_text).first()
                if currency_model_instance:
                    # Проверяем, существует ли уже такой курс, чтобы не создавать дубликаты
                    # Используем дату, которую вернул ЦБ (rates_date_obj_from_xml)
                    if not ExchangeRate.objects.filter(currency=currency_model_instance, date=rates_date_obj_from_xml).exists():
                        ExchangeRate.objects.create(
                            currency=currency_model_instance,
                            date=rates_date_obj_from_xml, # Сохраняем на дату от ЦБ
                            value=value_decimal,
                            nominal=nominal_int
                            # unit_rate будет вычисляться через @property в модели
                        )
                        successfully_saved_rate_char_codes.append(char_code_text)
                        # logger.info(f"Сохранен курс: {char_code_text} на {rates_date_obj_from_xml.strftime('%d.%m.%Y')}")
                    # else:
                        # logger.info(f"Курс для {char_code_text} на {rates_date_obj_from_xml.strftime('%d.%m.%Y')} уже существует в БД.")
                else:
                    logger.warning(f"Валюта {char_code_text} из ответа ЦБ не найдена в справочнике Currency. Курс не сохранен.")

            except (InvalidOperation, ValueError) as e_convert:
                logger.error(f"Ошибка конвертации данных для {char_code_text} (ID: {cbr_id}) в XML_daily.asp: {e_convert}. Value: '{value_text}', Nominal: '{nominal_text}'")
                continue 
        
        if successfully_saved_rate_char_codes:
             logger.info(f"Успешно сохранено {len(successfully_saved_rate_char_codes)} новых курсов в БД на дату {rates_date_obj_from_xml.strftime('%d.%m.%Y')}: {', '.join(successfully_saved_rate_char_codes)}.")
        elif raw_parsed_rates_from_xml: # Если что-то было получено, но ничего нового не сохранено (все уже было)
            logger.info(f"Все курсы ({len(raw_parsed_rates_from_xml)} шт.) от ЦБ на {rates_date_obj_from_xml.strftime('%d.%m.%Y')} уже присутствовали в БД.")
        else: # Если ничего не было получено/распарсено из XML
            logger.info(f"Не было получено корректных данных о курсах от ЦБ на {rates_date_obj_from_xml.strftime('%d.%m.%Y')}.")


        # Возвращаем список всех успешно распарсенных данных из XML и дату, на которую ЦБ дал эти курсы
        return raw_parsed_rates_from_xml, rates_date_obj_from_xml

    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при запросе к XML_daily.asp ({url}, params: {params})")
        return None, None
    except requests.exceptions.HTTPError as e_http:
        logger.error(f"HTTP ошибка при запросе к XML_daily.asp: {e_http}. URL: {url}, Params: {params}, Status: {e_http.response.status_code}, Response: {e_http.response.text[:500]}")
        return None, None
    except requests.exceptions.RequestException as e_req:
        logger.error(f"Сетевая ошибка при запросе к XML_daily.asp: {e_req}. URL: {url}, Params: {params}")
        return None, None
    except ET.ParseError as e_parse:
        logger.error(f"Ошибка парсинга XML из XML_daily.asp: {e_parse}. Данные (первые 500 символов): {xml_data[:500] if 'xml_data' in locals() else 'N/A'}")
        return None, None
    except Exception as e_unexpected:
        logger.exception(f"Непредвиденная ошибка при получении и сохранении ежедневных курсов: {e_unexpected}")
        return None, None

# fetch_period_rates остается без изменений, так как он не используется для автоматического сохранения в текущей логике.
# Если для него тоже нужно автосохранение, его нужно будет доработать аналогично.
def fetch_period_rates(cbr_id, date_req1_str, date_req2_str):
    """
    Получает динамику курса для одной валюты за период.
    НЕ СОХРАНЯЕТ В БД АВТОМАТИЧЕСКИ.
    """
    base_url = getattr(settings, 'CBRF_API_BASE_URL', "http://www.cbr.ru/scripts/")
    url = base_url + "XML_dynamic.asp"
    timeout_period = getattr(settings, 'CBRF_API_TIMEOUT_PERIOD', 30)
    params = {'date_req1': date_req1_str, 'date_req2': date_req2_str, 'VAL_NM_RQ': cbr_id}    
    try:
        datetime.strptime(date_req1_str, '%d/%m/%Y'); datetime.strptime(date_req2_str, '%d/%m/%Y')
    except ValueError:
        logger.error(f"Неверный формат дат для XML_dynamic.asp: {date_req1_str}, {date_req2_str}.")
        return None
    parsed_rates = []
    try:
        logger.info(f"Запрос динамики курсов ЦБ: URL={url}, Params={params}")
        response = requests.get(url, params=params, timeout=timeout_period)
        response.raise_for_status(); response.encoding = 'windows-1251'; xml_data = response.text
        root = ET.fromstring(xml_data)
        if not root.findall('Record'):
            logger.info(f"Нет данных 'Record' для {cbr_id} за период {date_req1_str}-{date_req2_str} в XML_dynamic.asp")
            return parsed_rates 
        for record_node in root.findall('Record'):
            date_str_rec = record_node.get('Date')
            nominal_node = record_node.find('Nominal'); value_node = record_node.find('Value')
            nominal_text = nominal_node.text.strip() if nominal_node is not None and nominal_node.text else None
            value_text = value_node.text.strip() if value_node is not None and value_node.text else None
            if not all([date_str_rec, nominal_text, value_text]):
                logger.warning(f"Неполные данные для {cbr_id} в XML_dynamic.asp: {ET.tostring(record_node, encoding='unicode')}")
                continue
            try:
                value = Decimal(value_text.replace(',', '.')); nominal = int(nominal_text)
                record_date = datetime.strptime(date_str_rec, '%d.%m.%Y').date()
                parsed_rates.append({'cbr_id': cbr_id, 'nominal': nominal, 'value': value, 'date': record_date})
            except (InvalidOperation, ValueError) as e_convert_dyn:
                logger.error(f"Ошибка конвертации для {cbr_id} на {date_str_rec} в XML_dynamic.asp: {e_convert_dyn}.")
        logger.info(f"Получено {len(parsed_rates)} записей для {cbr_id} за {date_req1_str}-{date_req2_str}")
        return parsed_rates
    except requests.exceptions.Timeout: logger.error(f"Таймаут XML_dynamic.asp для {cbr_id}"); return None
    except requests.exceptions.HTTPError as e: logger.error(f"HTTP ошибка XML_dynamic.asp для {cbr_id}: {e}"); return None
    except requests.exceptions.RequestException as e: logger.error(f"Сетевая ошибка XML_dynamic.asp для {cbr_id}: {e}"); return None
    except ET.ParseError as e: logger.error(f"Ошибка парсинга XML_dynamic.asp для {cbr_id}: {e}"); return None
    except Exception as e: logger.exception(f"Непредвиденная ошибка XML_dynamic.asp для {cbr_id}: {e}"); return None
