import json
import logging

from pii.analyzer import PresidioAnalyzer, anonymizer
from pii.language_detection import LanguageDetector
from pii.schema_explorer import SchemaExplorer
from pii.settings_utils import list_or_string_to_list
from pii.spacy_model import init_models

from context import PiiSmtContext

_analyze_fn = None
_settings = None


def init(settings):
    global _settings
    _settings = json.loads(settings) if len(settings) > 0 else {}
    # global pii_smt_context
#    pii_smt_context = init_context(models=settings_obj.get("models"),
#                                   entity_types=settings_obj.get("entity_types"),
#                                   recognizers=settings_obj.get("recognizers"),
#                                   languages=settings_obj.get("languages", "auto"))


def init_context(models=None, entity_types=None, recognizers=None, languages="en"):
    # PII detection init
    pii_detection_config = init_models(models)
    analyzer = PresidioAnalyzer(pii_detection_config, entity_types, recognizers)

    # Language detection init (warning: setting it to "auto" costs 800 MB in memory)
    # empty or "auto" = default --> all supported languages are pre-loaded
    # or a list of language codes separated by commas
    # if only language code is provided, no language detection happens
    lang_settings = languages
    if lang_settings == "auto":
        lang_codes = []
    else:
        lang_codes = list_or_string_to_list(lang_settings) if lang_settings else []

    if len(lang_codes) == 1:
        lang_code = lang_codes[0]
        lang_detector = None
    else:
        lang_code = None
        lang_detector = LanguageDetector(lang_codes)

    return PiiSmtContext(
        lang_code=lang_code,
        lang_detector=lang_detector,
        analyzer=analyzer
    )


def anonymize(raw_text, language=None):
    pii_smt_context = init_context()
    print("context initialised")
    if not language:
        language = pii_smt_context.lang_code if pii_smt_context.lang_code \
            else pii_smt_context.lang_detector.detect_lang(raw_text)

    print(f"language={language}")
    analysis_results = pii_smt_context.analyzer.analyze(raw_text, language=language)
    print(f'Analysis results: {analysis_results}')
    anonymised_result = anonymizer.anonymize(text=raw_text, analyzer_results=analysis_results)
    anonymised_text = anonymised_result.text
    print(f'Anonymised Text: {anonymised_text}')
    return anonymised_text


def anonymize_record(record):
    schema_explorer = SchemaExplorer(record)
    aggregated_text = ' '.join(schema_explorer.all_text_values())
    language = pii_smt_context.lang_code if pii_smt_context.lang_code \
        else pii_smt_context.lang_detector.detect_lang(aggregated_text)

    for field_name, field_value in schema_explorer.all_text_fields():
        print(f"anonymising {field_name}: {field_value}")
        anonymized_value = anonymize(field_value, language)
        res = schema_explorer.update_field_from_dotted(field_name, anonymized_value)
        if not res:
            logging.error(f"{field_name} was not found in the record. The field will not be anonymized.")
    return record


def transform_key_or_value(kafka_key_or_value, schema_name):
    if not kafka_key_or_value:
        return None
    if schema_name == "JSON":
        print(f"transform JSON")
        json_obj = json.loads(kafka_key_or_value)
        kafka_key_or_value = anonymize_record(json_obj)
    elif schema_name == "STRING":
        print(f"transform STRING")
        kafka_key_or_value = anonymize(kafka_key_or_value)
    return kafka_key_or_value


def transform(record):
    print(f"received: {record}")
    record['key'] = transform_key_or_value(record['key'], record['key_schema'])
    record['value'] = transform_key_or_value(record['value'], record['value_schema'])
    print(f"transformed into: {record}")
    return record


if __name__ == '__main__':
    init("{\"models\": \"en_core_web_lg, fr_core_news_lg\", \"languages\": \"en, fr\"}")

    record = {'key_schema': 'INT32',
              'topic': 'test-topic-9094d9cc-8b96-4cb1-832f-d5d1ad2718dd',
              'value_schema': 'JSON',
              'value': '{"user":{"first_name":"John","last_name":"Doe","age":25},"balance":123.45,"is_premium":true}',
              'key': 0}
    transform(record)

    record = {'key_schema': 'INT32', 'topic': 'test-topic-5e528032-8ac1-4932-aabf-913dc7017223', 'value_schema': 'STRING',
     'value': 'Value 0', 'key': 0}

    transform(record)

    record = {'key_schema': 'INT64',
              'topic': 'plates-full-text-anonymized',
              'value_schema': 'STRING',
              'value': 'American whole magazine truth stop whose. On traditional measure example sense peace. 489-24115-7 R59-DLN Christine Tran 80160 Clayton Isle Apt. 513\nEast Linda, FL 71671 Ball floor meet usually. Feeling fact by four. ',
              'key': 990}
    transform(record)

