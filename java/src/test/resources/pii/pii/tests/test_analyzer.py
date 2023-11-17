from unittest import TestCase

from pii.analyzer import PresidioAnalyzer, anonymizer
from pii.language_detection import LanguageDetector
from pii.spacy_model import init_models


class TestPresidioAnalyzer(TestCase):
    def setUp(self) -> None:
        pii_detection_config = init_models("en_core_web_lg, fr_core_news_lg")
        self.analyzer = PresidioAnalyzer(nlp_configuration=pii_detection_config, entity_types=[])
        self.lang_detector = LanguageDetector(["en", "fr"])

    def anonymize(self, raw_text):
        language = self.lang_detector.detect_lang(raw_text)
        analysis_results = self.analyzer.analyze(raw_text, language=language)

        anonymized_result = anonymizer.anonymize(text=raw_text, analyzer_results=analysis_results)
        return anonymized_result.text

    def test_analyze(self):
        self.assertEqual(self.anonymize("mon nom est Bob"), "mon nom est Bob")
        self.assertEqual(self.anonymize("mon nom est Bob D"), "mon nom est <PERSON>")
        self.assertEqual(self.anonymize("mon nom est Laurent"), "mon nom est <PERSON>")
        self.assertEqual(self.anonymize("mon nom est Robert"), "mon nom est <PERSON>")
        self.assertEqual(self.anonymize("my name is Bob"), "my name is <PERSON>")
        self.assertEqual(self.anonymize("my name is Laurent"), "my name is <PERSON>")
        self.assertEqual(self.anonymize("my name is Robert"), "my name is <PERSON>")


