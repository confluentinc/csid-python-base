from unittest import TestCase

from lingua import Language

from pii.language_detection import LanguageDetector


class TestLanguageDetector(TestCase):
    def test_iso639_1_to_language(self):
        self.assertEqual(LanguageDetector.iso639_1_to_language("fr"), Language.FRENCH)

    def test_language_to_iso639_1(self):
        self.assertEqual(LanguageDetector.language_to_iso639_1(Language.FRENCH), "fr")

    def test_detect_lang(self):
        detector = LanguageDetector([])
        self.assertEqual("fr", detector.detect_lang("mon nom est Laurent"))
        self.assertEqual("en", detector.detect_lang("here is my credit card"))
        self.assertEqual("es", detector.detect_lang("hola cabron que tal"))
        self.assertEqual("it", detector.detect_lang("adesso che cosa fai"))
