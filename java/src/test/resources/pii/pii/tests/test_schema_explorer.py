from unittest import TestCase

from pii.schema_explorer import SchemaExplorer


class TestSchemaExplorer(TestCase):
    def test_basic(self):
        record = {'first_name': 'John', 'last_name': 'Doe', 'age': 25, 'balance': 123.45, 'is_premium': True}
        explorer = SchemaExplorer(record)
        text_values = explorer.all_text_values()
        self.assertIn('John', text_values)
        self.assertIn('Doe', text_values)

    def test_field_names(self):
        record = {'first_name': 'John', 'last_name': 'Doe', 'age': 25, 'balance': 123.45, 'is_premium': True,
                  'address': {'address_1': '1 Main street', 'city': 'Manchester'}}
        explorer = SchemaExplorer(record)
        text_values = explorer.all_text_fields()

        assert ('address.address_1', '1 Main street') in text_values
        assert ('last_name', 'Doe') in text_values

    def test_field_access(self):
        empty = SchemaExplorer({})
        self.assertEqual(empty.get_parent_from_field_path_list([""]), None)

        simple = {"first": "value1"}
        expl = SchemaExplorer(simple)
        parent1 = expl.get_parent_from_field_path_list(["first"])
        self.assertEqual(parent1, simple)

        two_levels = {"first": {"second": "value2"}}
        expl = SchemaExplorer(two_levels)
        parent2 = expl.get_parent_from_field_path_list(["first", "second"])
        self.assertEqual(parent2, two_levels["first"])

        three_levels = {"first": {"second": {"third": "value3"}}}
        expl = SchemaExplorer(three_levels)
        parent3 = expl.get_parent_from_field_path_list(["first", "second", "third"])
        self.assertEqual(parent3, three_levels["first"]["second"])

        not_exist = expl.get_parent_from_field_path_list(["not_found", "other"])
        self.assertEqual(not_exist, None)

    def test_update_field(self):
        three_levels = {"first": {"second": {"third": "value3"}}}
        expl = SchemaExplorer(three_levels)
        result = expl.update_field_from_dotted("first.second.third", "new value")
        self.assertTrue(result)
        self.assertEqual(three_levels["first"]["second"]["third"], "new value")

        result = expl.update_field_from_dotted("first.second.nope", "new value")
        self.assertFalse(result)

        result = expl.update_field_from_dotted("first.nope.third", "new value")
        self.assertFalse(result)
