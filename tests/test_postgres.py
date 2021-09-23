import unittest

from tests.implementations import TweetRepository


class TestPostgresRepository(unittest.TestCase):

    def test_get_conditions_and_values(self):
        kwargs = {
            'a': None,
            'b': 1,
            'c': 'three',
        }
        conditions, values = TweetRepository._get_conditions_and_values(
            **kwargs)
        expected_conditions = 'b = %s AND c = %s'
        self.assertEqual(expected_conditions, conditions)
        expected_values = [1, 'three']
        self.assertEqual(expected_values, values)

    def test_get_selector_returns_star_if_no_projection(self):
        kwargs = {}
        selector = TweetRepository._get_selector(**kwargs)
        expected = '*'
        self.assertEqual(expected, selector)

    def test_get_selector_returns_star_if_projection_is_none(self):
        kwargs = {'projection': None}
        selector = TweetRepository._get_selector(**kwargs)
        expected = '*'
        self.assertEqual(expected, selector)

    def test_get_selector_returns_projection_if_specified(self):
        kwargs = {'projection': ['attr1', 'attr2']}
        selector = TweetRepository._get_selector(**kwargs)
        expected = 'attr1,attr2'
        self.assertEqual(expected, selector)

    def test_add_inserts_item(self):
        repo = TweetRepository(table='test_add_inserts_item')
        repo.add(1, 'tweet1')
        item = repo.get(1)
        expected = {'tweet_id': 1, 'tweet': 'tweet1'}
        self.assertEqual(expected, item)

    def test_all_returns_all_items(self):
        repo = TweetRepository(table='test_all_returns_all_items')
        repo.add(1, 'tweet1')
        repo.add(2, 'tweet2')
        items = list(repo.all())
        expected = [
            {'tweet_id': 1, 'tweet': 'tweet1'},
            {'tweet_id': 2, 'tweet': 'tweet2'},
        ]
        self.assertEqual(expected, items)

    def test_count_returns_number_of_items_in_table(self):
        repo = TweetRepository(
            table='test_count_returns_number_of_items_in_table')
        repo.add(1, 'tweet1')
        repo.add(2, 'tweet2')
        repo.add(3, 'tweet3')
        count = repo.count()
        self.assertEqual(3, count)

    def test_delete_removes_items(self):
        repo = TweetRepository(table='test_delete_removes_items')
        repo.add(1, 'tweet1')
        self.assertTrue(repo.exists(1))
        repo.delete(1)
        self.assertFalse(repo.exists(1))

    def test_exists_returns_true_when_item_exists(self):
        repo = TweetRepository(
            table='test_exists_returns_true_when_item_exists')
        repo.add(1, 'tweet1')
        self.assertTrue(repo.exists(1))

    def test_exists_returns_false_when_item_does_not_exist(self):
        repo = TweetRepository(
            table='test_exists_returns_false_when_item_does_not_exist')
        self.assertFalse(repo.exists(1))

    def test_get_returns_none_when_item_does_not_exist(self):
        repo = TweetRepository(
            table='test_get_returns_none_when_item_does_not_exist')
        item = repo.get(1)
        self.assertIsNone(item)

    def test_get_returns_item_when_exists(self):
        repo = TweetRepository(
            table='test_get_returns_item_when_exists')
        repo.add(1, 'tweet1')
        item = repo.get(1)
        self.assertIsNotNone(item)

    def test_search_returns_correct_items(self):
        repo = TweetRepository(
            table='test_search_returns_correct_items')
        repo.add(1, 'tweet1')
        repo.add(2, 'tweet1')
        repo.add(3, 'tweet3')
        items = list(repo.search(tweet='tweet1'))
        self.assertEqual(2, len(items))