import json
import os
import tempfile
import unittest

from services.engine.src.items import ItemPriceManager, item_price_manager
from services.engine.src.poller import TelemetryEngine

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "sample_live_data.json")


class TestItemPrices(unittest.TestCase):
    def test_known_items_total_gold(self):
        """Verifies that Data Dragon provides true total gold for completed items and components."""
        # Completed items whose combine/recipe cost in Riot API is much lower than total
        self.assertEqual(item_price_manager.get_item_price(3089), 3600)  # Rabadon's Deathcap
        self.assertEqual(item_price_manager.get_item_price(3003), 2900)  # Archangel's Staff
        self.assertEqual(item_price_manager.get_item_price(3135), 3000)  # Void Staff
        self.assertEqual(item_price_manager.get_item_price(6657), 2600)  # Rod of Ages

        # Basic components & consumables
        self.assertEqual(item_price_manager.get_item_price(1055), 450)  # Doran's Blade
        self.assertEqual(item_price_manager.get_item_price(2055), 75)  # Control Ward
        self.assertEqual(item_price_manager.get_item_price(1001), 300)  # Boots
        self.assertEqual(item_price_manager.get_item_price(3340), 0)  # Stealth Ward

    def test_unknown_item_fallback(self):
        """Verifies that unmapped or non-existent items fall back to raw price without error."""
        self.assertEqual(item_price_manager.get_item_price(999999, fallback=333), 333)
        self.assertEqual(item_price_manager.get_item_price(None, fallback=450), 450)
        self.assertEqual(item_price_manager.get_item_price(0, fallback=100), 100)

    def test_sample_live_data_calculation_with_ddragon(self):
        """Verifies that calculate_metrics_and_features calculates true item gold for sample_live_data.json."""
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = TelemetryEngine(model_path="services/engine/models/model.onnx", data_dir=temp_dir)

            with open(FIXTURE_PATH, encoding="utf-8-sig") as f:
                sample_data = json.load(f)

            payload = engine.calculate_metrics_and_features(sample_data)

            # Check Ryze (Order team player 0)
            ryze = payload["blue_team"][0]
            self.assertEqual(ryze["champion"], "Ryze")

            # Expected items:
            # 3865 (World Atlas: 400g)
            # 3003 (Archangel's: 2900g)
            # 6657 (Rod of Ages: 2600g)
            # 3089 (Rabadon's Deathcap: 3600g)
            # 3135 (Void Staff: 3000g)
            # 3340 (Trinket: 0g)
            # Total: 400 + 2900 + 2600 + 3600 + 3000 = 12500g (previously only 3450g)
            self.assertEqual(ryze["items_gold"], 12500)

            # Check that individual regular items have enriched 'total_price'
            regular_items = ryze["items"]
            item_map = {item["itemID"]: item["total_price"] for item in regular_items}
            self.assertEqual(item_map.get(3089), 3600)
            self.assertEqual(item_map.get(3135), 3000)
            self.assertEqual(item_map.get(3003), 2900)
            self.assertEqual(item_map.get(6657), 2600)
            self.assertEqual(item_map.get(3865), 400)

            # Blue total gold must be higher than before due to realistic item values
            self.assertGreater(payload["metrics"]["blue_gold"], 12500)

    def test_cache_load_and_save(self):
        """Verifies saving and reloading a custom cache file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_file = os.path.join(temp_dir, "test_prices.json")
            mgr = ItemPriceManager(cache_path=test_cache_file, default_version="14.20.1")
            self.assertEqual(len(mgr.prices), 0)

            mgr.prices = {1001: 300, 3089: 3600}
            mgr.save_cache()
            self.assertTrue(os.path.exists(test_cache_file))

            mgr2 = ItemPriceManager(cache_path=test_cache_file)
            self.assertEqual(mgr2.get_item_price(1001), 300)
            self.assertEqual(mgr2.get_item_price(3089), 3600)
            self.assertEqual(mgr2.version, "14.20.1")


if __name__ == "__main__":
    unittest.main()
