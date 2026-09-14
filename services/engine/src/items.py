import json
import logging
import os

import httpx

logger = logging.getLogger("rift-pulse.items")

DEFAULT_DDRAGON_VERSION = "14.20.1"
DEFAULT_CACHE_PATH = os.path.join("data", "item_prices.json")


class ItemPriceManager:
    """
    Resolves total gold prices of League of Legends items using Riot Data Dragon.
    Fixes the Live Client Data API issue where 'item.price' only reports combine/recipe cost
    instead of the full item value including components.
    """

    def __init__(self, cache_path: str = DEFAULT_CACHE_PATH, default_version: str = DEFAULT_DDRAGON_VERSION):
        self.cache_path = cache_path
        self.version = default_version
        self.prices: dict[int, int] = {}
        self.load_cache()

    def load_cache(self) -> bool:
        """Loads cached item prices from disk if available."""
        if not os.path.exists(self.cache_path):
            return False

        try:
            with open(self.cache_path, encoding="utf-8") as f:
                data = json.load(f)

            raw_prices = data.get("prices", {})
            self.prices = {int(k): int(v) for k, v in raw_prices.items() if str(k).isdigit()}
            self.version = data.get("version", self.version)
            logger.debug(f"Loaded {len(self.prices)} item prices (v{self.version}) from {self.cache_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to load item prices cache from {self.cache_path}: {e}")
            return False

    def save_cache(self) -> bool:
        """Persists the current item prices to disk cache."""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.cache_path)), exist_ok=True)
            payload = {
                "version": self.version,
                "total_items": len(self.prices),
                "prices": {str(k): v for k, v in self.prices.items()},
            }
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Saved {len(self.prices)} item prices to {self.cache_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to save item prices to {self.cache_path}: {e}")
            return False

    def get_item_price(self, item_id: int | None, fallback: int = 0) -> int:
        """
        Returns the total gold price of an item from Data Dragon.
        If item_id is missing, 0, or not found in Data Dragon, returns the fallback price
        (usually item.price from the Live Client API).
        """
        if not item_id:
            return fallback

        try:
            iid = int(item_id)
        except (ValueError, TypeError):
            return fallback

        total = self.prices.get(iid)
        # If total is known (even 0 for free wards/trinkets)
        if total is not None:
            # Special case: if total in Data Dragon is 0 but live client reports a price > 0, fallback
            if total == 0 and fallback > 0:
                return fallback
            return total

        return fallback

    async def update_from_ddragon(
        self, client: httpx.AsyncClient | None = None, target_version: str | None = None
    ) -> bool:
        """
        Asynchronously checks Data Dragon and refreshes item prices if a newer version is available.
        Does not block live telemetry polling.
        """
        should_close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)
            should_close_client = True

        try:
            # Determine target version if not provided
            if not target_version:
                try:
                    v_res = await client.get("https://ddragon.leagueoflegends.com/api/versions.json")
                    if v_res.status_code == 200:
                        versions_list = v_res.json()
                        if versions_list and isinstance(versions_list, list):
                            target_version = versions_list[0]
                except Exception as ve:
                    logger.debug(f"Could not fetch latest ddragon versions.json: {ve}")

            version_to_fetch = target_version or self.version or DEFAULT_DDRAGON_VERSION

            # If we already have prices for this version and plenty of items, skip download
            if version_to_fetch == self.version and len(self.prices) > 100:
                logger.debug(f"Item prices already up-to-date for version {version_to_fetch}")
                return True

            items_url = f"https://ddragon.leagueoflegends.com/cdn/{version_to_fetch}/data/en_US/item.json"
            res = await client.get(items_url)
            if res.status_code != 200:
                logger.warning(f"Data Dragon item.json returned status {res.status_code} for {items_url}")
                return False

            dd_data = res.json()
            raw_items = dd_data.get("data", {})
            updated_prices = {}

            for item_id_str, info in raw_items.items():
                try:
                    iid = int(item_id_str)
                    gold_obj = info.get("gold", {})
                    tot = gold_obj.get("total", 0)
                    updated_prices[iid] = tot
                except (ValueError, TypeError):
                    continue

            if updated_prices:
                self.prices = updated_prices
                self.version = version_to_fetch
                self.save_cache()
                logger.info(f"Successfully updated {len(self.prices)} item prices from Data Dragon {version_to_fetch}")
                return True

            return False
        except Exception as e:
            logger.warning(f"Failed to update item prices from Data Dragon: {e}")
            return False
        finally:
            if should_close_client:
                await client.aclose()


# Singleton instance
item_price_manager = ItemPriceManager()
