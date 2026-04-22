import unittest

from snotel import get_site_info, get_site_name, get_site_state
from snotel_sites import SITE_ID_TO_INFO


class SnotelSiteNameMapTest(unittest.TestCase):
    def test_site_info_mapping_is_populated(self):
        self.assertGreater(len(SITE_ID_TO_INFO), 0)

    def test_site_id_395_maps_to_name_and_state(self):
        self.assertEqual(SITE_ID_TO_INFO["395"]["name"], "Chemult Alternate")
        self.assertEqual(SITE_ID_TO_INFO["395"]["state"], "OR")
        self.assertEqual(get_site_info("395"), SITE_ID_TO_INFO["395"])
        self.assertEqual(get_site_name("395"), "Chemult Alternate")
        self.assertEqual(get_site_state("395"), "OR")

    def test_unknown_site_id_raises_key_error(self):
        with self.assertRaises(KeyError):
            get_site_info("999999")


if __name__ == "__main__":
    unittest.main()
