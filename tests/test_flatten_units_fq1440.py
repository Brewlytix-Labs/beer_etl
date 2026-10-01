"""FQ-1440: the BeerXML import stores BeerXML 1.0's own units.

BeerXML 1.0 gives batch and boil sizes in litres, fermentable and hop amounts in kilograms, and a misc's or a yeast's
amount in kilograms when AMOUNT_IS_WEIGHT is TRUE and in litres otherwise. The import used to guess instead: a batch
over 10 was gallons (20.82 L became 78.81 L), a fermentable over 0.1 was pounds, a hop or misc over 0.01 was ounces, a
smaller amount was stored as kilograms under a grams label, and every yeast whose TYPE was not one of five exact words
became Ale. Each case below is a value COWORK found in the Recipe Bank (UAT row RC03.1), read the right way.

Runs with the standard library's unittest (and under pytest). flatten.py imports pytz and bson for timestamps and ids
only; when they are not installed, they are stubbed, because no unit depends on them.
"""
import datetime
import importlib.util
import os
import sys
import types
import unittest
import xml.etree.ElementTree as ET

for _mod in ("pytz", "bson"):
    try:
        __import__(_mod)
    except ImportError:
        _stub = types.ModuleType(_mod)
        if _mod == "bson":
            _stub.ObjectId = type("ObjectId", (str,), {})
        else:
            _stub.timezone = lambda name: datetime.timezone.utc
            _stub.UTC = datetime.timezone.utc
        sys.modules[_mod] = _stub

_HERE = os.path.dirname(os.path.abspath(__file__))
_FLATTEN = os.environ.get("FQ1440_FLATTEN") or os.path.join(_HERE, "..", "include", "etl", "flatten.py")
_spec = importlib.util.spec_from_file_location("fq1440_flatten", _FLATTEN)
flatten = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(flatten)

RECIPE = """<?xml version="1.0" encoding="UTF-8"?>
<RECIPES>
  <RECIPE>
    <NAME>FQ-1440 Corona clone</NAME><VERSION>1</VERSION><TYPE>All Grain</TYPE><BREWER>FORGE</BREWER>
    <BATCH_SIZE>25.0</BATCH_SIZE><BOIL_SIZE>30.0</BOIL_SIZE><BOIL_TIME>60</BOIL_TIME><EFFICIENCY>72</EFFICIENCY>
    <FERMENTABLES>
      <FERMENTABLE><NAME>Wheat Malt Extract</NAME><VERSION>1</VERSION><TYPE>Extract</TYPE><AMOUNT>2.7216</AMOUNT>
        <YIELD>78</YIELD><COLOR>4</COLOR></FERMENTABLE>
      <FERMENTABLE><NAME>Black Malt</NAME><VERSION>1</VERSION><TYPE>Grain</TYPE><AMOUNT>0.03</AMOUNT>
        <YIELD>55</YIELD><COLOR>500</COLOR></FERMENTABLE>
    </FERMENTABLES>
    <HOPS>
      <HOP><NAME>Hallertau Hersbrucker</NAME><VERSION>1</VERSION><ALPHA>4.0</ALPHA><AMOUNT>0.0283495</AMOUNT>
        <USE>Boil</USE><TIME>60</TIME><FORM>Pellet</FORM></HOP>
      <HOP><NAME>Magnum</NAME><VERSION>1</VERSION><ALPHA>12.0</ALPHA><AMOUNT>0.00001</AMOUNT>
        <USE>Boil</USE><TIME>60</TIME><FORM>Pellet</FORM></HOP>
    </HOPS>
    <YEASTS>
      <YEAST><NAME>Bavarian Lager M76</NAME><VERSION>1</VERSION><TYPE>lager</TYPE><FORM>Dry</FORM>
        <AMOUNT>0.011</AMOUNT><AMOUNT_IS_WEIGHT>TRUE</AMOUNT_IS_WEIGHT></YEAST>
      <YEAST><NAME>Bohemian Lager 2124</NAME><VERSION>1</VERSION><FORM>Liquid</FORM><AMOUNT>0.125</AMOUNT></YEAST>
      <YEAST><NAME>US-05</NAME><VERSION>1</VERSION><TYPE>Ale</TYPE><FORM>Dry</FORM><AMOUNT>0.0115</AMOUNT>
        <AMOUNT_IS_WEIGHT>true</AMOUNT_IS_WEIGHT></YEAST>
      <YEAST><NAME>House strain</NAME><VERSION>1</VERSION><TYPE>Bottom</TYPE><FORM>Liquid</FORM></YEAST>
    </YEASTS>
    <MISCS>
      <MISC><NAME>Irish Moss</NAME><VERSION>1</VERSION><TYPE>Fining</TYPE><USE>Boil</USE><TIME>15</TIME>
        <AMOUNT>0.005</AMOUNT><AMOUNT_IS_WEIGHT>TRUE</AMOUNT_IS_WEIGHT></MISC>
      <MISC><NAME>Lactic Acid</NAME><VERSION>1</VERSION><TYPE>Water Agent</TYPE><USE>Mash</USE><TIME>0</TIME>
        <AMOUNT>0.002</AMOUNT></MISC>
    </MISCS>
  </RECIPE>
</RECIPES>
"""


class FlattenUnitsFQ1440(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        recipes = flatten.flatten_beerxml_to_json(RECIPE, "fq1440/test.xml")
        assert len(recipes) == 1, recipes
        cls.r = recipes[0]

    def test_batch_and_boil_sizes_are_litres(self):
        self.assertAlmostEqual(self.r["batch"]["target_volume_L"], 25.0)
        self.assertAlmostEqual(self.r["batch"]["boil_volume_L"], 30.0)

    def test_fermentables_are_kilograms(self):
        by = {f["name"]: f for f in self.r["fermentables"]}
        self.assertAlmostEqual(by["Wheat Malt Extract"]["amount_g"], 2721.6)
        self.assertAlmostEqual(by["Wheat Malt Extract"]["original"]["amount_kg"], 2.7216)
        self.assertAlmostEqual(by["Black Malt"]["amount_g"], 30.0)  # 0.03 kg, once stored as "0.03 g"

    def test_hops_are_kilograms(self):
        by = {h["name"]: h for h in self.r["hops"]}
        self.assertAlmostEqual(by["Hallertau Hersbrucker"]["amount_g"], 28.3495)  # 1 oz, once "0.80 g"
        self.assertAlmostEqual(by["Magnum"]["amount_g"], 0.01)

    def test_yeast_type_and_amount(self):
        by = {y["name"]: y for y in self.r["yeasts"]}
        self.assertEqual(by["Bavarian Lager M76"]["type"], "Lager")  # TYPE in another case
        self.assertEqual(by["Bohemian Lager 2124"]["type"], "Lager")  # no TYPE: read from its name
        self.assertEqual(by["US-05"]["type"], "Ale")
        self.assertEqual(by["House strain"]["type"], "Ale")  # no word to read: the schema's fallback
        self.assertAlmostEqual(by["Bavarian Lager M76"]["amount_kg"], 0.011)
        self.assertIsNone(by["Bavarian Lager M76"]["amount_l"])
        self.assertAlmostEqual(by["Bohemian Lager 2124"]["amount_l"], 0.125)
        self.assertIsNone(by["Bohemian Lager 2124"]["amount_kg"])
        self.assertAlmostEqual(by["US-05"]["amount_kg"], 0.0115)  # AMOUNT_IS_WEIGHT in lower case
        for y in by.values():
            self.assertIsNone(y["amount_cells_billion"])  # BeerXML gives no cell count

    def test_miscs_by_weight_or_volume(self):
        by = {m["name"]: m for m in self.r["miscs"]}
        self.assertAlmostEqual(by["Irish Moss"]["amount_g"], 5.0)
        self.assertIsNone(by["Irish Moss"]["amount_ml"])
        self.assertAlmostEqual(by["Lactic Acid"]["amount_ml"], 2.0)
        self.assertAlmostEqual(by["Lactic Acid"]["amount_g"], 2.0)  # one millilitre read as one gram


class YeastEdgesFQ1440(unittest.TestCase):
    """Round 2 (VERITY's notes): a namespaced BeerXML 0.9 document, as the import reads it."""

    NS = {'beerxml': 'http://www.beerxml.com/beerxml_0.9'}

    def yeasts(self, body):
        recipe = ET.fromstring('<RECIPE xmlns="http://www.beerxml.com/beerxml_0.9"><YEASTS>%s</YEASTS></RECIPE>' % body)
        return {y['name']: y for y in flatten.extract_yeasts(recipe, self.NS)}

    def test_a_barleywine_yeast_is_an_ale(self):
        by = self.yeasts('<YEAST><NAME>Barleywine Ale Yeast</NAME><VERSION>1</VERSION><FORM>Liquid</FORM>'
                         '<AMOUNT>0.1</AMOUNT></YEAST>')
        self.assertEqual(by['Barleywine Ale Yeast']['type'], 'Ale')  # 'wine' is inside 'barleywine'

    def test_a_zero_amount_stays_zero(self):
        by = self.yeasts('<YEAST><NAME>Zero Lager</NAME><VERSION>1</VERSION><TYPE>Lager</TYPE><FORM>Dry</FORM>'
                         '<AMOUNT>0</AMOUNT><AMOUNT_IS_WEIGHT>TRUE</AMOUNT_IS_WEIGHT></YEAST>')
        self.assertEqual(by['Zero Lager']['amount_kg'], 0.0)  # not None: a real 0 is a value


class NamespacedZerosFQ1440(unittest.TestCase):
    """Round 3 (VERITY's follow-up): on a namespaced BeerXML 0.9 document, a real 0 is a value."""

    @classmethod
    def setUpClass(cls):
        xml = ('<RECIPES xmlns="http://www.beerxml.com/beerxml_0.9"><RECIPE>'
               '<NAME>FQ-1440 Zeros</NAME><VERSION>1</VERSION><TYPE>All Grain</TYPE><BREWER>FORGE</BREWER>'
               '<BATCH_SIZE>20.0</BATCH_SIZE><BOIL_SIZE>24.0</BOIL_SIZE><BOIL_TIME>0</BOIL_TIME><EFFICIENCY>70</EFFICIENCY>'
               '<IBU>0</IBU>'
               '<FERMENTABLES><FERMENTABLE><NAME>Table Sugar</NAME><VERSION>1</VERSION><TYPE>Sugar</TYPE>'
               '<AMOUNT>0.5</AMOUNT><YIELD>100</YIELD><COLOR>0</COLOR></FERMENTABLE></FERMENTABLES>'
               '<YEASTS><YEAST><NAME>US-05</NAME><VERSION>1</VERSION><TYPE>Ale</TYPE><FORM>Dry</FORM>'
               '<AMOUNT>0.0115</AMOUNT><AMOUNT_IS_WEIGHT>TRUE</AMOUNT_IS_WEIGHT></YEAST></YEASTS>'
               '</RECIPE></RECIPES>')
        recipes = flatten.flatten_beerxml_to_json(xml, 'fq1440/zeros.xml')
        assert len(recipes) == 1, recipes
        cls.r = recipes[0]

    def test_a_zero_boil_time_stays_zero(self):
        self.assertEqual(self.r['batch']['boil_time_min'], 0)  # was 60, the plain tag's default

    def test_a_sugars_zero_color_stays_zero(self):
        self.assertEqual(self.r['fermentables'][0]['color_srm'], 0.0)  # was None

    def test_a_zero_ibu_stays_zero(self):
        self.assertEqual(self.r['estimates']['ibu']['value'], 0.0)  # was None


if __name__ == "__main__":
    unittest.main()
