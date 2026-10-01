"""FQ-1440: the BeerXML import stores BeerXML 1.0's own units.

BeerXML 1.0 gives batch and boil sizes in litres, fermentable and hop amounts in kilograms, and a misc's or a yeast's
amount in kilograms when AMOUNT_IS_WEIGHT is TRUE and in litres otherwise. The import used to guess instead: a batch
over 10 was gallons (20.82 L became 78.81 L), a fermentable over 0.1 was pounds, a hop or misc over 0.01 was ounces, a
smaller amount was stored as kilograms under a grams label, and every yeast whose TYPE was not one of five exact words
became Ale. Each case below is a value COWORK found in the Recipe Bank (UAT row RC03.1), read the right way.

Round 4: every value is written in a shape the Synth recipe content schema accepts, so the cutover can carry it.

Runs with the standard library's unittest (and under pytest). flatten.py imports pytz and bson for timestamps and ids
only; when they are not installed, they are stubbed, because no unit depends on them.
"""
import ast
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


# The keys the Synth recipe content schema allows on each ingredient and on batch (brewlytix-backend
# services/recipehash/schema/synth_recipe_content_v1.schema.json, read at epic-build 8db68950). Each of them there is
# additionalProperties: false, and the cutover's walker refuses an unknown key (UNKNOWN_KEY).
SYNTH_KEYS = {
    "fermentables": {"_ref", "amount", "amount_g", "color_srm", "diastatic_power", "diastatic_power_Lintner",
                     "late_addition", "name", "notes", "origin", "original", "type", "yields_potential_sg"},
    "hops": {"_ref", "alpha_acid", "alpha_acid_pct", "amount", "amount_g", "form", "name", "notes", "origin",
             "stage", "time", "time_min", "use"},
    "yeasts": {"_ref", "amount", "amount_cells_billion", "attenuation", "attenuation_pct", "form", "max_temp",
               "max_temp_C", "min_temp", "min_temp_C", "name", "notes", "type"},
    "miscs": {"amount", "amount_g", "name", "notes", "stage", "time", "time_min", "type", "use"},
    "batch": {"boil_time", "boil_time_min", "boil_volume", "boil_volume_L", "efficiency", "efficiency_pct",
              "target_volume", "target_volume_L"},
}
# The units the schema's typed amount allows for a yeast's mass or volume.
AMOUNT_UNITS = {"g", "kg", "oz", "lb", "mL", "L", "tsp", "tbsp", "fl oz", "cup"}


def load_sanitizer():
    """The DAG's sanitize step, which runs between the import and Mongo. The DAG imports Airflow, so only this function
    and its two helpers are read out of it."""
    path = os.environ.get("FQ1440_DAG") or os.path.join(_HERE, "..", "dags", "beerxml_etl_dag.py")
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    wanted = [n for n in tree.body if isinstance(n, ast.FunctionDef)
              and n.name in ("_to_float", "_to_int", "sanitize_recipes_for_mongo")]
    assert len(wanted) == 3, [n.name for n in wanted]
    # Its log lines are not under test, and they carry emoji a cp1252 console cannot print.
    namespace = {"datetime": datetime.datetime, "print": lambda *args, **kwargs: None}
    exec(compile(ast.Module(body=wanted, type_ignores=[]), path, "exec"), namespace)
    return namespace["sanitize_recipes_for_mongo"]


class SynthContentShapeFQ1440(unittest.TestCase):
    """Round 4: what is stored is a shape the Synth recipe content schema accepts, so the cutover can carry it."""

    ZEROS = ('<RECIPES xmlns="http://www.beerxml.com/beerxml_0.9"><RECIPE>'
             '<NAME>FQ-1440 Zeros</NAME><VERSION>1</VERSION><TYPE>All Grain</TYPE><BREWER>FORGE</BREWER>'
             '<BATCH_SIZE>20.0</BATCH_SIZE><BOIL_SIZE>24.0</BOIL_SIZE><BOIL_TIME>0</BOIL_TIME><EFFICIENCY>70</EFFICIENCY>'
             '<FERMENTABLES><FERMENTABLE><NAME>Table Sugar</NAME><VERSION>1</VERSION><TYPE>Sugar</TYPE>'
             '<AMOUNT>0.5</AMOUNT><YIELD>100</YIELD><COLOR>0</COLOR></FERMENTABLE></FERMENTABLES>'
             '<YEASTS><YEAST><NAME>US-05</NAME><VERSION>1</VERSION><TYPE>Ale</TYPE><FORM>Dry</FORM>'
             '<AMOUNT>0.0115</AMOUNT><AMOUNT_IS_WEIGHT>TRUE</AMOUNT_IS_WEIGHT></YEAST></YEASTS>'
             '</RECIPE></RECIPES>')

    @classmethod
    def setUpClass(cls):
        cls.imported = (flatten.flatten_beerxml_to_json(RECIPE, 'fq1440/test.xml')
                        + flatten.flatten_beerxml_to_json(cls.ZEROS, 'fq1440/zeros.xml'))
        assert len(cls.imported) == 2, cls.imported
        # The sanitizer edits the documents it is given, so it gets its own import of the two.
        cls.stored = load_sanitizer()(flatten.flatten_beerxml_to_json(RECIPE, 'fq1440/test.xml')
                                      + flatten.flatten_beerxml_to_json(cls.ZEROS, 'fq1440/zeros.xml'))
        assert len(cls.stored) == 2, cls.stored

    def check_keys(self, recipes):
        for r in recipes:
            self.assertEqual(set(r['batch']) - SYNTH_KEYS['batch'], set(), r['name'])
            for part in ('fermentables', 'hops', 'yeasts', 'miscs'):
                for item in r.get(part) or []:
                    self.assertEqual(set(item) - SYNTH_KEYS[part], set(), (r['name'], part, item['name']))

    def test_every_imported_key_is_one_the_schema_allows(self):
        self.check_keys(self.imported)

    def test_every_stored_key_is_one_the_schema_allows(self):
        self.check_keys(self.stored)  # after the DAG's sanitize step, which is what Mongo holds

    def test_one_amount_on_each_fermentable_hop_and_misc(self):
        # The schema's oneOf: exactly one of amount and amount_g; with both, two sources project to one key.
        for r in self.stored:
            for part in ('fermentables', 'hops', 'miscs'):
                for item in r.get(part) or []:
                    self.assertEqual(len({'amount', 'amount_g'} & set(item)), 1, (part, item['name']))

    def test_a_yeast_amount_is_typed_and_no_cell_count_is_stored(self):
        for r in self.stored:
            for y in r['yeasts']:
                self.assertNotIn('amount_cells_billion', y)  # the sanitizer used to add it as null
                if 'amount' in y:
                    self.assertEqual(set(y['amount']), {'value', 'unit'})
                    self.assertIn(y['amount']['unit'], AMOUNT_UNITS)
                    self.assertIsInstance(y['amount']['value'], float)
        by = {y['name']: y for y in self.stored[0]['yeasts']}
        self.assertEqual(by['Bohemian Lager 2124']['amount'], {'value': 0.125, 'unit': 'L'})


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
        self.assertNotIn("original", by["Wheat Malt Extract"])  # its one schema slot is pounds
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
        self.assertEqual(by["Bavarian Lager M76"]["amount"], {"value": 0.011, "unit": "kg"})
        self.assertEqual(by["Bohemian Lager 2124"]["amount"], {"value": 0.125, "unit": "L"})
        self.assertEqual(by["US-05"]["amount"], {"value": 0.0115, "unit": "kg"})  # AMOUNT_IS_WEIGHT in lower case
        self.assertNotIn("amount", by["House strain"])  # no AMOUNT, so no amount
        for y in by.values():
            self.assertNotIn("amount_cells_billion", y)  # BeerXML gives no cell count

    def test_miscs_by_weight_or_volume(self):
        by = {m["name"]: m for m in self.r["miscs"]}
        self.assertAlmostEqual(by["Irish Moss"]["amount_g"], 5.0)
        self.assertAlmostEqual(by["Lactic Acid"]["amount_g"], 2.0)  # 2 mL, one millilitre read as one gram


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
        self.assertEqual(by['Zero Lager']['amount'], {'value': 0.0, 'unit': 'kg'})  # kept: a real 0 is a value


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
