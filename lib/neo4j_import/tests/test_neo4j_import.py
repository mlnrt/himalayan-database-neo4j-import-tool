# Reference:
# This code is based on the code from the following articles:
# Alexey Smirnov (2020, Sept 6). Testing database with pytest. https://smirnov-am.github.io/pytest-testing_database/
# Alexey Smirnov (2020, Sept 6). Advanced fixtures with pytest. https://smirnov-am.github.io/pytest-advanced-fixtures/
import pytest

from pathlib import Path
from typing import List
from neo4j import Result

from lib.neo4j_import.neo4j_import import HimalayasDatabaseImport


# Some expeditions have been creating issues during the import, we list some of them here to test that they are imported
# correctly
EXTRA_TEST_EXPEDITIONS = ['EVER93102', 'CHOY09331', 'URKM71102', 'MAK279301', 'EVER52301', 'EVER19134', 'MANA72101',
                          'KANG10101', 'HUNK12301', 'CHUR90301', 'CHRI99201', 'CHRW83301', 'DOMO12301', 'DROM98101',
                          'GAN383301', 'GAN483301', 'SISN13101', 'NUMB83301', 'LIK107301', 'LOBE91101', 'FIRN87301',
                          'CHAN98301', 'RAKS97301', 'EVER19101', 'EVER19106', 'EVER52101']
test_path = Path(__file__)


# Class to tun Cypher test queries on the cached test database
class CacheService:
    def __init__(self, test_db):
        self.test_db = test_db

    def run_test_query(self, query_path: Path, query_file: str) -> List[Result]:
        """Run a test query on the cached test database
        :param query_path: the path to the test query file
        :param query_file: the name of the file containing the query to run
        :return: the result of the query
        """
        # Open the Cypher query to test the full mesh subgraph of the members of an expedition
        try:
            with query_path.with_name(query_file).open('r') as f:
                test_members_mesh_query = f.read()
        except Exception as e:
            print(f'Error reading the Neo4j Cypher query file {query_file}', e)
            raise e
        # Run the query
        with self.test_db.driver.session(database=self.test_db.db_name) as session:
            res = session.run(test_members_mesh_query)
            return list(res)


# PyTest Fixture to create the test database and get the connection driver
@pytest.fixture(scope='session')
def neo4j_test_db() -> HimalayasDatabaseImport:
    """Create a test database and upload part of the data
    :return: the test database"""
    try:
        # Create a test database and upload part of the data
        test_db = HimalayasDatabaseImport(db_name='himalayastest', test_size=100,
                                          extra_test_expeditions=EXTRA_TEST_EXPEDITIONS)
        yield test_db
    finally:
        # Delete the test database and close the connection
        with test_db.driver.session(database='system') as session:
            session.run(f'DROP DATABASE {test_db.db_name}')
        test_db.close()


# PyTest Fixture to import the test data in the test database
@pytest.fixture(autouse=True, scope='session')
def setup_neo4j_test_db(neo4j_test_db):
    """Import the test data in the test database"""
    just_testing = True
    neo4j_test_db.import_expeditions_data(test=just_testing)
    neo4j_test_db.import_members_data(test=just_testing)
    neo4j_test_db.import_peaks_data(test=just_testing)


@pytest.fixture
def db_cache(neo4j_test_db) -> CacheService:
    return CacheService(neo4j_test_db)


def test_expeditions(db_cache):
    # Test there are expeditions without any member
    query_res = db_cache.run_test_query(test_path, 'test-expedition_members_count.cypher')
    nb_empty_expeditions = len(query_res)
    assert nb_empty_expeditions == 0, "All Expeditions should have at least 1 Member." \
        f" Found {nb_empty_expeditions} expeditions without a member."
    # Test if there are members with more than 2 relationships of the same type with the same expedition
    # There should be only one relationship of each type between a member and an expedition
    # E.g. a member can have a LED and WORKED_FOR relationship with an expedition but not two LED relationships
    query_res = db_cache.run_test_query(test_path, 'test-expedition_members_relations.cypher')
    nb_members = len(query_res)
    assert nb_members == 0, "All members of an expedition should have only one relationship of each type with the " \
                            f"expedition. Found {nb_members} members with incorrect relationships to their expedition."
    # Some expeditions pre commercial period which have climbed a route which will later become a commercial route
    # Must be assigned the NonCommercialExpedition label
    query_res = db_cache.run_test_query(test_path, 'test-non-commercial-expedition.cypher')
    nb_expeditions = len(query_res)
    assert nb_expeditions == 0, "Even if the COMRTE column is set to True in the Himalayan Database, an expedition pre " \
                                "commercial period must be labeled as a NonCommercialExpedition" \


def test_members_full_mesh(db_cache):
    query_res = db_cache.run_test_query(test_path, 'test-members-mesh.cypher')
    nb_non_full_meshed_expeditions = len(query_res)
    assert nb_non_full_meshed_expeditions == 0, "All Expeditions' Members subgraph should be full meshed subgraph." \
        f" Found {nb_non_full_meshed_expeditions} non full meshed expedition(s)."

def test_peaks(db_cache):
    # Test there are no peaks with no name
    query_res = db_cache.run_test_query(test_path, 'test-peak-names.cypher')
    nb_peaks = len(query_res)
    assert nb_peaks == 0, f"There should be no peaks without a name property. Found {nb_peaks} without a name."
    # Test that there is no peak in more than one range
    query_res = db_cache.run_test_query(test_path, 'test-peak-ranges-count.cypher')
    nb_peaks = len(query_res)
    assert nb_peaks == 0, f"There should be no peak in more than 1 range. Found {nb_peaks} peaks in more than one range."
    # All peaks should be part of 1 range
    query_res = db_cache.run_test_query(test_path, 'test-peak-no-ranges-count.cypher')
    nb_peaks = len(query_res)
    assert nb_peaks == 0, f"There should be no peak not part of any range. Found {nb_peaks} peaks in no mountain range."

def test_countries(db_cache):
    # Test there are no countries with a "/" in their name
    query_res = db_cache.run_test_query(test_path, 'test-country-names.cypher')
    nb_countries = len(query_res)
    assert nb_countries == 0, "There should be no country with a '/' in their name. They should be split into" \
                              f" separate countries. Found {nb_countries} countries with a '/' in their name."


def test_districts(db_cache):
    # Test there are no countries with a "/" in their name
    query_res = db_cache.run_test_query(test_path, 'test-district-names.cypher')
    nb_districts = len(query_res)
    assert nb_districts == 0, "There should be no district with a '/' in their name. They should be split into" \
                              f" separate districts. Found {nb_districts} districts with a '/' in their name."
    # Test that there is no "NC" (China) or "NI" (India) districts.
    query_res = db_cache.run_test_query(test_path, 'test-nc-ni-district.cypher')
    nb_districts = len(query_res)
    assert nb_districts == 0, f"There should be no 'NC' (China) or 'NI' (India) districts. Found {nb_districts}" \
                              " districts set as either China or India."
    # Test that there is no district in more than one province
    query_res = db_cache.run_test_query(test_path, 'test-district-provinces-count.cypher')
    nb_districts = len(query_res)
    assert nb_districts == 0, f"There should be no district in more than 1 province. Found {nb_districts}" \
                              " districts in more than one province."
    # All districts should be part of 1 province
    query_res = db_cache.run_test_query(test_path, 'test-district-no-province-count.cypher')
    nb_districts = len(query_res)
    assert nb_districts == 0, f"There should be no district not part of any province. Found {nb_districts}" \
                              " districts in no province."
