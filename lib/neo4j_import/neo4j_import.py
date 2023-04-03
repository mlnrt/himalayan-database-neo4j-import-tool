import os
import time
import pandas as pd
import neo4j

from tqdm import tqdm
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv


load_dotenv()
JUST_TESTING = False
NEO4J_SERVER_URL = os.environ.get('NEO4J_SERVER_URL')
NEO4J_DATABASE_NAME = 'himalayandb' if os.environ.get('NEO4J_DATABASE_NAME') is None \
    else os.environ.get('NEO4J_DATABASE_NAME')
NEO4J_SERVER_USERNAME = os.environ.get('NEO4J_SERVER_USERNAME')
NEO4J_SERVER_PASSWORD = os.environ.get('NEO4J_SERVER_PASSWORD')


class HimalayasDatabaseImport:
    def __init__(self, db_name: str = NEO4J_DATABASE_NAME, expedition_file: str = 'processed/exped.csv',
                 members_file: str = 'processed/members.csv', peaks_file: str = 'processed/peaks.csv',
                 import_batch_size: int = 50, test_size: int = 100, extra_test_expeditions: List[str] = None):
        """
        Initialize the HimalayasDatabaseImport class to import the Himalayan Database data into a Neo4j graph database.
        :param db_name: The name of the Neo4j database.
        :param expedition_file: The path to the expedition file.
        :param members_file: The path to the members file.
        :param peaks_file: The path to the peaks file.
        :param import_batch_size: The number of records to import in a batch.
        :param test_size: The number of records to import in test mode.
        :param extra_test_expeditions: The list of extra expeditions to import in test mode.
        """
        try:
            self.driver = neo4j.GraphDatabase.driver(NEO4J_SERVER_URL, auth=(NEO4J_SERVER_USERNAME,
                                                                             NEO4J_SERVER_PASSWORD))
        except Exception as e:
            print('Error connecting to the Neo4j server', e)
            raise e
        self.db_name = db_name
        self.script_path = Path(__file__)
        self.data_path = self.script_path.parent.parent.parent / 'assets/data'
        self.import_files = {
            "expeditions": self.data_path / expedition_file,
            "members": self.data_path / members_file,
            "peaks": self.data_path / peaks_file
        }
        self.import_batch_size = import_batch_size
        self.test_size = test_size
        self.extra_test_expeditions = extra_test_expeditions
        self._create_database()

    def close(self):
        """Close the driver connection"""
        if self.driver is not None:
            self.driver.close()

    def _create_database(self):
        """
        Create a new database if it does not exist or replace the existing database if it already exists
        :return: None
        """
        with self.driver.session(database='system') as session:
            print(f'Creating or replacing the Neo4j database {self.db_name}')
            session.run(f'CREATE OR REPLACE DATABASE {self.db_name}')

    @staticmethod
    def _set_unkown_successful_routes(df: pd.DataFrame) -> pd.DataFrame:
        """
        There are some routes in the HD peaks data with a successful route 1 but no route name. We set the route name to
        "Unknown" for these peaks. Otherwise, it creates a problem when importing the data into the graph database as it
        will create disconnected nodes
        :param df: The Peaks dataframe.
        :return: Peaks dataframe with the ROUTE1 column set to "Unknown" if the SUCCESS1 column is True
        """
        corrected_df = df.copy()
        corrected_df.loc[(corrected_df['ROUTE1'].isna()) & corrected_df['SUCCESS1'], 'ROUTE1'] = 'Unknown'
        return corrected_df

    @staticmethod
    def _import_data_batch(tx: neo4j.Transaction, df: pd.DataFrame, query: str, table_name: str) \
            -> Tuple[int, int]:
        """
        Import a batch of data into the Neo4j database
        :param tx: the opened Neo4j transaction
        :param df: the Pandas DataFrame containing the batch of data to import
        :param query: the query to execute to import the data
        :param table_name: the name of the table in the Neo4j Cypher query
        :return: A tuple containing the number of nodes created and the number of relationships created
        """
        res = tx.run(query, parameters={table_name: df.to_dict('records')})
        nodes_created = res.consume().counters.nodes_created
        relationships_created = res.consume().counters.relationships_created
        return nodes_created, relationships_created

    def _import_data(self, table_name: str, df: pd.DataFrame, query: str, constraints: List[str] = None):
        """
        Import the data into the Neo4j database in batches
        :param table_name: The name of the table to import
        :param df: The Pandas DataFrame containing the data to import
        :param query: The Neo4j Cypher query to execute to import the data
        :param constraints: The list of constraints to add to the table
        :return: None
        """
        # Get the list of all the keys in the expedition_dtype dictionary which have a value set to the string type
        # Fill the columns with a string type NaN values to an empty string because the Neo4j doesn't
        # support NaN values for strings
        columns_to_convert_nan = [k for k, v in df.dtypes.items() if v == str or v == object]
        for column in columns_to_convert_nan:
            df[column].fillna('', inplace=True)
        with self.driver.session(database=self.db_name) as session:
            # You can't edit the schema and write data in the same transaction
            # First transaction to create unique constraints on the nodes
            if constraints:
                print(f'Creating the nodes unique constraints')
                for constraint in constraints:
                    session.run(constraint)
            # Second transactions to import the nodes
            total_nodes_created = 0
            total_relationships_created = 0
            # Split the expeditions DataFrame into batches of 100 rows and import each batch in a separate
            # transaction. Use tqdm to show the progress bar
            for i in tqdm(range(0, df.shape[0], self.import_batch_size), position=0, leave=True):
                batch_df = df.iloc[i:i + self.import_batch_size]
                nodes_created, relationships_created = session.execute_write(self._import_data_batch,  df=batch_df,
                                                                             query=query, table_name=table_name)
                total_nodes_created += nodes_created
                total_relationships_created += relationships_created
            print(f'Number of nodes created: {total_nodes_created}')
            print(f'Number of relationships created: {total_relationships_created}')
        # Add a 1 sec time for the TQDM progress bar to finish
        time.sleep(1)

    def import_expeditions_data(self, test: bool = False):
        """
        Import the Himalayan Database expedition data into the Neo4j database
        :param test: If True, only the first 100 rows of the expedition data file will be imported
        :return: None
        """
        exped_df = pd.read_csv(self.import_files['expeditions'], encoding='utf-8', engine='python')
        exped_df = self._set_unkown_successful_routes(exped_df)
        # If testing we take only the first self.test_size rows
        if test:
            if self.extra_test_expeditions:
                exped_df = pd.concat([exped_df.head(self.test_size),
                                      exped_df[exped_df['EXPID'].isin(self.extra_test_expeditions)]])
            else:
                exped_df = exped_df.head(self.test_size)
        try:
            with self.script_path.with_name('import-exped.cypher').open('r') as f:
                query = f.read()
        except Exception as e:
            print('Error reading the Neo4j Cypher query file import-exped.cypher', e)
            raise e
        constraints = ['CREATE CONSTRAINT IF NOT EXISTS FOR (e:Expedition) REQUIRE (e.expeditionId, e.year) IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (p:Peak) REQUIRE p.peakId IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (a:Agency) REQUIRE a.name IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (r:Route) REQUIRE r.name IS UNIQUE;']
        print(f'====> Importing the Himalayan Database expeditions data in the {self.db_name} database')
        print('==> Creating the Expeditions, Peaks, Agencies and Routes nodes and their relationships')
        self._import_data(table_name='expeditions', df=exped_df, query=query, constraints=constraints)

    def import_members_data(self, test: bool = False):
        """
        Import the Himalayan Database members data into the Neo4j database
        :param test: If True, only import the members who are in the expeditions imported in test mode
        :return: None
        """
        members_df = pd.read_csv(self.import_files['members'], encoding='utf-8', engine='python')
        # We sort the members by MYEAR and MSEASON so that the last members data (e.g. RESIDENCE) will be the one
        # remaining in the database for the node
        members_df.sort_values(by=['MYEAR', 'MSEASON'], inplace=True)
        # Get the unique expedition by ID and year
        exped_df = pd.read_csv(self.import_files['expeditions'], encoding='utf-8', engine='python')
        exped_df = exped_df[['EXPID', 'YEAR']].drop_duplicates()
        # If testing, only import the members who are in the self.test_size expeditions that have been imported
        if test:
            # Get the first self.test_size expedition IDs
            if self.extra_test_expeditions:
                exped_df = pd.concat([exped_df.head(self.test_size),
                                      exped_df[exped_df['EXPID'].isin(self.extra_test_expeditions)]])
            else:
                exped_df = exped_df.head(self.test_size)
            # Get the members corresponding to the imported expeditions
            members_df = members_df[members_df['EXPID'].isin(exped_df['EXPID'].tolist())]
        try:
            with self.script_path.with_name('import-members.cypher').open('r') as f:
                people_query = f.read()
        except Exception as e:
            print('Error reading the Neo4j Cypher query file import-members.cypher', e)
            raise e
        try:
            with self.script_path.with_name('import-memberships.cypher').open('r') as f:
                members_query = f.read()
        except Exception as e:
            print('Error reading the Neo4j Cypher query file import-memberships.cypher', e)
            raise e
        try:
            with self.script_path.with_name('generate-members-relations.cypher').open('r') as f:
                climb_together_query = f.read()
        except Exception as e:
            print('Error reading the Neo4j Cypher query file generate-members-relations.cypher', e)
            raise e
        constraints = ['CREATE CONSTRAINT IF NOT EXISTS FOR (m:Member) REQUIRE (m.personId) IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE;']
        # We will create the unique member nodes
        print(f'====> Importing the Himalayan Database members data in the {self.db_name} database')
        print('==> Creating the Member and Country nodes')
        members_columns = ['PERSID', 'FNAME', 'LNAME', 'SEX', 'YOB', 'CITIZEN', 'RESIDENCE', 'OCCUPATION', 'SHERPA',
                           'TIBETAN']
        people_df = members_df[members_columns].drop_duplicates(subset=['PERSID'], keep='last')
        self._import_data(table_name='members', df=people_df, query=people_query, constraints=constraints)
        print(f'==> Creating the members to expedition memberships')
        self._import_data(table_name='members', df=members_df, query=members_query)
        print(f'==> Creating relationships between the members of the same expedition')
        total_relationships_created = 0
        with self.driver.session(database=self.db_name) as session:
            # Process the expeditions in batches, and for each batch, crete the relationships between the members
            # of each expedition
            # Important: the query nust NOT be executed in batches. If executed in batches, the query might create
            # multiple relationships between two members, if they joined together multiple expeditions. The writes
            # won't be committed to the database until the end of the transaction So, when creating the relationships
            # between members of the second expeditions, it will find no relationship, while one is in the pipe
            # to be created between the two members who were in the first expedition
            for i in tqdm(range(0, len(exped_df)), position=0, leave=True):
                single_expedition = exped_df.iloc[i:i + 1]
                expedition_id = single_expedition['EXPID'].values[0]
                expedition_year = single_expedition['YEAR'].values[0]
                result = session.run(climb_together_query, parameters={"id": expedition_id, "year": expedition_year})
                total_relationships_created += result.consume().counters.relationships_created
            print(f'{total_relationships_created} climb together relationships created')

    def import_peaks_data(self, test: bool = False):
        """
        Import the Himalayan Database peaks data into the Neo4j database
        :param test: If True, only import the peaks which have been climbed by the expeditions imported in test mode
        :return: None
        """
        peaks_df = pd.read_csv(self.import_files['peaks'], encoding='utf-8', engine='python')
        # Get the expedition IDs
        exped_df = pd.read_csv(self.import_files['expeditions'], encoding='utf-8', engine='python')
        # If testing, only import the peaks which have been climbed by the self.test_size expeditions that have been
        # imported
        if test:
            # Get the first self.test_size expedition IDs
            if self.extra_test_expeditions:
                expeditions = exped_df['EXPID'].tolist()[:self.test_size] + self.extra_test_expeditions
            else:
                expeditions = exped_df['EXPID'].tolist()[:self.test_size]
            # Get the peaks ID corresponding to the test expeditions
            exped_df = exped_df[exped_df['EXPID'].isin(expeditions)]
            expeditions_peaks = exped_df['PEAKID'].tolist()
            # Get the members corresponding to the imported est expeditions
            peaks_df = peaks_df[peaks_df['PEAKID'].isin(expeditions_peaks)]
        try:
            with self.script_path.with_name('import-peaks.cypher').open('r') as f:
                query = f.read()
        except Exception as e:
            print('Error reading the Neo4j Cypher query file import-peaks.cypher', e)
            raise e
        constraints = ['CREATE CONSTRAINT IF NOT EXISTS FOR (r:Range) REQUIRE r.name IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (d:District) REQUIRE d.name IS UNIQUE;',
                       'CREATE CONSTRAINT IF NOT EXISTS FOR (p:Province) REQUIRE p.name IS UNIQUE;']
        print(f'====> Importing the Himalayan Database Peaks data in the {self.db_name} database')
        print('==> Creating the Peaks, Ranges and Regions nodes and relationships')
        self._import_data(table_name='peaks', df=peaks_df, query=query, constraints=constraints)


if __name__ == '__main__':
    if JUST_TESTING:
        print("""====> IMPORTANT: The JUST_TESTING flag is set to True.
                                  Only the amount of expeditions and the related data specified in the test_size 
                                  variable in the class definition will be imported.""")
    himalayas_db = HimalayasDatabaseImport()
    himalayas_db.import_expeditions_data(test=JUST_TESTING)
    himalayas_db.import_members_data(test=JUST_TESTING)
    himalayas_db.import_peaks_data(test=JUST_TESTING)
    himalayas_db.close()
