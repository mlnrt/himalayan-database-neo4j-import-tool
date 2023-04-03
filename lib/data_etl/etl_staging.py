import os
import sys
import json
import random
import hashlib
import numpy as np
import pandas as pd

from typing import Dict, List
from pathlib import Path
from datetime import datetime
from pydoc import locate
from dbfread.dbf import DBF

DATA_DIR = Path(__file__).parent.parent.parent / 'assets/data'
HD_DATA_DIR = DATA_DIR / 'hdb'
STAGED_DATA_DIR = DATA_DIR / 'staged'
dytpes_file = Path(__file__).with_name('hd_dtypes.json')
with dytpes_file.open('r') as f:
    hd_dytpes = json.load(f)


class GetDescriptions:
    def __init__(self):
        self.source_file = Path(__file__).parent / 'hd_descrips.json'
        self.json = json.load(open(self.source_file))

    def return_dict(self, dict_param) -> Dict[str, str]:
        for k_a, v_a in self.json.items():
            self.json[k_a] = {int(k): v for k, v in v_a.items()}
        return self.json[dict_param]


class HimalayanDatabaseEtl:
    def __init__(self, file_name: str, dtype: Dict[str, str]):
        """
        Base class for the ETL process of the Himalayan Database
        :param file_name: The name of the file to process
        :param dtype: The dictionary containing the column names and the data types
        """
        self.source_dir = HD_DATA_DIR
        self.target_dir = STAGED_DATA_DIR
        self.source_file = self.source_dir / file_name
        self.target_file = self.target_dir / f'{file_name.split(".")[0]}.csv'
        #self.dtype = np.dtype([(c, locate(t)) for c, t in dtype.items()])
        # Load the data into a dataframe from the DBF file
        dbf = DBF(self.source_file)
        self.df = pd.DataFrame(iter(dbf))
        # Convert all the types to the correct types (this can't be done in the DataFrame constructor)
        for c, t in dtype.items():
            self.df[c] = self.df[c].astype(locate(t))

    def save_data(self):
        """
        Save the processed data to the processed data directory
        """
        self.df.to_csv(self.target_file, index=False)

    def _fix_time_column(self, time_column: str):
        """
        Fix the time string in the dataframe. Add a semi-column between the hour and the minute and for the time with
        just the hour, add a zero minute. If the time has minutes > 59, set the minutes to 00. Finally, if the time
        is > 2359, discard the value.
        :param time_column: the name of the column containing the time
        """
        fixed_time = self.df[time_column].copy()
        # For non-empty strings, if the time is just the hour, add a zero minute
        fixed_time[(fixed_time.str.len() > 0) & (fixed_time.str.len() <= 2)] = fixed_time + '00'
        # If the time is of length 3 (e.g. 945) add a zero in front of the hour
        fixed_time[fixed_time.str.len() == 3] = '0' + fixed_time
        # In the time column fix all values which have the last 2 digits minutes > 59 and replace the last 2 digits
        # with 00
        fixed_time[fixed_time.str[-2:] > '59'] = fixed_time.str[:-2] + '00'
        # If the value is > '2359' replace it with an empty string
        fixed_time[fixed_time > '2359'] = ''
        # Add a semi-column between the hour and the minute
        fixed_time[fixed_time.str.len() > 0] = fixed_time.str[:-2] + ':' + fixed_time.str[-2:]
        # Add the Nepal time zone to the SMTTIME column
        fixed_time[fixed_time.str.len() > 0] = fixed_time + '+0545'
        self.df[time_column] = fixed_time

    def _replace_none_values(self, columns: List[str]):
        """
        Replace the 'None' string values in some columns with an empty string
        :param columns: the list of columns to process
        """
        for col in columns:
            self.df[col] = self.df[col].replace('None', '')


class ExpeditionsEtl(HimalayanDatabaseEtl):
    def __init__(self, file_name: str = 'exped.DBF'):
        """
        Class for the ETL process of the expeditions data
        :param file_name: The name of the file to process
        """
        super().__init__(file_name, hd_dytpes['EXPED_DTYPE'])

    def _discard_expeditions_without_members(self, members_df: pd.DataFrame):
        """
        Remove the expeditions which have no members in the members dataframe (this removes 16 expeditions).
        :param members_df: the members dataframe
        """
        self.df = self.df[self.df['EXPID'].isin(members_df['EXPID'].unique())]

    def _cleanup_expedition_route_names(self):
        """
        Some expeditions have route name with additional details in parenthesis (e.g. (to 6000m). This artificially
        creates new routes. We clean up the route names to try to reduce such cass.
        """
        # For all ROUTE1, ROUTE2, ROUTE3 and ROUTE4 columns,
        for col in ['ROUTE1', 'ROUTE2', 'ROUTE3', 'ROUTE4']:
            # Remove any mention of altitude in parenthesis
            self.df[col] = self.df[col].str.replace(r'\(to \d{4}m\)', '', regex=True)
            self.df[col] = self.df[col].str.replace(r'\(\d{4} high\)', '', regex=True)
            # Remove mention of acclimatization route in parenthesis
            self.df[col] = self.df[col].str.replace(r'\(acclimatization rte\)|for acclimatization', '', regex=True)
            # Remove mention of descent, we keep only the ascent route
            self.df[col] = self.df[col].str.replace(r'[,;/].*\(?down\)?', '', regex=True)
            # Remove mention of up at the end
            self.df[col] = self.df[col].str.replace(r'\s*up$', '', regex=True)
            # Normalize the word 'COl' to 'Col'
            self.df[col] = self.df[col].str.replace('COl', 'Col', regex=False)
            # Normalize the word 'Couloir' to 'couloir'
            self.df[col] = self.df[col].str.replace('Couloir', 'couloir', regex=False)
            # Normalize the word 'Couloirs' to 'couloirs'
            self.df[col] = self.df[col].str.replace('Couloirs', 'couloirs', regex=False)
            # Normalize the word 'Rte' to 'rte'
            self.df[col] = self.df[col].str.replace(r'\sRte\)', ' rte)', regex=True)
            # Fix mentions of the Tichy route on Cho-Oyu
            self.df[col] = self.df[col].str.replace(r'\(Tichy\)', '(Tichy rte)', regex=True)
            # Normalize the word 'Rib' to 'rib'
            self.df[col] = self.df[col].str.replace(r'Rib', 'rib', regex=True)
            # Keep parenthesis only if it ends with 'rte', 'couloir' 'rib' in it or is (new line), indicating a specific
            # route is detailed
            self.df[col] = self.df[col].str.replace(r'\s\((?!.*rte\)|.*couloir\)|.*couloirs\)|.*rib\)|new line).*\)',
                                                    '', regex=True)
            # Remove any via of from mention
            self.df[col] = self.df[col].str.replace(r'\s(via|from)\s.*', '', regex=True)
            # Remove trekking mention instead of routes
            self.df[col] = self.df[col].str.replace('(permit for trekking only)', '', regex=False)
            # Remove members mentions
            self.df[col] = self.df[col].str.replace(r'\sby\s\d.*members', '', regex=True)
            # Remove exclamation marks at the end of route names
            self.df[col] = self.df[col].str.replace(r'\?$', '', regex=True)
            # Fix route name connector
            self.df[col] = self.df[col].str.replace(r'\s?-\s?', '-', regex=True)
            # Standardize some specific route names
            self.df[col] = self.df[col].str.replace('S Sol-SE Ridge', 'S Col-SE Ridge', regex=False)
            self.df[col] = self.df[col].str.replace('Genava', 'Geneva', regex=False)
            self.df[col] = self.df[col].str.replace('(1980 German rte, left of the rib)', '(1980 German rte)',
                                                    regex=False)
            self.df[col] = self.df[col].str.replace('N Face (French 1950 rte)', 'N Face (French rte)', regex=False)
            self.df[col] = self.df[col].str.replace('SW Face (Bonington 1975 rte)', 'SW Face (Bonington rte)',
                                                    regex=False)
            # Remove any trailing spaces
            self.df[col] = self.df[col].str.strip()

    def _exped_descriptions(self):
        """
        Add descriptions to coded labels in dataframe
        """
        descrips = GetDescriptions()
        seas_dict = descrips.return_dict('SEAS_DESC')
        host_dict = descrips.return_dict('EXHOST_DESC')
        exterm_dict = descrips.return_dict('EXTERM_DESC')

        self.df['SEASON_DESC'] = self.df['SEASON'].apply(lambda x: seas_dict[x])
        self.df['HOST_DESC'] = self.df['HOST'].apply(lambda x: host_dict[x])
        self.df['TERMREASON_DESC'] = self.df['TERMREASON'].apply(lambda x: exterm_dict[x])

    def process(self, members_df: pd.DataFrame):
        """
        Process the expeditions data
        :param members_df: the members dataframe
        """
        self._fix_time_column('SMTTIME')
        self._replace_none_values(['COMRTE', 'STDRTE', 'PRIMREF', 'TERMDATE', 'ROUTEMEMO', 'BCDATE', 'SMTDATE'])
        self._discard_expeditions_without_members(members_df)
        self._cleanup_expedition_route_names()
        self._exped_descriptions()


class MembersEtl(HimalayanDatabaseEtl):
    def __init__(self, file_name: str = 'members.DBF'):
        """
        Class for the ETL process of the members data
        :param file_name: The name of the file to process
        """
        super().__init__(file_name, hd_dytpes['MEMBERS_DTYPE'])

    def _create_member_unique_id(self):
        """
        Create a unique ID for each member and return the new updated DataFrame. Members are uniquely identified by
        their first name, last name, gender and birth year.
        """
        id_df = pd.DataFrame(columns=['comb', 'PERSID'])
        check = 1
        # Set the random seed to a fixed value so that the same ID is generated for the same member
        random.seed(0)
        # For Sherpas only we are also going to use their address to uniquely identify them
        # To do so we create a temporary column which includes the residence column only for Sherpas
        self.df['TEMP_RESIDENCE'] = self.df.apply(lambda x: x['RESIDENCE'] if x['SHERPA'] else '', axis=1)
        # We strip all spaces and special characters from the TEMP_RESIDENCE to avoid differences due to different
        # inputs. We also lowercase the values
        self.df['TEMP_RESIDENCE'] = self.df['TEMP_RESIDENCE'].str.replace('[^a-zA-Z0-9]', '', regex=True).str.lower()
        # Fill the NaNs in YOB and TEMP_RESIDENCE values with an empty string
        self.df['TEMP_RESIDENCE'].fillna('', inplace=True)
        self.df['YOB'].fillna('', inplace=True)
        while check == 1:
            self.df['comb'] = self.df['FNAME'].astype(str) + self.df['LNAME'].astype(str) + self.df['SEX'].astype(str) \
                              + self.df['YOB'].astype(str) + self.df['TEMP_RESIDENCE'].astype(str)
            id_df['comb'] = self.df[['comb']].copy()
            id_df = id_df.drop_duplicates()
            id_df['PERSID'] = id_df['comb'].apply(lambda x: int(str(int(
                hashlib.sha1(str(x).encode('utf-8')).hexdigest(), 16) + (sys.maxsize*random.randint(1, 999999)))[:10]))
            # As we take a portion of the hash (the first 10 characters) to make the IDs more human readable, there
            # is a chance that the same ID is generated for two different members. If this happens, we rerun the
            # process (which will use a different random number) until all the IDs are unique
            if len(id_df[id_df['PERSID'].duplicated()].sort_values('PERSID')) == 0:
                check = 0
        self.df = pd.merge(self.df, id_df[['comb', 'PERSID']], how='left', left_on='comb', right_on='comb')
        self.df.drop(['comb', 'TEMP_RESIDENCE'], axis=1, inplace=True)

    def _discard_unnamed_members(self):
        """
        Remove all the members with a last name of Unknown and a first name as NaN or a first name equal to a number
        """
        # Remove all the members with a last name of Unknown and a first name as NaN or a first name equal to a number
        self.df = self.df[~((self.df['LNAME'] == 'Unknown') &
                            ((self.df['FNAME'].isna()) | (self.df['FNAME'].str.isnumeric())))]

    def _cleanup_countries(self):
        """
        Fix some known country name issues
        """
        self.df['CITIZEN'] = self.df['CITIZEN'].astype(str)
        # We fix misspellings of some countries
        self.df['CITIZEN'].replace('Malaysi', 'Malaysia', inplace=True)
        # Change 'W Germany' to 'Germany'
        self.df['CITIZEN'].replace('W Germany', 'Germany', inplace=True)
        # For Members who a number as CITIZEN column, we set the value to an empty string
        self.df['CITIZEN'] = self.df['CITIZEN'].apply(lambda x: '' if x.isnumeric() else x)
        # Some countries have a question mark at the end of their name. We remove it
        self.df['CITIZEN'] = self.df['CITIZEN'].str.replace('?', '', regex=False)
        # We remove any leading or trailing spaces
        self.df['CITIZEN'] = self.df['CITIZEN'].str.strip()

    def _memb_descriptions(self):
        """
        Add descriptions to coded labels in dataframe
        """
        descrips = GetDescriptions()
        deathT_dict = descrips.return_dict('MEMDEATHTYPE_DESC')
        deathC_dict = descrips.return_dict('MEMDEATHCLASS_DESC')
        inj_dict = descrips.return_dict('MEMINJ_DESC')
        summb_dict = descrips.return_dict('MEMSUMMBID_DESC')
        summterm_dict = descrips.return_dict('MEMSUMMBIDTERM_DESC')
        self.df['DEATHTYPE_DESC'] = self.df['DEATHTYPE'].apply(lambda x: deathT_dict[x])
        self.df['DEATHCLASS_DESC'] = self.df['DEATHCLASS'].apply(lambda x: deathC_dict[x])
        # Correcting issue on expedition PUMO96105, Tandler coded with wrong injury
        self.df.loc[self.df['LNAME'] == 'Tandler', 'INJURYTYPE'] = 1
        self.df['INJURYTYPE_DESC'] = self.df['INJURYTYPE'].apply(lambda x: inj_dict[x])
        self.df['MSMTBID_DESC'] = self.df['MSMTBID'].apply(lambda x: summb_dict[x])
        self.df['MSMTTERM_DESC'] = self.df['MSMTTERM'].apply(lambda x: summterm_dict[x])

    def _fix_some_members(self):
        """
        This function fixes some issues with the members data discovered during manual observation of the data
        """
        # The Members with Last Name "Tombazi" and first name "Nicolas Alexander" is registered elsewhere iw
        # first name "N. A.". We fix that to avoid duplicates
        na_yob = self.df.loc[(self.df['LNAME'] == 'Tombazi') & (self.df['FNAME'] == 'Nicolas Alexander'), 'YOB'].values[0]
        self.df.loc[(self.df['LNAME'] == 'Tombazi') & (self.df['FNAME'] == 'N. A.'), 'YOB'] = na_yob
        self.df.loc[(self.df['LNAME'] == 'Tombazi') & (self.df['FNAME'] == 'N. A.'), 'FNAME'] = 'Nicolas Alexander'

    def process(self):
        # Fix the time columns
        members_time_columns = ['MSMTTIME1', 'MSMTTIME2', 'MSMTTIME3', 'DEATHTIME', 'INJURYTIME']
        for column in members_time_columns:
            self._fix_time_column(column)
        self._replace_none_values(['BIRTHDATE', 'MSPEED', 'MSMTDATE1', 'MSMTDATE2', 'MSMTDATE3', 'DEATHDATE',
                                      'INJURYDATE', 'MEMBERMEMO', 'NECROLOGY'])
        # Remove all the members with a last name of Unknown and a first name as NaN or a first name equal to a number
        self._discard_unnamed_members()
        # Fix some issues with the members data discovered during manual observation of the data
        self._fix_some_members()
        # Fix country names
        self._cleanup_countries()
        # Create a unique ID for each member
        self._create_member_unique_id()
        self._memb_descriptions()


class PeaksEtl(HimalayanDatabaseEtl):
    def __init__(self, file_name: str = 'peaks.DBF'):
        """
        Class for the ETL process of the peaks data
        :param file_name: The name of the file to process
        """
        super().__init__(file_name, hd_dytpes['PEAKS_DTYPE'])

    def _fix_peaks_dates(self, expeditions_df: pd.DataFrame):
        """
        Convert the PSMTDATE date column in the peaks dataframe from dd/mm format to ISO format using the year value
        from the PYEAR column
        :param expeditions_df: The expeditions dataframe
        """
        # Peak SPH2 has a PYEAR set to "201". The first ascent expedition is SPH218301 which was done in 2018. So we
        # set the PYEAR to 2018
        self.df.loc[self.df['PEAKID'] == 'SPH2', 'PYEAR'] = '2018'
        # All peaks PHUK, KYR1, CHOP, PARC, PIMU, RAMD, RAMT, LING, PANT have a broken PSMTDATE first ascent summit date
        # column value. But they have a proper summit value in the expedition table. We create a list of these peaks,
        # temporarily set the value to NaN and we will later copy the value from the expedition table
        peaks_with_broken_psmtdate = ['PHUK', 'KYR1', 'CHOP', 'PARC', 'PIMU', 'RAMD', 'RAMT', 'LING', 'PANT']
        self.df.loc[self.df['PEAKID'].isin(peaks_with_broken_psmtdate), 'PSMTDATE'] = np.nan
        # Peaks DHAM, GANC, GHYM, MERA, SPHN, CHRI, TKPO, YAUP, DUDH, NILE have a PSMTDATE set to just the month name
        # (e.g. 'May'). In the expedition table, these expeditions do not have a SMTDATE. So we set the PSMTDATE to NaN
        # for all these peaks. These will stay as NaN
        self.df.loc[self.df['PEAKID'].isin(['DHAM', 'GANC', 'GHYM', 'MERA', 'SPHN', 'CHRI', 'TKPO', 'YAUP', 'DUDH',
                                            'NILE']), 'PSMTDATE'] = np.nan
        # If the PSMTDATE is not NaN, convert the date column from "%b %d" format to ISO format using the year value
        # from the PYEAR column
        self.df['PSMTDATE'] = self.df.apply(lambda x:
                                  datetime.strptime(f'{x["PSMTDATE"]} {x["PYEAR"]}', '%b %d %Y').strftime('%Y-%m-%d') if
                                  not (pd.isna(x['PSMTDATE']) or x['PSMTDATE'] == '') else x['PSMTDATE'], axis=1)
        # For all the peaks in the peaks_with_broken_psmtdate list, copy the value from the SMTDATE in the expedition
        # table
        for peak in peaks_with_broken_psmtdate:
            peak_exped_id = self.df[self.df['PEAKID'] == peak]['PEXPID'].values[0]
            peak_exped_smtdate = expeditions_df[expeditions_df['EXPID'] == peak_exped_id]['SMTDATE'].values[0]
            self.df.loc[self.df['PEAKID'] == peak, 'PSMTDATE'] = peak_exped_smtdate

    def _peak_descriptions(self):
        """
        Add descriptions to coded labels in dataframe
        """
        descrips = GetDescriptions()
        host_dict = descrips.return_dict('PHOST_DESC')
        self.df['PHOST_DESC'] = self.df['PHOST'].apply(lambda x: host_dict[x])

    def _set_climbed_status_to_boolean(self):
        """
        Replace the status column values with boolean values
        """
        status_dict = {
            0: None,
            1: False,
            2: True
        }
        self.df['PCLIMBED'] = self.df['PSTATUS'].apply(lambda x: status_dict[x])

    def process(self, expeditions_df: pd.DataFrame):
        """
        Process the peaks data
        :param expeditions_df: The expeditions dataframe
        """
        self._fix_peaks_dates(expeditions_df)
        self._replace_none_values(['PEAKMEMO', 'REFERMEMO', 'PHOTOMEMO'])
        self._set_climbed_status_to_boolean()
        self._peak_descriptions()


if __name__ == '__main__':
    # Check if the ETL_DATA_DIR exist. If it does not, create it
    os.makedirs(STAGED_DATA_DIR, exist_ok=True)
    # Load all the data
    peaks = PeaksEtl()
    expeditions = ExpeditionsEtl()
    members = MembersEtl()
    # Process the Peaks data
    peaks.process(expeditions.df)
    # Process the Members data
    members.process()
    # Process the Expeditions data
    expeditions.process(members.df)
    # Save all the dataframes to csv files
    members.save_data()
    expeditions.save_data()
    peaks.save_data()
