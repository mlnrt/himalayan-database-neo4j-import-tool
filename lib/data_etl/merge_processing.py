import os
import pandas as pd
import shutil

from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / 'assets/data'
NHPP_DATA_DIR = DATA_DIR / 'nhpp'
STAGED_DATA_DIR = DATA_DIR / 'staged'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'


def merge_peaks(nepal_peaks_df: pd.DataFrame, hd_peaks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the Himalayan Data Peaks data into the Nepal peaks data collected from the Nepal Himalayan Peak Profile and
    Peakvisor data scrapped from the web
    :param nepal_peaks_df: Nepal peaks dataframe
    :param hd_peaks_df: HD peaks dataframe
    :return: Merged peaks dataframe
    """
    # Create a "IS_HD_PEAK" column to identify the peaks that are originally in the HD peaks data
    hd_peaks_df['IS_HD_PEAK'] = True
    # left join the HD peaks data into the Nepal peaks data
    peaks_df = nepal_peaks_df.merge(hd_peaks_df, how='left', on='PEAKID')
    # We drop the NHPP dataset ID column since we don't need it anymore. We keep the HD database peak ID for consistency
    peaks_df.drop(columns=['ID'], inplace=True)
    # Fill the missing values from the PKANME, PKANME2 columns with the NAME and ALTERNATE_NAMES columns
    # We keep the HD database PKNAME and PKNAME2 columns for consistency and drop the columns NAME and ALTERNATE_NAMES
    peaks_df['PKNAME'] = peaks_df['PKNAME'].fillna(peaks_df['NAME'])
    peaks_df['PKNAME2'] = peaks_df['PKNAME2'].fillna(peaks_df['ALTERNATE_NAMES'])
    peaks_df.drop(columns=['NAME', 'ALTERNATE_NAMES'], inplace=True)
    # Fill the rows with no values in the IS_HD_PEAK column with False
    peaks_df['IS_HD_PEAK'] = peaks_df['IS_HD_PEAK'].fillna(False)
    # Fill the missing values in the HEIGHTM and HEIGHTF columns of the HD database with the values from the Nepal
    # database ELEVATION_M and ELEVATION_FT and discard these two columns
    peaks_df['HEIGHTM'] = peaks_df['HEIGHTM'].fillna(peaks_df['ELEVATION_M'])
    peaks_df['HEIGHTF'] = peaks_df['HEIGHTF'].fillna(peaks_df['ELEVATION_FT'])
    peaks_df.drop(columns=['ELEVATION_M', 'ELEVATION_FT'], inplace=True)
    # Fill the missing values from the OPEN column with True if the STATUS column=="Opened", with False if the value is
    # "Proposed to open" or "Closed", and with NaN otherwise and discard the STATUS column
    peaks_df['OPEN'] = peaks_df['OPEN'].fillna(
        peaks_df['STATUS'].apply(
            lambda x: True if x == 'Opened' else False if x in ['Proposed to open', 'Closed', 'Not open for expedition']
            else None))
    peaks_df['RESTRICT'] = peaks_df['RESTRICT'].fillna(peaks_df['STATUS'])
    peaks_df.drop(columns=['STATUS'], inplace=True)
    # We discard the FIRST_ASCENT_ON and FIRST_ASCENT_BY from the NHPP dataset as they are not reliable and the HD
    # dataset has more reliable data
    peaks_df.drop(columns=['FIRST_ASCENT_ON', 'FIRST_ASCENT_BY'], inplace=True)
    return peaks_df


if __name__ == '__main__':
    # Check if the ETL_DATA_DIR exist. If it does not, create it
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    # read in the Nepal peaks data
    nepal_peaks_df = pd.read_csv(NHPP_DATA_DIR / 'preprocessed_nhpp_peaks.csv')
    # read in the ETL processed HD peaks data
    hd_peaks_df = pd.read_csv(STAGED_DATA_DIR / 'peaks.csv')
    # merge the peaks data
    merged_peaks_df = merge_peaks(nepal_peaks_df, hd_peaks_df)
    # save the merged peaks data
    merged_peaks_df.to_csv(PROCESSED_DATA_DIR / 'peaks.csv', index=False)
    # There is no further processing or merging of data in this step, so we just copy the expeditions and members files
    # from the staged folder to the processed folder
    shutil.copyfile(STAGED_DATA_DIR / 'exped.csv', PROCESSED_DATA_DIR / 'exped.csv')
    shutil.copyfile(STAGED_DATA_DIR / 'members.csv', PROCESSED_DATA_DIR / 'members.csv')
