import json
import pandas as pd

from typing import List, Dict
from pathlib import Path
from dbfread.dbf import DBF

DATA_DIR = Path(__file__).parent.parent.parent / 'assets/data'
NHPP_DATA_DIR = DATA_DIR / 'nhpp'
HDB_DATA_DIR = DATA_DIR / 'hdb'
# Load the peaks_corrections.json file
with open(NHPP_DATA_DIR / 'peaks_corrections.json', 'r') as f:
    peaks_corrections = json.load(f)
NHPP_PEAKS_CORRECTIONS = peaks_corrections['NHPP_PEAKS_CORRECTIONS']
HD_PEAK_CORRECTIONS = peaks_corrections['HD_PEAK_CORRECTIONS']


def apply_corrections(df: pd.DataFrame, corrections_to_apply: List[Dict[str, str]], is_nhpp: bool) -> pd.DataFrame:
    """
    This function applies the corrections to the NHPP or Himalayan Database peaks DataFrames.
    :param df: The Himalayan Database peaks DataFrame
    :param corrections_to_apply: The list of corrections to apply
    :param is_nhpp: A boolean indicating if the DataFrame is the NHPP peaks DataFrame
    :return: The corrected Himalayan Database peaks DataFrame
    """
    corected_df = df.copy()
    # Search in the corrections_to_apply list of dictionaries for an entry with matching PEAKID
    # If there is a match, we need to convert the peak ID and overwrite any correction
    if is_nhpp:
        peak_key = 'NHPP_ID'
        df_key = 'ID'
    else:
        peak_key = 'PEAKID'
        df_key = 'PEAKID'
    for peak in corrections_to_apply:
        # For all key-values in the dictionary correct the values in the corected_df matching the PEAKID
        for key, value in peak.items():
            if key != peak_key and key != df_key:
                corected_df.loc[corected_df[df_key] == peak[peak_key], key] = value
        # Only once we corrected all the values, we can overwrite the PEAKID
        if df_key in peak.keys():
            corected_df.loc[corected_df[df_key] == peak[peak_key], df_key] = peak[df_key]
    return corected_df


def merge_nepal_peaks_datasets():
    """
    This function concatenates the peaks data collected from the NHPP and Peakvisor websites and manually collected data
    and adds the references from the Himalayan Database into result dataset.
    The function creates a new file called nhpp_peaks_with_hd_references.csv in the /data/all_nhpp_alt_names folder.
    :return: The merged DataFrame
    """
    # Read the data from the NHPP website into a Pandas DataFrame
    nhpp_peaks_df = pd.read_csv(NHPP_DATA_DIR / 'nhpp_peaks.csv')
    nhpp_peaks_df = apply_corrections(nhpp_peaks_df, NHPP_PEAKS_CORRECTIONS, is_nhpp=True)
    # Read the data from the Peakvisor website into a Pandas DataFrame
    peakvisor_peaks_df = pd.read_csv(NHPP_DATA_DIR / 'peakvisor_peaks.csv')  # For testing
    additional_peaks_df = pd.read_csv(NHPP_DATA_DIR / 'manually_collected_peaks.csv', sep=';', encoding='utf-8')
    nhpp_peaks_df = pd.concat([nhpp_peaks_df, peakvisor_peaks_df, additional_peaks_df])
    # Get the Himalayan Database peaks data into a Pandas DataFrame
    peaks_dbf = DBF(HDB_DATA_DIR / 'peaks.DBF')
    hd_peaks_df = pd.DataFrame(iter(peaks_dbf))
    hd_peaks_df = apply_corrections(hd_peaks_df, HD_PEAK_CORRECTIONS, is_nhpp=False)
    # In both datasets create a new column ALL_NAMES as a list containing the NAME and  the content of the alternate
    # name column transformed as a list split by comma and stripped from spaces
    nhpp_peaks_df['ALL_NAMES'] = nhpp_peaks_df[['NAME', 'ALTERNATE_NAMES']].fillna('').agg(','.join, axis=1)
    nhpp_peaks_df['ALL_NAMES'] = nhpp_peaks_df['ALL_NAMES'].apply(
        lambda x: [name.strip() for name in x.split(',') if name])
    hd_peaks_df['ALL_NAMES'] = hd_peaks_df[['PKNAME', 'PKNAME2']].fillna('').agg(','.join, axis=1)
    hd_peaks_df['ALL_NAMES'] = hd_peaks_df['ALL_NAMES'].apply(lambda x: [name.strip() for name in x.split(',') if name])
    # Explode both datasets so that each row contains only one peak name
    nhpp_peaks_df = nhpp_peaks_df.explode('ALL_NAMES')
    hd_peaks_df = hd_peaks_df.explode('ALL_NAMES')
    # Merge the datasets based on all their possible names
    nhpp_peaks_df = nhpp_peaks_df.merge(hd_peaks_df[['PEAKID', 'PKNAME', 'PKNAME2', 'ALL_NAMES']],
                                        how='left', on='ALL_NAMES')
    # Regroup the  peaks by ID keeping the first vlu o every column
    nhpp_peaks_df = nhpp_peaks_df.groupby('ID').first().reset_index()
    # Copy the ID into PEAKID when it is null
    nhpp_peaks_df['PEAKID'] = nhpp_peaks_df['PEAKID'].fillna(nhpp_peaks_df['ID'])
    # Drop the PKNAME, PKNAME2 and ALL_NAMES columns. We don't need them anymore
    nhpp_peaks_df.drop(['PKNAME', 'PKNAME2', 'ALL_NAMES'], axis=1, inplace=True)
    # Move the PEAKID column as the second column for easier comparison with the ID column
    cols = nhpp_peaks_df.columns.tolist()
    cols = cols[:1] + cols[-1:] + cols[1:-1]
    nhpp_peaks_df = nhpp_peaks_df[cols]
    # Sort the peaks by ID
    nhpp_peaks_df.sort_values('ID', inplace=True)
    # Cleanup the RANGE column to remove any " Himal" suffix
    nhpp_peaks_df['RANGE'] = nhpp_peaks_df['RANGE'].str.replace(' Himal', '', regex=False)
    # Save the dataset into a CSV file
    nhpp_peaks_df.to_csv(NHPP_DATA_DIR / 'preprocessed_nhpp_peaks.csv', index=False)


if __name__ == "__main__":
    print("Merging the peaks from the NHPP and Peakvisor websites and match them to the HD dataset ID for "
          f"easier downstrem join. The output is stored in the {NHPP_DATA_DIR / 'preprocessed_nhpp_peaks.csv'}"
          " file.""")
    merge_nepal_peaks_datasets()
