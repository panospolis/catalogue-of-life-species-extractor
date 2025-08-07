import csv
import os
import polars as pl
import requests
import sys
from termcolor import cprint
import time
from typing import Dict
import zipfile

"""
This script downloads the Catalogue of Life database, unzips it, and processes the NameUsage.tsv file to extract species information.
- NameUsage.tsv contains information about species, including their names, classifications, and other relevant data.
- VernacularName.tsv contains common names for species.

"""

def write_species_to_file(data: Dict, suffix: str = None):
    """
    Writes species data to a CSV file.
    :param data: Dictionary containing species data.
    :param suffix: A suffix for the file name (usually a rank ID or name).
    """
    filename = 'species.csv' if suffix is None else 'species_%s.csv' % suffix
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', filename)
    file_exists = os.path.exists(filepath)
    with open(filepath, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def write_vernacular_name_to_file(data: Dict, suffix: str = None):
    """
    Writes vernacular name data to a CSV file.
    :param data: Dictionary containing vernacular name data.
    :param suffix: A suffix for the file name (usually a rank ID or name).
    """
    filename = 'vernacular_names.csv' if suffix is None else 'vernacular_names_%s.csv' % suffix
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', filename)
    # Prepare dataframe schema
    schema = {'col_id': str}
    for lang in LANGUAGES_INCLUDED:
        schema['vernacular_names_%s' % lang] = str
    # Open CSV file or create if it doesn't exist
    df = pl.read_csv(filepath) if os.path.exists(filepath) else pl.DataFrame(schema=schema)
    # If col_id already exists, append vernacular name ONLY
    if len(df['col_id']) > 0 and data['col_id'] in df['col_id'].to_list():
        df_row = df.filter(df['col_id'] == data['col_id'])
        lang_column = next((k for k in data if k.startswith('vernacular_names_')), None)
        current_lang_name = df_row[lang_column][0]
        new_name = data[lang_column]
        # if the vernacular name for this language is not empty, append it to the existing name
        if current_lang_name is not None:
            new_name = '%s, %s' % (current_lang_name, new_name)
        # Update the vernacular name for the existing col_id
        df = df.with_columns(
            pl
                .when(pl.col('col_id') == data['col_id'])
                .then(pl.lit(new_name))
                .otherwise(pl.col(lang_column))
                .alias(lang_column)
        )
    # else append a new row
    else:
        df = pl.concat([df, pl.DataFrame([data], schema=list(schema))])

    # Save
    df.write_csv(filepath)


if __name__ == "__main__":

    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
    ZIP_URL = 'https://api.checklistbank.org/dataset/310958/export.zip?extended=true&format=ColDP'
    ZIP_PATH = os.path.join(CURRENT_PATH, 'temp', 'COL_database.zip')

    KINGDOM_INCLUDED = [
        'Animalia',
        'Plantae',
    ]
    # Note:
    # The NameUsage.tsv file does not contain domain rank information. Being explicit about the kingdoms we want to include
    # is necessary also to avoid including unwanted domains like Archaea, Bacteria, and Viruses.

    PHYLUM_EXCLUDED = [
        # from Animalia kingdom
        'Acanthocephala',       # Spiny-headed worms; intestinal parasites of vertebrates
        'Annelida',             # Segmented worms; includes earthworms, leeches, and polychaetes
        'Chaetognatha',         # Arrow worms; small planktonic marine predators
        'Dicyemida',            # Small, parasitic flatworms with a complex life cycle
        'Gastrotricha',         # Microscopic, free-living worms in freshwater and marine environments
        'Gnathostomulida',      # Minute marine worms living in interstitial spaces in sand
        'Hemichordata',         # Acorn worms and pterobranchs; marine, gill slits present
        'Loricifera',           # Microscopic marine animals living in sediment particles
        'Micrognathozoa',       # Microscopic freshwater animals with complex jaws
        'Nematoda',             # Roundworms; unsegmented, pseudocoelomate, many are parasitic
        'Nematomorpha',         # Horsehair worms; parasitic larvae of arthropods
        'Nemertea',             # Ribbon worms; mostly marine, with a long eversible proboscis
        'Onychophora',          # Velvet worms; soft-bodied, terrestrial predators
        'Orthonectida'          # Parasitic marine animals with a simple body plan
        'Placozoa'              # Simplest free-living animals; flattened, multicellular organisms
        'Platyhelminthes',      # Flatworms; soft-bodied, dorsoventrally flattened, often parasitic
        'Priapulida',           # Penis worms; burrowing marine predators
        'Rotifera',             # Microscopic freshwater animals with ciliated wheel-like organs
        'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        'Tardigrada',           # Water bears; microscopic, highly resilient animals
        'Xenacoelomorpha',      # Simple marine worms, close to the base of Bilateria
        # from Plantae kingdom
        'Glaucophyta',  # Freshwater microscopic algae with plastids retaining peptidoglycan.
    ]

    CLASS_EXCLUDED = [
        'Insecta' # TODO: Insecta class only contains ~1 millions species. Consider exclude
    ]
    ORDER_EXCLUDED = [
        'Sarcoptiformes', 'Trombidiformes' # Acari
    ]
    FAMILY_EXCLUDED = []
    GENUS_EXCLUDED = []

    LANGUAGES_INCLUDED = ['eng', 'spa', 'por', 'fra', 'rus', 'deu', 'ita', 'jpn', 'zho', 'kor']

    print('\n')
    cprint('#########################################################', 'green')
    cprint('######  CATALOGUE OF lIFE API - Species extractor  ######', 'green')
    cprint('#########################################################', 'green')
    print('\n')
    start_time = time.time()

    # Remove all csv files in the data folder
    cprint('Cleaning the data folder...', 'yellow')
    data_folder = os.path.join(CURRENT_PATH, 'data')
    for file in os.listdir(data_folder):
        if file.endswith('.csv'):
            os.remove(os.path.join(data_folder, file))
            cprint('File removed %s' % file, 'blue')
    cprint('Data folder cleaned. All CSV files removed.', 'green')

    # TODO: remove downloaded zip file and unzipped folder if they exist when a specific option is provided

    # ##########  Download the ZIP file  ##########
    cprint('Downloading the Catalogue of Life database...', 'yellow')
    if not os.path.exists(ZIP_PATH):
        response = requests.get(ZIP_URL)
        if response.status_code == 200:
            with open(ZIP_PATH, 'wb') as file:
                file.write(response.content)
            cprint("Archive download completed successfully.\n", 'green')
        else:
            cprint(f"Failed to download the file. Status code: {response.status_code}", 'red')
            sys.exit()
    else:
        cprint('Skipped. Archive already downloaded.\n', 'green')

    # ##########  Unzip the file  ##########
    cprint('Unzipping the Catalogue of Life database...', 'yellow')
    if not os.path.exists(os.path.join(CURRENT_PATH, 'temp', 'COL_database')):
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(CURRENT_PATH, 'temp', 'COL_database'))
        cprint("Unzipping completed successfully.\n", 'green')
    else:
        cprint('Skipped. Archive already unzipped.\n', 'green')

    # ##########  Process NameUsage.tsv to extract species   ##########
    cprint('Processing the NameUsage.tsv file to extract species...', 'yellow')
    name_usage_path = os.path.join(CURRENT_PATH, 'temp', 'COL_database', 'NameUsage.tsv')
    # Read NameUsage.tsv file using polars libray
    species_df = pl.read_csv(name_usage_path,
        separator='\t',
        ignore_errors=True,
        quote_char=None
    )
    # Retrieve a filtered list from the TSV file
    species_filtered_df = species_df.filter(
        (pl.col('col:status') == 'accepted') &
        (pl.col('col:rank') == 'species') &
        ((pl.col('col:extinct') != True) | pl.col('col:extinct').is_null()) &
        (pl.col('col:kingdom').is_in(KINGDOM_INCLUDED)) &
        ((~pl.col('col:phylum').is_in(PHYLUM_EXCLUDED)) | pl.col('col:phylum').is_null()) &
        ((~pl.col('col:class').is_in(CLASS_EXCLUDED)) | pl.col('col:class').is_null()) &
        ((~pl.col('col:order').is_in(ORDER_EXCLUDED)) | pl.col('col:order').is_null()) &
        ((~pl.col('col:family').is_in(FAMILY_EXCLUDED)) | pl.col('col:family').is_null()) &
        ((~pl.col('col:genus').is_in(GENUS_EXCLUDED)) | pl.col('col:genus').is_null())
    )
    cprint(f'Filtered to {len(species_filtered_df)} accepted species', 'green')
    # Iterate through the filtered dataset and write species data to file
    count_species = 0
    count_total_species = len(species_filtered_df)
    for i, row in enumerate(species_filtered_df.iter_rows(named=True)):
        species = {
            'col_id': row['col:ID'],
            'species': row['col:scientificName'],
            'genus': row['col:genus'] if isinstance(row['col:genus'], str) else None,
            'family': row['col:family'] if isinstance(row['col:family'], str) else None,
            'order': row['col:order'] if isinstance(row['col:order'], str) else None,
            'class': row['col:class'] if isinstance(row['col:class'], str) else None,
            'phylum': row['col:phylum'] if isinstance(row['col:phylum'], str) else None,
            'kingdom': row['col:kingdom'] if isinstance(row['col:kingdom'], str) else None,
            'authorship': row['col:authorship'] if isinstance(row['col:authorship'], str) else None,
            'environment': row['col:environment'] if isinstance(row['col:environment'], str) else None,
        }
        count_species += 1
        # Write species data to file
        write_species_to_file(species)
        cprint('Species parsed: %s of %s' % (count_species, count_total_species), 'blue', end='\r')
    cprint('\nDone. %s species saved.' % count_species, 'green', end='\r')
    print('\n')


    # # ##########  Process VernacularName.tsv to extract vernacular names  ##########
    cprint('Processing the VernacularName.tsv file to extract vernacular names...', 'yellow')
    vernacular_name_path = os.path.join(CURRENT_PATH, 'temp', 'COL_database', 'VernacularName.tsv')
    # Read the VernacularName.tsv file using polars libray
    vernacular_names_df = pl.read_csv(vernacular_name_path,separator='\t',
        ignore_errors=True,
        quote_char=None
    )
    # Retrieve a filtered list from the TSV file
    vernacular_names_filtered_df = vernacular_names_df.filter(
        pl.col('col:language').is_in(LANGUAGES_INCLUDED)
    )
    # Iterate through the filtered dataset and write species data to file
    count_names = 0
    count_total_names = len(vernacular_names_filtered_df)
    for i, row in enumerate(vernacular_names_filtered_df.iter_rows(named=True)):
        # Process each row
        count_names += 1
        name = row['col:name']
        language = row['col:language']
        # Compare vernacular name and transliteration: include both if they differ significantly
        if language in ['jpn', 'zho', 'kor', 'rus'] and isinstance(row['col:transliteration'], str):
            name = '%s (%s)' % (name, row['col:transliteration'])
        # Create a dictionary to hold the vernacular name data
        vernacular_name_data = {
            'col_id': row['col:taxonID'],
            'vernacular_names_%s' % language: name,
        }
        # Write vernacular name data to file
        write_vernacular_name_to_file(vernacular_name_data)
        cprint('Vernacular names parsed: %s of %s' % (count_names, count_total_names), 'blue', end='\r')
    print('\n')

    end_time = time.time()
    minutes, seconds = divmod(end_time-start_time, 60)
    cprint('--------------------------------', 'green')
    # cprint('Total saved species: %s ' % count_species, 'green')
    cprint('Execution time: %s min. %s sec.' % (int(minutes), int(seconds)), 'green')
    cprint('--------------------------------', 'green')
    print('\n')
