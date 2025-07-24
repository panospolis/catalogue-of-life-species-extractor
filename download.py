import csv
import os
import pandas as pd
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
        'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        'Tardigrada',           # Water bears; microscopic, highly resilient animals
        'Xenacoelomorpha',      # Simple marine worms, close to the base of Bilateria
        # from Plantae kingdom
        'Glaucophyta',  # Freshwater microscopic algae with plastids retaining peptidoglycan.
    ]

    CLASS_EXCLUDED = []
    ORDER_EXCLUDED = []
    FAMILY_EXCLUDED = []
    GENUS_EXCLUDED = []

    LANGUAGES_INCLUDED = ['eng', 'spa', 'por', 'fra', 'rus', 'deu', 'ita', 'jpn', 'zho', 'kor']

    CHUNK_SIZE = 3000

    print('\n')
    cprint('#########################################################', 'green')
    cprint('######  CATALOGUE OF lIFE API - Species extractor  ######', 'green')
    cprint('#########################################################', 'green')
    print('\n')
    start_time = time.time()

    # Remove all csv files in the data folder
    data_folder = os.path.join(CURRENT_PATH, 'data')
    for file in os.listdir(data_folder):
        if file.endswith('.csv'):
            os.remove(os.path.join(data_folder, file))
    cprint('Data folder cleaned. All CSV files removed.', 'green')

    # TODO: remove downloaded zip file and unzipped folder if they exist when a specific option is provided

    # ##########  Download the ZIP file  ##########
    cprint('Downloading the Catalogue of Life database...', 'yellow')
    if not os.path.exists(ZIP_PATH):
        response = requests.get(ZIP_PATH)
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
    count_species = 0
    # Iterate through the NameUsage.tsv file in chunks to avoid memory issues
    for chunk in pd.read_csv(name_usage_path, sep='\t', chunksize=CHUNK_SIZE):
        # Process each row in the chunk
        for _, row in chunk.iterrows():
            if (row['col:status'] == 'accepted'
                    and row['col:rank'] == 'species'
                    and row['col:extinct'] != True
                    and row['col:kingdom'] in KINGDOM_INCLUDED
                    and row['col:phylum'] not in PHYLUM_EXCLUDED
                    and row['col:class'] not in CLASS_EXCLUDED
                    and row['col:order'] not in ORDER_EXCLUDED
                    and row['col:family'] not in FAMILY_EXCLUDED
                    and row['col:genus'] not in GENUS_EXCLUDED):
                count_species += 1

                # Retrieve the species information
                species = {
                    'id': row['col:ID'],
                    'name': row['col:scientificName'],
                    'authorship': row['col:authorship'] if isinstance(row['col:authorship'], str) else None,
                    'environment': row['col:environment'] if isinstance(row['col:environment'], str) else None,
                    'genus': row['col:genus'] if isinstance(row['col:genus'], str) else None,
                    'family': row['col:family'] if isinstance(row['col:family'], str) else None,
                    'order': row['col:order'] if isinstance(row['col:order'], str) else None,
                    'class': row['col:class'] if isinstance(row['col:class'], str) else None,
                    'phylum': row['col:phylum'] if isinstance(row['col:phylum'], str) else None,
                    'kingdom': row['col:kingdom'] if isinstance(row['col:kingdom'], str) else None,
                }

                # TODO: retrieve vernacular names

                # Write species data to file
                write_species_to_file(species)

        cprint('Progressive species count: %s' % count_species, 'blue', end='\r')
    cprint('\nDone. %s species saved.' % count_species, 'green', end='\r')

    end_time = time.time()
    print('\n')
    cprint('--------------------------------', 'green')
    cprint('Total saved species: %s ' % count_species, 'green')
    cprint('Execution time: %.2fs' % (end_time - start_time), 'green')
    cprint('--------------------------------', 'green')
    print('\n')
