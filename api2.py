import requests
import os
from typing import Dict, List, Optional, Any
import csv
from dotenv import load_dotenv
from termcolor import cprint
import pandas as pd
import sys

load_dotenv()

def execute_api_request(
        url: str,
        params: Optional[Dict] = None) -> Optional[Dict]:
    """
    Executes the API request to the specified URL with the given parameters.
    """
    try:
        # Execute the API request
        response = requests.get(
            f"{os.getenv('API_BASE_URL')}{url}",
            params=params,
            auth=(os.getenv('API_USERNAME'), os.getenv('API_PASSWORD')),
            timeout=30
        )
        # Check if the response is successful: raise an error if not
        response.raise_for_status()
        # return the JSON response
        return response.json()
    except requests.exceptions.RequestException as e:
        cprint(f"Error retrieving data: {e}", 'red')
        return None

def write_species_to_file(data: Dict, genus_key: str):
    """
    Writes species data to a CSV file.
    """
    filepath = "%s/species_%s.csv" % (os.getenv('PATH_TO_DATA').rstrip('/'), genus_key)
    primary_key_column = 'species'
    # Open CSV file (create if it doesn't exist)
    df = pd.read_csv(filepath) if os.path.exists(filepath) else pd.DataFrame()
    # Append new species if it does not exist already
    if primary_key_column not in df.columns or data[primary_key_column] not in df[primary_key_column].values:
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    # Save
    df.to_csv(filepath, index=False)

class API:

    @staticmethod
    def get_dataset_tree(dataset_id: int) -> Dict:
        """
        Retrieves the dataset tree for the given dataset ID.
        """
        return execute_api_request(f"dataset/{dataset_id}/tree")

    @staticmethod
    def get_taxonomy_breakdown(dataset_id: int, taxon_id: int):
        """
        Retrieves the breakdown of a taxon for the given taxon ID and dataset ID.
        """
        return execute_api_request(f"dataset/{dataset_id}/taxon/{taxon_id}/breakdown")

    @staticmethod
    def get_species_by_genus(dataset_id: int, genus_id: int, api_limit: int = 1000, api__offset: int = 0):
        """
        Retrieves species for a given genus ID in the specified dataset (extinct species are excluded).
        """
        return execute_api_request(f"dataset/{dataset_id}/tree/{genus_id}/children",
            {
                'limit': api_limit,
                'offset': api__offset,
                'extinct': False
            }
        )

    @staticmethod
    def get_species_vernacular_names(dataset_id: int, species_id: int) -> Dict:
        """
        Retrieves species information by species ID in the specified dataset.
        """
        return execute_api_request(f"dataset/{dataset_id}/taxon/{species_id}/vernacular")


if __name__ == '__main__':

    COL_2025_dataset_id = 310463

    DOMAIN_INCLUDED = [
        # 'Archaea',     # single-celled prokaryotic organisms characterized by unique molecular and biochemical features
        # 'Bacteria',    # single-celled prokaryotic organisms with a wide range of shapes and metabolic capabilities
        'Eukaryota',   # Eukaryotic organisms with complex cells containing a nucleus and organelles
        # 'Viruses',     # Viruses
    ]
    KINGDOM_INCLUDED = [
        # Eukaryotic domain
        'Animalia',     # multicellular eukaryotes with specialized tissues and organs
        # 'Chromista',    # algae and protists with chloroplasts
        # 'Fungi',        # yeasts, molds, and mushrooms
        'Plantae',      # multicellular eukaryotes with chloroplasts, including mosses, ferns, and flowering plants
        # 'Protozoa',     # single-celled eukaryotes, often motile and heterotrophic
    ]
    PHYLUM_INCLUDED = [
        # Animalia kingdom
        # 'Acanthocephala',       # Spiny-headed worms; intestinal parasites of vertebrates
        # 'Annelida',             # Segmented worms; includes earthworms, leeches, and polychaetes
        'Arthropoda',           # Insects, spiders, crustaceans; exoskeleton and jointed appendages
        'Brachiopoda',          # Lamp shells; marine animals with two shells, superficially like bivalves
        'Bryozoa',              # Moss animals; small colonial filter feeders
        # 'Chaetognatha',         # Arrow worms; small planktonic marine predators
        'Chordata',             # Vertebrates and their relatives; possess a notochord at some stage
        'Cnidaria',             # Jellyfish, corals, sea anemones; radial symmetry with stinging cells (cnidocytes)
        'Ctenophora',           # Comb jellies; marine predators with ciliary plates and bioluminescence
        'Cycliophora',          # Tiny marine animals living on lobster mouthparts
        # 'Dicyemida',            # Small, parasitic flatworms with a complex life cycle
        'Echinodermata',        # Sea stars, sea urchins, sea cucumbers; radial symmetry in adults
        'Entoprocta',           # Sessile aquatic filter feeders with a crown of tentacles
        # 'Gastrotricha',         # Microscopic, free-living worms in freshwater and marine environments
        # 'Gnathostomulida',      # Minute marine worms living in interstitial spaces in sand
        # 'Hemichordata',         # Acorn worms and pterobranchs; marine, gill slits present
        'Kinorhyncha',          # Mud dragons; tiny segmented marine invertebrates
        # 'Loricifera',           # Microscopic marine animals living in sediment particles
        # 'Micrognathozoa',       # Microscopic freshwater animals with complex jaws
        'Mollusca',             # Soft-bodied animals often with a shell; snails, clams, octopuses, squids
        # 'Nematoda',             # Roundworms; unsegmented, pseudocoelomate, many are parasitic
        # 'Nematomorpha',         # Horsehair worms; parasitic larvae of arthropods
        # 'Nemertea',             # Ribbon worms; mostly marine, with a long eversible proboscis
        # 'Onychophora',          # Velvet worms; soft-bodied, terrestrial predators
        'Orthonectida'          # Parasitic marine animals with a simple body plan
        'Phoronida'             # Worm-like marine animals living in tubes
        'Placozoa'              # Simplest free-living animals; flattened, multicellular organisms
        # 'Platyhelminthes',      # Flatworms; soft-bodied, dorsoventrally flattened, often parasitic
        'Porifera'              # Sponges; simple, porous, sessile filter feeders
        'Priapulida',           # Penis worms; burrowing marine predators
        # 'Rotifera',             # Microscopic freshwater animals with ciliated wheel-like organs
        # 'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        # 'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        # 'Tardigrada',           # Water bears; microscopic, highly resilient animals
        # 'Xenacoelomorpha',      # Simple marine worms, close to the base of Bilateria

        # Plantae kingdom
        'Anthocerotophyta',     # (Hornworts),"Non‑vascular land plants with elongated horn‑shaped sporophytes."
        'Bryophyta',            # (Mosses),"Non‑vascular land plants with leafy gametophytes and persistent sporophytes."
        'Marchantiophyta',      # (Liverworts),"Non‑vascular land plants, often leafy or thalloid, with simple unbranched sporophytes."
        'Tracheophyta',         # (Vascular plants),"All vascular plants with xylem and phloem; includes ferns, gymnosperms, and angiosperms.
        'Charophyta',           # (Charophyte algae),"Freshwater green algae closely related to land plants."
        'Chlorophyta',          # (Green algae),"Mostly aquatic green algae with chlorophyll a and b, precursors to terrestrial plants."
        'Glaucophyta',          # (Glaucophyte algae),"Freshwater microscopic algae with plastids retaining peptidoglycan."
        'Rhodophyta',           # (Red algae),"Mostly marine algae with phycobiliproteins as accessory pigments."
    ]

    print('\n ##### CATALOGUE OF lIFE API - Species extractor ##### \n')

    # Retrieve all datasets from COL 2025
    col_datasets = API.get_dataset_tree(dataset_id=COL_2025_dataset_id)

    # Iterate over domains (filtering by DOMAIN_INCLUDED)
    for domain_obj in col_datasets.get('result', []):
        domain_name = domain_obj.get('name')
        if domain_name in DOMAIN_INCLUDED:
            cprint('DOMAIN: ' + domain_name, 'yellow')

            # Retrieve and iterate over kingdoms (filtering by KINGDOM_INCLUDED)
            domain_breakdown = API.get_taxonomy_breakdown(dataset_id=COL_2025_dataset_id, taxon_id=domain_obj.get('id'))
            for kingdom_obj in domain_breakdown:
                kingdom_name = kingdom_obj.get('name')
                if kingdom_name in KINGDOM_INCLUDED:
                    cprint('   -> KINGDOM: ' + kingdom_name, 'yellow')

                    # Iterate over phyla (filtering by PHYLUM_INCLUDED)
                    for phylum_obj in kingdom_obj.get('children', []):
                        phylum_name = phylum_obj.get('name')
                        if phylum_name in PHYLUM_INCLUDED:
                            cprint('      -> PHYLUM: ' + phylum_name, 'yellow')

                            # Retrieve and iterate over classes
                            phylum_breakdown = API.get_taxonomy_breakdown(dataset_id=COL_2025_dataset_id, taxon_id=phylum_obj.get('id'))
                            for class_obj in phylum_breakdown:
                                class_name = class_obj.get('name')
                                cprint('         -> CLASS: ' + class_name, 'yellow')

                                # Note: the children attribute is not 100% reliable (few cases of skipped taxon levels) so we need
                                # to retrieve the sublevels explicitly with a new API request.
                                # For example, class "Leptocardii" has no direct children (order), but it contains a family ("Branchiostomatidae")
                                # and few genera

                                # Retrieve and iterate over orders
                                class_breakdown = API.get_taxonomy_breakdown(dataset_id=COL_2025_dataset_id, taxon_id=class_obj.get('id'))
                                for order_obj in class_breakdown:
                                    order_name = order_obj.get('name')
                                    cprint('            -> ORDER: ' + order_name, 'yellow')

                                    # Retrieve and iterate over families
                                    order_breakdown = API.get_taxonomy_breakdown(dataset_id=COL_2025_dataset_id, taxon_id=order_obj.get('id'))
                                    for family_obj in order_breakdown:
                                        family_name = family_obj.get('name')
                                        cprint('               -> FAMILY: ' + family_name, 'yellow')

                                        # Retrieve and iterate over genera
                                        family_breakdown = API.get_taxonomy_breakdown(dataset_id=COL_2025_dataset_id, taxon_id=family_obj.get('id'))
                                        for genus_obj in family_breakdown:
                                            genus_name = genus_obj.get('name')
                                            species_count_total = genus_obj.get('species', 0)
                                            species_count_total_retrieved = 0
                                            cprint('                  -> GENUS: ' + genus_name + ' (species: ' + str(species_count_total) + ')', 'yellow')

                                            # Retrieve and iterate over species (in chunks of 1000)
                                            limit = 1000
                                            offset = 0
                                            while species_count_total_retrieved < species_count_total:
                                                species_by_genus = API.get_species_by_genus(dataset_id=COL_2025_dataset_id, genus_id=genus_obj.get('id'), api_limit=limit, api__offset=offset)
                                                offset += limit
                                                species_count_total_retrieved += species_by_genus.get('total')
                                                for species_obj in species_by_genus.get('result', []):

                                                    # Create species dictionary (to be used as a row in the CSV file)
                                                    species = {
                                                        'id': species_obj.get('id'),
                                                        'species': species_obj.get('name'),
                                                        'genus': genus_name,
                                                        'family': family_name,
                                                        'order': order_name,
                                                        'class': class_name,
                                                        'phylum': phylum_name,
                                                        'kingdom': kingdom_name,
                                                        'domain': domain_name,
                                                        'authorship': species_obj.get('authorship')
                                                    }

                                                    # Retrieve vernacular names
                                                    species_vernacular_names = API.get_species_vernacular_names(dataset_id=COL_2025_dataset_id, species_id=species_obj.get('id'))
                                                    if species_vernacular_names:
                                                        # Add vernacular names to species dictionary (filtering by LANGUAGES_INCLUDED)
                                                        for species_vernacular_name in species_vernacular_names:
                                                            language = species_vernacular_name.get('language')
                                                            if language in os.getenv('LANGUAGES_INCLUDED'):
                                                                species_vernacular = species_vernacular_name.get('name')
                                                                species['vernacular_%s' % language] = species_vernacular

                                                    # TODO: retrieve additional information
                                                    #  - retrieve distributions (optional)
                                                    #  - retrieve environments (optional)

                                                    # Write species data to file
                                                    write_species_to_file(species, family_obj.get('id'))

                                                cprint('                     Retrieved and saved %s/%s species ' % (species_count_total_retrieved, species_count_total), 'green')
