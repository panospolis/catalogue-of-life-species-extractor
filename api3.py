import requests
import os
from typing import Dict, List, Optional, Any
import csv
from dotenv import load_dotenv
from termcolor import cprint
import pandas as pd
import sys

load_dotenv()

def get_env_values_to_list(env_values: str) -> List[str]:
    """
    Converts a comma-separated string of environment values into a list.
    :param env_values:
    :return:
    """
    return [value.strip() for value in env_values.split(',') if value.strip()]


def execute_api_request(
        url: str,
        params: Optional[Dict] = None) -> Optional[Dict]:
    """
    Executes the API request to the specified URL with the given parameters.
    :param url: The API endpoint to call.
    :param params: Optional dictionary of parameters to include in the request.
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

def write_species_to_file(data: Dict, suffix: str):
    """
    Writes species data to a CSV file.
    :param data: Dictionary containing species data.
    :param suffix: A suffix for the file name (usually a rank ID or name).
    """
    filepath = "%s/species_%s.csv" % (os.getenv('PATH_TO_DATA').rstrip('/'), suffix)
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
        :param dataset_id: The ID of the dataset to query.
        """
        return execute_api_request(f"dataset/{dataset_id}/tree")

    @staticmethod
    def get_taxonomy_breakdown(dataset_id: int, taxon_id: int):
        """
        Retrieves the breakdown of a taxon for the given taxon ID and dataset ID.
        :param dataset_id: The ID of the dataset to query.
        :param taxon_id: The ID of the taxon to query.
        """
        return execute_api_request(f"dataset/{dataset_id}/taxon/{taxon_id}/breakdown")

    @staticmethod
    def get_species_by_genus(dataset_id: int, genus_id: int, api_limit: int = 1000, api__offset: int = 0):
        """
        Retrieves species for a given genus ID in the specified dataset (extinct species are excluded).
        :param dataset_id: The ID of the dataset to query.
        :param genus_id: The ID of the genus to query.
        :param api_limit: The maximum number of results to return (default is 1000).
        :param api__offset: The offset for pagination (default is 0).
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
        :param dataset_id: The ID of the dataset to query.
        :param species_id: The ID of the species to query.
        """
        return execute_api_request(f"dataset/{dataset_id}/taxon/{species_id}/vernacular")


def taxonomy_rank_is_included(rank: str, name: str) -> bool:
    if rank == "domain":
        return name in DOMAIN_INCLUDED
    elif rank == "kingdom":
        return name in KINGDOM_INCLUDED
    elif rank == "phylum":
        return name in PHYLUM_INCLUDED
    elif rank == "order":
        return name not in [
            'Amblypygi',
            'Araneae',
            'Holothyrida',
            'Opilioacarida',
            'Opiliones',
            'Palpigradi',
            'Sarcoptiformes',
            'Trombidiformes',
        ]
    return True

def save_species(species, path):
    """
    Save species data to a CSV file.
    :param species: Dictionary containing species data.
    :param path: Dictionary accumulating ranks from root to here.
    :return:
    """
    # Keep only the relevant fields
    species = {k: v for k, v in species.items() if k in ["id", "name", "authorship"]}
    # Merge the species data with the path (taxonomy ranks accumulated during traversal)
    species.update(path)
    # Retrieve vernacular names
    species_vernacular_names = API.get_species_vernacular_names(dataset_id=COL_2025_dataset_id,
                                                                species_id=species['id'])
    if species_vernacular_names:
        # Add vernacular names to species dictionary (filtering by LANGUAGES_INCLUDED)
        for species_vernacular_name in species_vernacular_names:
            language = species_vernacular_name.get('language')
            if language in get_env_values_to_list(os.getenv('LANGUAGES_INCLUDED')):
                species_vernacular = species_vernacular_name.get('name')
                species['vernacular_%s' % language] = species_vernacular

    # TODO: retrieve additional information
    #  - retrieve distributions (optional)
    #  - retrieve environments (optional)

    # Write species data to file
    write_species_to_file(species, '%s_%s' % (species['class'], species['order']))

def traverse_genra(genus_id, species_count_total, path, indent):
    """
    Retrieve all species in a genus (API requests executed in chunks)
    :param genus_id: The genus ID
    :param species_count_total: Total number of species in the genus
    :param path: Dictionary accumulating ranks from root to here
    :param indent: Indentation for console output
    """
    species_count_total_retrieved = 0
    limit = 1000
    offset = 0
    while species_count_total_retrieved < species_count_total:
        # Retrieve species in chunks
        species_by_genus = ((API.get_species_by_genus(dataset_id=COL_2025_dataset_id, genus_id=genus_id, api_limit=limit, api__offset=offset))
                            .get('result'))
        # If no species are found, break the loop
        if not species_by_genus:
            break
        # Subgenus rank found: iterate over subgenra
        if species_by_genus[0]['rank'] != 'species':
            species_by_subgenus = []
            for subgenus in species_by_genus:
                subgenus_result = (API.get_species_by_genus(dataset_id=COL_2025_dataset_id, genus_id=subgenus['id'], api_limit=limit, api__offset=offset)).get('result')
                if subgenus_result is not None:
                    species_by_subgenus = species_by_subgenus + subgenus_result
            species_by_genus = species_by_subgenus
        # Update counters
        species_count_total_retrieved += len(species_by_genus)
        offset += limit
        # Print to console the current chunk
        cprint('%s - Retrieving and saving %s/%s species ' % (indent, species_count_total_retrieved, species_count_total), 'green')
        # Iterate over the retrieved species
        for species in species_by_genus:
            save_species(species, path)


def traverse_taxonomy(data, path):
    """
    Traverse all taxonomy ranks recursively from domain to genus.
    :param data: Current taxonomy (dict with 'id', 'name', 'rank', etc.)
    :param path: dictionary accumulating ranks from root to here
    """
    # Print to console the current rank
    indent = ' ' * (RANK_INDEX[data["rank"]] * 3)
    cprint('%s %s: %s' % (indent, data['rank'].upper(), data['name']), 'yellow')

    # Add the current rank to the data object
    path[data["rank"]] = data["name"]

    # At rank "genus" retrieve the species (in chunks) and write to CSV file
    if data["rank"] == "genus":
        traverse_genra(data["id"], data["species"], path, indent)
        return

    # Retrieve rank breakdown from API
    rank_breakdown = API.get_taxonomy_breakdown(COL_2025_dataset_id, data["id"])

    # If the rank breakdown is empty, it means there are no children for this rank#
    if not rank_breakdown:
        # print ('%s NO CHILDREN for %s %s' % (indent, data["rank"], data["name"]))
        return

    for child in rank_breakdown:
        child_rank = child.get("rank")
        # Skip unknown ranks
        if child_rank not in RANK_INDEX:
            # print(RANK_INDEX)
            # continue
            cprint('%s Skipped unknown rank %s %s' % (indent, child_rank, child.get("name")), 'red')
            sys.exit(0)
        # Skip ranks that are not included in the filters
        if not taxonomy_rank_is_included(child_rank, child.get("name")):
            continue
        traverse_taxonomy(child, path.copy())


if __name__ == '__main__':

    COL_2025_dataset_id = 310463

    DOMAIN_INCLUDED = [
        # 'Archaea',     # single-celled prokaryotic organisms characterized by unique molecular and biochemical features
        # 'Bacteria',    # single-celled prokaryotic organisms with a wide range of shapes and metabolic capabilities
        'Eukaryota',  # Eukaryotic organisms with complex cells containing a nucleus and organelles
        # 'Viruses',     # Viruses
    ]
    KINGDOM_INCLUDED = [
        # Eukaryotic domain
        'Animalia',  # multicellular eukaryotes with specialized tissues and organs
        # 'Chromista',    # algae and protists with chloroplasts
        # 'Fungi',        # yeasts, molds, and mushrooms
        'Plantae',  # multicellular eukaryotes with chloroplasts, including mosses, ferns, and flowering plants
        # 'Protozoa',     # single-celled eukaryotes, often motile and heterotrophic
    ]
    PHYLUM_INCLUDED = [
        # Animalia kingdom
        # 'Acanthocephala',       # Spiny-headed worms; intestinal parasites of vertebrates
        # 'Annelida',             # Segmented worms; includes earthworms, leeches, and polychaetes
        'Arthropoda',  # Insects, spiders, crustaceans; exoskeleton and jointed appendages
        'Brachiopoda',  # Lamp shells; marine animals with two shells, superficially like bivalves
        'Bryozoa',  # Moss animals; small colonial filter feeders
        # 'Chaetognatha',         # Arrow worms; small planktonic marine predators
        'Chordata',  # Vertebrates and their relatives; possess a notochord at some stage
        'Cnidaria',  # Jellyfish, corals, sea anemones; radial symmetry with stinging cells (cnidocytes)
        'Ctenophora',  # Comb jellies; marine predators with ciliary plates and bioluminescence
        'Cycliophora',  # Tiny marine animals living on lobster mouthparts
        # 'Dicyemida',            # Small, parasitic flatworms with a complex life cycle
        'Echinodermata',  # Sea stars, sea urchins, sea cucumbers; radial symmetry in adults
        'Entoprocta',  # Sessile aquatic filter feeders with a crown of tentacles
        # 'Gastrotricha',         # Microscopic, free-living worms in freshwater and marine environments
        # 'Gnathostomulida',      # Minute marine worms living in interstitial spaces in sand
        # 'Hemichordata',         # Acorn worms and pterobranchs; marine, gill slits present
        'Kinorhyncha',  # Mud dragons; tiny segmented marine invertebrates
        # 'Loricifera',           # Microscopic marine animals living in sediment particles
        # 'Micrognathozoa',       # Microscopic freshwater animals with complex jaws
        'Mollusca',  # Soft-bodied animals often with a shell; snails, clams, octopuses, squids
        # 'Nematoda',             # Roundworms; unsegmented, pseudocoelomate, many are parasitic
        # 'Nematomorpha',         # Horsehair worms; parasitic larvae of arthropods
        # 'Nemertea',             # Ribbon worms; mostly marine, with a long eversible proboscis
        # 'Onychophora',          # Velvet worms; soft-bodied, terrestrial predators
        'Orthonectida'  # Parasitic marine animals with a simple body plan
        'Phoronida'  # Worm-like marine animals living in tubes
        'Placozoa'  # Simplest free-living animals; flattened, multicellular organisms
        # 'Platyhelminthes',      # Flatworms; soft-bodied, dorsoventrally flattened, often parasitic
        'Porifera'  # Sponges; simple, porous, sessile filter feeders
        'Priapulida',  # Penis worms; burrowing marine predators
        # 'Rotifera',             # Microscopic freshwater animals with ciliated wheel-like organs
        # 'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        # 'Sipuncula',            # Peanut worms; unsegmented marine burrowers
        # 'Tardigrada',           # Water bears; microscopic, highly resilient animals
        # 'Xenacoelomorpha',      # Simple marine worms, close to the base of Bilateria

        # Plantae kingdom
        'Anthocerotophyta',  # (Hornworts),"Non‑vascular land plants with elongated horn‑shaped sporophytes."
        'Bryophyta',  # (Mosses),"Non‑vascular land plants with leafy gametophytes and persistent sporophytes."
        'Marchantiophyta',
        # (Liverworts),"Non‑vascular land plants, often leafy or thalloid, with simple unbranched sporophytes."
        'Tracheophyta',
        # (Vascular plants),"All vascular plants with xylem and phloem; includes ferns, gymnosperms, and angiosperms.
        'Charophyta',  # (Charophyte algae),"Freshwater green algae closely related to land plants."
        'Chlorophyta',
        # (Green algae),"Mostly aquatic green algae with chlorophyll a and b, precursors to terrestrial plants."
        'Glaucophyta',  # (Glaucophyte algae),"Freshwater microscopic algae with plastids retaining peptidoglycan."
        'Rhodophyta',  # (Red algae),"Mostly marine algae with phycobiliproteins as accessory pigments."
    ]

    print('\n#########################################################')
    print('######  CATALOGUE OF lIFE API - Species extractor  ######')
    print('#########################################################\n')

    # RANKS = ["domain", "kingdom", "phylum", "class", "order", "family", "genus"]
    RANKS = ["domain", "kingdom", "phylum", "class", "order", "family", "genus", "species"]
    RANK_INDEX = {rank: idx for idx, rank in enumerate(RANKS)}

    # Retrieve all datasets from COL 2025
    col_datasets = API.get_dataset_tree(dataset_id=COL_2025_dataset_id)

    # Iterate over domains (filtering by DOMAIN_INCLUDED)
    for domain_obj in col_datasets.get('result', []):
        domain_name = domain_obj.get('name')
        if taxonomy_rank_is_included('domain', domain_name):
            # Iterate over the other ranks (kingdom, phylum, etc.)
            traverse_taxonomy(domain_obj, {})
