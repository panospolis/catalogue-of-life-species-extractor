import requests
import os
from typing import Dict, List, Optional, Any
import csv
from dotenv import load_dotenv

load_dotenv()


def get_env_values_to_list(env_values: str) -> List[str]:
    return [value.strip() for value in env_values.split(',') if value.strip()]


ranks_to_include = get_env_values_to_list(os.getenv('RANKS_INCLUDED'))
phylum = get_env_values_to_list(os.getenv('PHYLUM_INCLUDED'))
languages = get_env_values_to_list(os.getenv('LANGUAGES_INCLUDED'))


def execute_api_request(
        url: str,
        params: Optional[Dict] = None) -> Optional[Dict]:
    """
    Executes the API request to the specified URL with the given parameters.
    """
    try:
        response = requests.get(
            f"{os.getenv('API_BASE_URL')}{url}",
            params=params,
            auth=(os.getenv('API_USERNAME'), os.getenv('API_PASSWORD')),
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data: {e}")
        return None


def retrieve_species_data(
        dataset_id: int,
        id: int = 1000) -> Optional[Dict]:
    url = (
        f"dataset/{dataset_id}/tree/{id}/children")
    print(url)
    params = {
        # 'catalogueKey': 3,
        # 'extinct': True,
        'type': 'PROJECT'
    }
    return execute_api_request(url, params)


def retrieve_nameusage_data(
        dataset_id: int,
        limit: int = 1000,
        offset: int = 0,
) -> Optional[Dict]:
    url = f"dataset/{dataset_id}/nameusage/search"
    params = {
        'limit': limit,
        'offset': offset,
        'rank': 'species'
    }
    return execute_api_request(url, params)


def retrieve_species_info(dataset_id: int, species_id: int) -> Any:
    url = f"dataset/{dataset_id}/taxon/{species_id}/info"
    return execute_api_request(url)


def parse_nameusage_results(dataset_id: int, data: Dict) -> List[Dict]:
    if not data or 'result' not in data:
        return []

    parsed_results = []
    for record in data['result']:

        if not os.path.exists(f"{os.getenv('PATH_TO_DATA')}/nameusage_data_{dataset_id}_{record.get('id')}.csv"):
            #!!!! stop looking for specific path -> ranks_to_include and retrieve all records !!!!
            # ranks = [cls.get('rank') for cls in record.get('classification', [])]
            # if all(rank in ranks for rank in ranks_to_include):
            # print(record)
            parsed_results = get_species_list(dataset_id, record)
            if len(parsed_results) > 0:
                write_to_file(parsed_results, f"nameusage_data_{dataset_id}.csv")
    return parsed_results


def get_species_list(dataset_id: int, record: Dict) -> Optional[List]:
    parsed_results = []
    #!!! stop searching for species tree !!!
    ## species_list = retrieve_species_data(dataset_id, record.get('id'))

    ## if species_list is not None and 'result' in species_list:
    ##     for species in species_list['result']:
    parsed_record = {
        'id': record.get('id'),
        'species': record.get('usage').get('name').get('scientificName', {}),
        'distribution': '',
        'environments': record.get('usage', {}).get('environments', [])
    }

    parsed_record.update(set_languages_dict())

    info = retrieve_species_info(dataset_id, record.get('id'))
    if info is not None:
        parsed_record = get_common_name(parsed_record, info)
        parsed_record = get_distributions(parsed_record, info)
        parsed_record = get_classification(parsed_record, record)

    parsed_results.append(parsed_record)
    return parsed_results


def get_classification(parsed_record: Dict, record: Dict) -> Dict:
    for cls in record.get('classification', []):
        if cls.get('rank') in ranks_to_include:
            parsed_record[cls.get('rank', '')] = cls.get('name', '')
    return parsed_record


def get_common_name(parsed_record: Dict, info: Dict) -> Dict:
    if 'vernacularNames' in info:
        for item in info.get('vernacularNames', {}):
            if 'name' in item:
                parsed_record[item.get('language', '')] = item.get('name', '')
    return parsed_record


def get_distributions(parsed_record: Dict, info: Dict) -> Dict:
    if 'distributions' in info:
        for dist in info.get('distributions', []):
            if 'area' in dist:
                if 'name' in dist.get('area', {}):
                    parsed_record['distribution'] += dist.get('area', '').get('name', '') + ', '
                elif 'globalId' in dist.get('area', {}):
                    parsed_record['distribution'] += dist.get('area', '').get('globalId', '') + ', '
    return parsed_record


def get_all_nameusage_data(dataset_id: int) -> List[Dict]:
    all_results = []
    offset = 0
    limit = 1000
    print("running dataset", dataset_id)
    while True:
        data = retrieve_nameusage_data(dataset_id, limit, offset)

        if not data:
            break

        results = parse_nameusage_results(dataset_id, data)
        all_results.extend(results)

        if data.get('last', True) or offset + limit >= 100000:
            break

        offset += limit
    return all_results


def write_to_file(data: List[Dict], filename: str):
    if not data:
        print("No data to write.")
        return

    file_exists = os.path.exists(f"{os.getenv('PATH_TO_DATA')}/{filename}")
    with open(f"{os.getenv('PATH_TO_DATA')}/{filename}", 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'species', 'distribution', 'environments', 'kingdom', 'phylum', 'class', 'order', 'family',
                      'genus']

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
        for result in data:
            row = {}
            row['id'] = result.get('id', 0)
            row['species'] = result.get('species', '')
            row['distribution'] = result.get('distribution', '')
            row['environments'] = ', '.join(result.get('environments', []))
            row['kingdom'] = result.get('kingdom', '')
            row['phylum'] = result.get('phylum', '')
            row['class'] = result.get('class', '')
            row['order'] = result.get('order', '')
            row['family'] = result.get('family', '')
            row['genus'] = result.get('genus', '')

            writer.writerow(row)

    with open(f"{os.getenv('PATH_TO_DATA')}/lang_{filename}", 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id']
        fieldnames += languages
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
        for result in data:
            row = {}
            row['id'] = result.get('id', 0)
            for value in languages:
                row[value] = result.get(value, '')

            writer.writerow(row)

    print(f"Data saved to {filename}")


def set_languages_dict() -> Dict:
    return {lang: "" for lang in languages}


if __name__ == '__main__':

    for dataset in get_env_values_to_list(os.getenv('DATASETS')):
        get_all_nameusage_data(int(dataset))
