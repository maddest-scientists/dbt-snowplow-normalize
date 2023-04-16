from jsonpath_ng.ext import parse as jsonpath_parse
import re


def get_filtered_keys(jsonData: dict, filters: list = None):
    filters = filters or []

    jsonpath_filters_expressions = [jsonpath_parse(_filter) for _filter in filters] if filters else []
    filtered = []
    for _filter_expression in jsonpath_filters_expressions:
        _matches = _filter_expression.find(jsonData)
        filtered.extend([str(_match.full_path).replace('properties.', '') for _match in _matches])
    filtered = set(filtered)
    
    return filtered

def get_leafs(paths):
    paths = sorted(paths, key=len)
    leafs = []

    for index, current in enumerate(paths):
        if any([other.startswith(current) for other in paths[index+1:]]):
            continue

        leafs.append(current)

    return leafs

def get_fields_from_schema(jsonData: dict, deep: bool = True, filters: list = None, return_keys: bool = False) -> list:
    jsonpath_for_keys = '$..properties.*' if deep else '$.properties.*'
    replace_for_keys = 'properties.'
    
    jsonpath_keys_expression = jsonpath_parse(jsonpath_for_keys)
    filtered = get_filtered_keys(jsonData, filters)

    _matches = jsonpath_keys_expression.find(jsonData)
    _paths = [str(_match.full_path).replace(replace_for_keys, '') for _match in _matches]
    paths = get_leafs(_paths)
    paths = sorted(paths)

    items = []

    for path in paths:
        if path in filtered:
            continue

        if return_keys:
            items.append(path)
            continue

        path_lst = path.split('.')
        if len(path_lst) == 1:
            properties = jsonData['properties'][path]
            items.append(properties)
        else:
            properties = jsonData['properties']
            for _path in path_lst:
                properties = properties[_path]
                if 'properties' in properties:
                    properties = properties['properties']

            items.append(properties)

    return items

def snakeify_case(text):
    camel_string1 = r'([A-Z]+)([A-Z][a-z])'
    camel_string2 = r'([a-z\d])([A-Z])'
    replace_string = '\\1_\\2'

    output_text = re.sub(camel_string2, replace_string, re.sub(camel_string1, replace_string, text)).replace('-', '_').lower()
    # output_text = output_text.replace('.', '_')

    return output_text