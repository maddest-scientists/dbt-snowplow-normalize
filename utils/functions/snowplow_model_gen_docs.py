import yaml
import os
import json
from collections import OrderedDict
from jsonpath_ng.ext import parse as jsonpath_parse

from .snowplow_model_gen_utils import get_fields_from_schema, snakeify_case


def get_docs(jsonData: dict, deep: bool = True, filters: list = None) -> list:
    """Get a list of docs from a Snowplow schema

    Args:
        jsonData (dict): A parsed Snowplow self-describing event or entity schema

    Returns:
        list: A list of docs for the properties in your schema
    """

    fields = get_fields_from_schema(jsonData, deep, filters)
    descriptions = []

    model_description = jsonData['description']

    for field in fields:
        description = field.get('description', '')
        descriptions.append(description)

    descriptions = (model_description, descriptions)
    
    return descriptions

def order_columns(obj, priority_columns):
    if not isinstance(obj, dict):
        return obj

    ordered = OrderedDict()

    for key in priority_columns:
        if key in obj:
            if not obj[key]:
                continue

            ordered[key] = obj[key]

    for key in sorted(obj.keys()):
        if key not in priority_columns:
            if not obj[key]:
                continue

            ordered[key] = obj[key]

    return ordered

def get_model_description(schemas_descriptions):
    if len(schemas_descriptions) == 0:
        return
    
    joined_schema_descriptions = '\n'.join(schemas_descriptions)
    description = f"Normalized event model from schemas with the following descriptions: {joined_schema_descriptions}"
    
    return description
    

def compose_documentation_content(event_names, sde_docs, sde_keys, sde_alias, model_name, documentation_content=None):
    if not documentation_content:
        documentation_content = OrderedDict()
        documentation_content['version'] = 2
        documentation_content['tables'] = []

    docs_tables = documentation_content.get('tables', [])
    schemas_descriptions = []
    doc_table_index = None

    for index, _table in enumerate(docs_tables):
        if _table['name'] == model_name:
            doc_table = OrderedDict(_table)
            doc_table_index = index

    if doc_table_index is None:
        doc_table = OrderedDict()
        doc_table['name'] = model_name

    multiple_events = len(event_names) > 1
    doc_table['columns'] = []

    for event_index, event_keys in enumerate(sde_keys):
        schema_description, event_docs = sde_docs[event_index]
        schemas_descriptions.append(schema_description)

        for key_index, key in enumerate(event_keys):
            doc_item = OrderedDict()

            description = event_docs[key_index]
            column_name = key if not multiple_events else f"{event_names[event_index]}_{key}"
            if sde_alias and type(sde_alias) == list and len(sde_alias) > 0:
                column_name = '_'.join([sde_alias[event_index], column_name])

            doc_item['name'] = snakeify_case(column_name)

            if description:
                doc_item['description'] = description

            doc_item = order_columns(doc_item, ['name', 'description'])
            doc_table['columns'].append(doc_item)

    model_description = get_model_description(schemas_descriptions)
    if model_description:
        doc_table['description'] = model_description

    doc_table = order_columns(doc_table, ['name', 'description', 'columns'])
    if doc_table_index is not None:
        docs_tables[doc_table_index] = doc_table
    else:
        docs_tables.append(doc_table)

    documentation_content = order_columns(
        documentation_content, ['version', 'description', 'tables']
    )

    return documentation_content

def docs_content(doc_filepath, event_names, sde_docs, sde_keys, sde_alias, model_name, documentation_content = None):
    if not sde_docs:
        return

    if not os.path.exists(doc_filepath): 
        os.makedirs(os.path.dirname(doc_filepath), exist_ok=True)
        return compose_documentation_content(event_names, sde_docs, sde_keys, sde_alias, model_name)

    with open(doc_filepath, 'r') as stream:
        documentation_content = yaml.safe_load(stream)

    documentation_content = compose_documentation_content(
        event_names, sde_docs, sde_keys, sde_alias, model_name, documentation_content
    )

    return documentation_content

def get_docs_yaml(documentation_content):
    documentation_content = json.dumps(documentation_content)
    documentation_content = yaml.dump(
        yaml.safe_load(documentation_content), default_flow_style=False, sort_keys=False
    )

    return documentation_content

def write_docs_file(filename: str, documentation: str, overwrite: bool = True):
    if not documentation:
        return

    documentation = get_docs_yaml(documentation)

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        f.write(documentation)