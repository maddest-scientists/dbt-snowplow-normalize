import yaml
import os
import json
from collections import OrderedDict
from jsonpath_ng.ext import parse as jsonpath_parse

from .snowplow_model_gen_utils import get_fields_from_schema


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


def compose_documentation_content(sde_docs, sde_keys, model_name, documentation_content):
    if not documentation_content:
        documentation_content = OrderedDict()
        documentation_content['version'] = 2
        documentation_content['tables'] = []

    docs_tables = documentation_content.get('tables')
    doc_table = None

    for index, _table in enumerate(docs_tables):
        if _table['name'] == model_name:
            doc_table = OrderedDict(_table)
            docs_tables[index] = doc_table
    
    if not doc_table:
        doc_table = OrderedDict()
        doc_table['name'] = model_name
        doc_table['columns'] = []

        docs_tables.append(doc_table)

    schemas_descriptions = []

    doc_table['columns'] = []
    for event_index, event_keys in enumerate(sde_keys):
        schema_description, event_docs = sde_docs[event_index]
        schemas_descriptions.append(schema_description)

        for key_index, key in enumerate(event_keys):
            doc_item = OrderedDict()

            description = event_docs[key_index]
            doc_item['name'] = key

            if description != '':
                doc_item['description'] = description

            doc_table['columns'].append(doc_item)

    joined_schema_descriptions = '\n\t '.join(schemas_descriptions)
    if len(joined_schema_descriptions):
        description = f"Normalized event model from schames with the following descriptions: {joined_schema_descriptions}"
        doc_table['description'] = description

    priority_keys = ['name', 'description', 'columns']
    all_keys  = list(doc_table.keys())

    ordered_keys = priority_keys + sorted([
        key for key in all_keys if key not in priority_keys
    ])

    final = OrderedDict()

    for key in ordered_keys:
        final[key] = doc_table[key]

    return final

def docs_content(doc_filepath, sde_docs, sde_keys, model_name, documentation_content):
    documentation_content = None
    
    if not sde_docs:
        return

    if not os.path.exists(doc_filepath): 
        os.makedirs(os.path.dirname(doc_filepath), exist_ok=True)
        return compose_documentation_content(
            sde_docs, sde_keys, model_name, {}
        )

    with open(doc_filepath, 'r') as stream:
        documentation_content = yaml.safe_load(stream)

    documentation_content = compose_documentation_content(
        sde_docs, sde_keys, model_name, documentation_content
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