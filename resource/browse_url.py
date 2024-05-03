import pandas
from webtable_extractor_commons import safe_get


def build_select_choices(choices=None):
    if not choices:
        return {"choices": []}
    if isinstance(choices, str):
        return {"choices": [{"value":None, "label": "{}".format(choices)}]}
    if isinstance(choices, list):
        return {"choices": choices}
    if isinstance(choices, dict):
        returned_choices = []
        for choice_key in choices:
            returned_choices.append({
                "label": choice_key,
                "value": choices.get(choice_key)
            })
            
def do(payload, config, plugin_config, inputs):
    parameter_name = payload.get('parameterName')
    tables_url = config.get("tables_url")
    if not tables_url:
        return build_select_choices()
    choices = []
    if parameter_name == "table_to_extract":
        web_page, error_message = safe_get(tables_url)
        if error_message:
            return build_select_choices(error_message)
        dataframes = pandas.read_html(web_page, header=0)
        table_number = 0
        for dataframe in dataframes:
            number_of_rows = dataframe.shape[0]
            number_of_columns = dataframe.shape[1]
            column_names = dataframe.columns
            choices.append({
                "label": "{}x{} - {}".format(number_of_columns, number_of_rows, " | ".join(column_names)),
                "value": table_number
            })
            table_number += 1
    return build_select_choices(choices)
