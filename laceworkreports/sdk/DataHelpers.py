import typing

import logging

from pandas import DataFrame


class ReferenceLookup:
    def __init__(
        self, key: str, field: typing.Any, dict: typing.Any, multivalue: bool = False
    ):
        self.key = key
        self.field = field
        self.dict = dict
        self.multivalue = multivalue

    def lookup(self, value: str, default: typing.Any = None) -> typing.Any:
        # only return the first matching result or default value
        dict = list(filter(lambda x: x[self.key] == value, self.dict))

        rows = []
        for row in dict:
            # return the entire row
            if self.field is None:
                rows.append(row)
            else:
                for i in self.field.split("."):
                    if i in row:
                        row = row[i]
                    else:
                        row = default
                    rows.append(row)

        # return all multiple values
        if self.multivalue:
            return rows

        # return first value only
        else:
            return rows.pop() if len(rows) > 0 else default


class DataHelpers:
    @staticmethod
    def dataframe_sql_columns(df: DataFrame, column_name: str) -> str:
        return_type: str = "TEXT"
        for i, j in zip(df.columns, df.dtypes):
            if "datetime" in str(j):
                if i == column_name:
                    return_type = "DATETIME"
                    break
            elif "float" in str(j):
                if i == column_name:
                    return_type = "FLOAT(3)"
                    break
            elif "int" in str(j):
                if i == column_name:
                    return_type = "INTEGER"
                    break
            elif "object" in str(j):
                if i == column_name:
                    return_type = "TEXT"
                    break
            else:
                logging.error(f"Failed to map column type {str(j)}")
                raise Exception(f"Failed to map column type {str(j)}")

        return return_type

    @staticmethod
    def dict_flatten(y: typing.Any) -> typing.Any:
        out = {}

        def flatten(x: typing.Any, name: typing.Any = "") -> typing.Any:
            if isinstance(x, dict):
                for a in x:
                    flatten(x[a], name + a + "_")
            elif isinstance(x, list):
                i = 0
                for a in x:

                    flatten(a, name + str(i) + "_")
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out

    @staticmethod
    def dict_lookup(
        key: str, dict: typing.Any, default: typing.Any = None
    ) -> typing.Any:
        for i in key.split("."):
            if i in dict:
                dict = dict[i]
            else:
                return default

        return dict

    @staticmethod
    def map_fields(data: typing.Any, field_map: typing.Any = None) -> typing.Any:
        if field_map is None:
            # flatten json
            # data = DataHelpers.dict_flatten(data)
            field_map = {}
            for key in data.keys():
                field_map[key] = key

        result = {}
        for field in field_map.keys():
            # for reference field find the matching local key and lookup the field value
            if isinstance(field_map[field], ReferenceLookup):
                result[field] = field_map[field].lookup(
                    DataHelpers.dict_lookup(field_map[field].key, data)
                )
            else:
                result[field] = DataHelpers.dict_lookup(field_map[field], data)

        return result
