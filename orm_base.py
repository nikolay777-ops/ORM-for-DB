import psycopg2
from db_connection_entities import DB_SETTINGS


class BaseManager:
    connection = None

    def __init__(self, model_class):
        self.model_class = model_class

    @classmethod
    def set_connection(cls, database_settings):
        connection = psycopg2.connect(**database_settings)
        connection.autocommit = True
        cls.connection = connection
        # return psycopg2.connect(**database_settings)

    @classmethod
    def _get_cursor(cls):
        cls.set_connection(DB_SETTINGS)
        return cls.connection.cursor()
        # cls.set_connection(DB_SETTINGS)
        # return cls.connection.cursor()

    @classmethod
    def _execute_query(cls, query, params=None):
        cursor = cls._get_cursor()
        cursor.execute(query, params)

    def select(self, *field_names, distinct=False, chunk_size=2000, where=None, orderby=None, join=None):
        fields_format = ", ".join(field_names)
        wh = None

        if distinct is True:
            query = f"SELECT DISTINCT {fields_format} FROM {self.model_class.table_name}"
        else:
            query = f"SELECT {fields_format} FROM {self.model_class.table_name} "

        if join is not None:
            query += self.join_configure(self.model_class.table_name, join)

        if where is not None:
            wh = self.where_configure(where)
            query += wh[0]

        if orderby is not None:
            query += self.orderby_configure(orderby)

        cursor = self._get_cursor()

        if wh is not None:
            cursor.execute(query, wh[1])
        else:
            cursor.execute(query)
        # self._execute_query(query, wh[1])

        is_fetching_completed = False
        model_objects = list()

        while not is_fetching_completed:
            result = cursor.fetchmany(size=chunk_size)

            for row_values in result:
                keys, values = field_names, row_values
                row_data = dict(zip(keys, values))
                model_objects.append(self.model_class(**row_data))

            is_fetching_completed = len(result) < chunk_size

        return model_objects

    def insert(self, rows: list):
        field_names = rows[0].keys()
        assert all(row.keys() == field_names for row in rows[1:])

        fields_format = ", ".join(field_names)
        values_placeholder = ", ".join([f'({", ".join(["%s"] * len(field_names))})'] * len(rows))

        query = f"INSERT INTO {self.model_class.table_name} ({fields_format}) " \
                f"VALUES {values_placeholder}"

        params = list()
        for row in rows:
            row_values = [row[field_name] for field_name in field_names]
            params += row_values

        print(query, params)
        self._execute_query(query, params)

    def update(self, new_data: dict, where=None):
        field_names = new_data.keys()
        placeholder_format = ", ".join([f'{field_name} = %s' for field_name in field_names])
        query = f"UPDATE {self.model_class.table_name} SET {placeholder_format} "
        params = list(new_data.values())

        if where is not None:
            query += self.where_configure(where)

        self._execute_query(query, params)

    def delete(self):
        query = f'DELETE FROM {self.model_class.table_name}'
        self._execute_query(query)

    # dict with tuple value this values make as table1.value1
    # AND/OR/NOT STATE1 AND STATE2  STATE1 OR STATE2  NOT STATE

    # where_configure(where=None)
    # where_configure(where={'rating': 10000}) ---> SELECT * FROM film WHERE rating=10000

    # where_configure({"AND": (state1{'>': (rating, 2)}, state2{'<': (year, 1999)})})
    # where_configure({"OR": (state1{'>': (rating, 2)}, state2{'<': (year, 1999)})})
    # where_configure({"NOT": {'=': (rating, 1)})
    # where_configure({"": {'=': (rating, 1}})

    def where_configure(self, parameters: dict) -> tuple:
        operator = list(parameters.keys())[0]
        print(f'OPERATOR: {operator}')
        expression = list(parameters.values())[0]
        where_query = f"WHERE "
        prepare_list = list()

        if operator == "":
            key = list(expression.keys())[0]
            values = list(expression.values())[0]
            where_query += f'{values[0]} {key} %s'
            prepare_list.append(values[1])

        elif operator != "NOT":
            for state in expression:
                key = list(state.keys())[0]
                values = list(state.values())[0]
                where_query += f"{values[0]} {key} %s {operator} "
                prepare_list.append(values[1])
            where_query = where_query[:len(where_query) - len(operator) - 1]

        else:
            key = list(expression.keys())[0]
            values = list(expression.values())[0]
            where_query += f'{operator} {values[0]} {key} %s'
            prepare_list.append(values[1])
        return where_query, prepare_list

    def orderby_configure(self, parameters: list) -> str:
        query = "ORDER BY "

        for param in parameters:
            if isinstance(param, str):
                query += f'{param}, '
            elif isinstance(param, tuple):
                query += f'{param[0]} {param[1]}'

        return query

    def groupby(self, group, *field_without_agg, chunk_size=2000, orderby=None, join=None, **aggregs):
        query = f"SELECT "
        field_names = list(aggregs.values())
        field_names.append(group)
        for field in field_without_agg:
            field_names.append(field)

        for key, value in aggregs.items():
            query += f'{key}({value}), '

        for field in field_without_agg:
            query += f'{field}, '

        query += f'{group} FROM {self.model_class.table_name} '

        if join is not None:
            query += self.join_configure(self.model_class.table_name, join)

        query += f" GROUP BY {group} "

        if orderby is not None:
            query += self.orderby_configure(orderby)
        cursor = self._get_cursor()
        cursor.execute(query)

        is_fetching_completed = False
        groupby_list = list()

        while not is_fetching_completed:
            result = cursor.fetchmany(size=chunk_size)

            for row_values in result:
                keys, values = field_names, row_values
                row_data = dict(zip(keys, values))
                groupby_list.append(self.model_class(**row_data))

            is_fetching_completed = len(result) < chunk_size

        return groupby_list

    # SELECT * FROM tab1 INNER/LEFT/RIGHT/FULL join tab2 ON tab1.prop = tab2.prop
    # [{join1: ({tab2: prop2}, prop1)}, {join2: (), join3: ()}]

    @staticmethod
    def join_configure(tab1: str, *args):
        query = ""

        for list in args:
            for dict in list:
                for key, value in dict.items():
                    for tkey, tvalue in value[0].items():
                        query += f'{key} JOIN {tkey} ON {tab1}.{value[1]} = {tkey}.{tvalue} '
                        tab1 = tkey

        return query


class MetaModel(type):
    manager_class = BaseManager

    def _get_manager(cls):
        return cls.manager_class(model_class=cls)

    @property
    def objects(cls):
        return cls._get_manager()


class BaseModel(metaclass=MetaModel):
    table_name = ""

    def __init__(self, **row_data):
        for field_name, value in row_data.items():
            setattr(self, field_name, value)

    def __repr__(self):
        attrs_format = ", ".join([f'{field}={value}' for field, value in self.__dict__.items()])
        return f"<{self.__class__.__name__}: {attrs_format}>"
