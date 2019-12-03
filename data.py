import sqlite3


class RecordNotFoundError(RuntimeError):
    pass


class Database:

    def __init__(self, dbfile):
        self.con = sqlite3.connect(dbfile)
        self.con.row_factory = sqlite3.Row

    def create_table(self, dbobj_class, if_not_exists=True, recreate=False):
        if recreate:
            self.drop_table(dbobj_class)
        with self.con:
            self.con.execute(
                dbobj_class.generate_create_table(if_not_exists=if_not_exists))

    def drop_table(self, dbobj_class):
        with self.con:
            self.con.execute(f"DROP TABLE {dbobj_class.table_name}")

    def get(self, dbobj_class, **kwargs):
        if not kwargs:
            raise ValueError("Lookup parameters for .get() are required")
        sql = f"SELECT * FROM {dbobj_class.table_name} WHERE "
        clauses = " AND ".join([f"{field} = ?" for field in kwargs.keys()])
        sql += clauses
        with self.con:
            cursor = self.con.execute(sql, list(kwargs.values()))
            row = cursor.fetchone()
            if row is None:
                raise RecordNotFoundError(
                    f"No record for {dbobj_class.__name__} found with params {kwargs}"
                )
            return dbobj_class(**row)

    def select_sql(self, dbobj_class, where=None, clauses=None):
        sql = f"SELECT * FROM {dbobj_class.table_name} "
        where_params = []
        if type(where) is str:
            sql += f"WHERE {where} "
        elif where is not None:
            sql += f"WHERE {where[0]} "
            where_params = where[1:]

        if clauses:
            sql += clauses

        return sql, where_params

    def select(self, dbobj_class, where=None, clauses=None):
        sql, where_params = self.select_sql(dbobj_class, where, clauses)

        with self.con:
            cursor = self.con.execute(sql, where_params)
            return [dbobj_class(**row) for row in cursor.fetchall()]

    def save(self, dbobj):
        if dbobj.id:
            return self._update(dbobj)
        else:
            return self._create(dbobj)

    def _update(self, dbobj):
        sql = f"REPLACE INTO {dbobj.table_name} (id, {dbobj._fields_str()}) "
        sql += f"VALUES (?, {dbobj._placeholders_str()})"
        with self.con:
            self.con.execute(sql, [dbobj.id, *dbobj.values()])

    def _create(self, dbobj):
        sql = f"INSERT INTO {dbobj.table_name} ({dbobj._fields_str()}) "
        sql += f"VALUES ({dbobj._placeholders_str()})"
        with self.con:
            cursor = c.execute(sql, dbobj.values())
            dbobj.id = cursor.lastrowid


class Field:
    dbtype = "BLOB"

    def __init__(self, null=False, default=None, unique=False,
                 constraints=None):
        self.null = null
        self.default = default
        self.unique = unique
        self.constraints = constraints

    def to_ddl(self):
        ddl = self.dbtype
        if self.null:
            ddl += " NULL"
        else:
            ddl += " NOT NULL"

        if self.default:
            ddl += f" DEFAULT {repr(self.default)}"

        if self.unique:
            ddl += " UNIQUE"

        if self.constraints:
            ddl += f" {self.constraints}"

        return ddl


class TextField(Field):
    dbtype = "TEXT"


class IntField(Field):
    dbtype = "INTEGER"


class FloatField(Field):
    dbtype = "REAL"


class DateTimeField(Field):
    dbtype = "TEXT"


class DBObject:
    fields = {}

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        for field in self.fields.keys():
            setattr(self, field, kwargs.get(field))

    def _fields_str(self):
        return ', '.join(self.fields.keys())

    def _placeholders_str(self):
        return ', '.join(['?' for _ in self.field_names()])

    def field_names(self):
        return list(self.fields.keys())

    def values(self):
        return [getattr(self, field) for field in self.field_names()]

    def as_dict(self):
        return dict(zip(self.field_names(), self.values()))

    def __str__(self):
        output = f"<{self.__class__.__qualname__} "
        if self.id:
            output += f"id={self.id} "
        output += " ".join(f"{k}={v}" for k, v in self.as_dict().items())
        output += ">"
        return output

    @classmethod
    def generate_create_table(cls, if_not_exists=True):
        if not cls.table_name:
            raise RuntimeError("table_name must be specified!")
        ddl = "CREATE TABLE "
        if if_not_exists:
            ddl += "IF NOT EXISTS "
        ddl += f"{cls.table_name} ("
        cols = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for colname, coltype in cls.fields.items():
            cols.append(f"{colname} {coltype.to_ddl()}")
        ddl += ",\n".join(cols)

        ddl += ")"
        return ddl


class User(DBObject):
    fields = {'username': TextField(unique=True), 'password': TextField()}
    table_name = 'users'


class Page(DBObject):
    fields = {'title': TextField(unique=True), 'body': TextField()}
    table_name = 'pages'


class PageVersion(DBObject):
    fields = {
        'body': TextField(),
        'user_id': IntField(constraints="REFERENCES users(id)"),
        'saved_at': DateTimeField()
    }
    table_name = 'page_versions'
