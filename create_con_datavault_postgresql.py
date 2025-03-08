import psycopg2


class ConfigurationPostgreSQL:

    def __init__(self):
        pass 

    def make_postgresql_connection(self, 
                                   host: str, 
                                   user: str, 
                                   password: str, 
                                   database: str
                                  ):
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    
    def type_postgresql(self) -> list: 
        types = ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 
                 'double', 'serial', 'bigserial', 'char', 'varchar', 'text', 
                 'bytea', 'boolean', 'date', 'time', 'timestamp', 'interval', 
                 'point', 'line', 'Iseg', 'box', 'path', 'polygon', 'circle', 
                 'inet', 'cidr', 'macaddr', 'json', 'jsonb', 'xml', 'uuid']
        return types
