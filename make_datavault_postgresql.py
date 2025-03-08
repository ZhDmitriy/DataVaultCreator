from create_con_datavault_postgresql import ConfigurationPostgreSQL


class DataVaultModel:

    def __init__(self, 
                 host: str, 
                 database: str, 
                 user: str, 
                 password: str
                ):
        self.host = host
        self.database = database
        self.user = user 
        self.password = password
        self.con = ConfigurationPostgreSQL().make_postgresql_connection(
            host=self.host, database=self.database, user=self.user, password=self.password
        )

    def make_hub(self, schema_name: str, hub_name: str, surrogat_key: str, business_keys: dict):
        
        cursor = self.con.cursor()

        # parse business keys
        business_keys_create = []
        for bus_key in business_keys.keys(): 
            business_keys_create.append(str(bus_key) + ' ' + str(business_keys[bus_key]) + ' UNIQUE NOT NULL') # по определению Data Vault - содержаться уникальные бизнес-ключи

        # check exists hub table
        cursor.execute(f""" 
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = '{schema_name}';
        """)
        tables = ' '.join([str(table) for table in cursor.fetchall()])
        if 'hub_' + str(hub_name) in tables: 
            print(f"{'hub_' + str(hub_name)} exists in database and this schema")
        else: 
            # create hub table 
            columns = ", ".join(business_keys_create)
            try: 
                cursor.execute(f""" 
                    CREATE TABLE {schema_name}.hub_{hub_name} (
                        {surrogat_key} SERIAL PRIMARY KEY,
                        {columns}, 
                        source_id integer, 
                        load_dttm timestamp
                    )
                """)
                self.con.commit()
            except Exception as create_table_exception: 
                print(f"Wrong for create hub {hub_name}: ", create_table_exception)

    def make_satellite(self, schema_name: str, hub_name_surrogat_key: dict, satellite_name: str, surrogat_key: str, description_keys: str):
        """ 
            hub_name_surrogat_key: dict -> hub_name => hub_surrogat_key
        """
        
        cursor = self.con.cursor()

        # parse description keys
        description_keys_create = []
        for des_key in description_keys.keys(): 
            description_keys_create.append(str(des_key) + ' ' + str(description_keys[des_key]))

        # check exists sat table
        cursor.execute(f""" 
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = '{schema_name}';
        """)
        tables = ' '.join([str(table) for table in cursor.fetchall()])
        if 'sat_' + str(satellite_name) in tables: 
            print(f"{'sat_' + str(satellite_name)} exists in database and this schema")
        else:
            # create sattelit table 
            columns = ", ".join(description_keys_create)
            try: 
                cursor.execute(f""" 
                    CREATE TABLE {schema_name}.sat_{satellite_name} (
                        {surrogat_key} SERIAL PRIMARY KEY,
                        {columns}, 
                        source_id integer, 
                        load_dttm timestamp,
                        fk_{list(hub_name_surrogat_key.values())[0]} INT REFERENCES hub_{list(hub_name_surrogat_key.keys())[0]}({list(hub_name_surrogat_key.values())[0]})
                    )
                """)
                self.con.commit()
            except Exception as create_table_exception: 
                print(f"Wrong for create satellit {satellite_name}: ", create_table_exception)

    def make_link(self, schema_name: str, hub_name_surrogat_key_left: str, hub_name_surrogat_key_right: str, link_name: str, surrogat_key: str):
        """ 
            hub_name_surrogat_key_left: dict -> hub_name_left => hub_surrogat_key_left
            hub_name_surrogat_key_right: dict -> hub_name_right => hub_surrogat_key_right
        """
        
        cursor = self.con.cursor()

        # check exists hub table
        cursor.execute(f""" 
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = '{schema_name}';
        """)
        tables = ' '.join([str(table) for table in cursor.fetchall()])
        if 'l_' + str(link_name) in tables: 
            print(f"{'l_' + str(link_name)} exists in database and this schema")
        else:
            # create link table 
            try: 
                cursor.execute(f""" 
                    CREATE TABLE {schema_name}.l_{link_name} (
                        {surrogat_key} SERIAL PRIMARY KEY,
                        source_id integer, 
                        load_dttm timestamp,
                        fk_{list(hub_name_surrogat_key_left.values())[0]} INT REFERENCES hub_{list(hub_name_surrogat_key_left.keys())[0]}({list(hub_name_surrogat_key_left.values())[0]}),
                        fk_{list(hub_name_surrogat_key_right.values())[0]} INT REFERENCES hub_{list(hub_name_surrogat_key_right.keys())[0]}({list(hub_name_surrogat_key_right.values())[0]}
                        )
                    )
                """)
                self.con.commit()
            except Exception as create_table_exception:
                print(f"Wrong for create link {link_name}: ", create_table_exception)


if __name__ == "__main__": 
    dv_model = DataVaultModel(host="localhost", database="FactTable", user="newuser", password="postgres")
    dv_model.make_hub(schema_name="public",
                      hub_name="test", 
                      surrogat_key="test_id_1", 
                      business_keys={"test": "integer", 
                                     "test2": "varchar(250)", 
                                     "test3": "timestamp"})
    dv_model.make_hub(schema_name="public",
                      hub_name="test_test", 
                      surrogat_key="test_id_2", 
                      business_keys={"test": "integer", 
                                     "test2": "varchar(250)", 
                                     "test3": "timestamp"})
    dv_model.make_satellite(schema_name="public", 
                        hub_name_surrogat_key={"test": "test_id_1"}, 
                        satellite_name="test_sattelit", 
                        surrogat_key="test_sat_surogat_key", 
                        description_keys={"test": "integer", 
                                     "test2": "varchar(250)", 
                                     "test3": "timestamp"}
                        )
    dv_model.make_link(schema_name="public",
                      hub_name_surrogat_key_left={"test": "test_id_1"}, 
                      hub_name_surrogat_key_right={"test_test": "test_id_2"}, 
                      link_name="test",
                      surrogat_key="test_id"
                      )