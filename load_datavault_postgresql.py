from create_con_datavault_postgresql import ConfigurationPostgreSQL
from typing import List, Dict, NoReturn
from datetime import datetime
import pandas as pd


class LoadDataVaultModelPostgreSQL: 

    def __init__(self, 
                host: str, 
                database: str, 
                user: str, 
                password: str, 
                schema_name: str
            ):
        self.host = host
        self.database = database
        self.user = user 
        self.schema_name = schema_name
        self.password = password
        self.con = ConfigurationPostgreSQL().make_postgresql_connection(
            host=self.host, database=self.database, user=self.user, password=self.password
        ) 


    def load_hub(self, surrogat_key: str, hub_name: str, business_keys: dict, source_id: int, load_dttm: datetime):
        """
            list[ dict([SURROGAT_KEY] => BUSINESS/NATURAL KEY) ]
        """
        
        cursor = self.con.cursor()
        columns = ",".join(business_keys.keys()) + f', source_id' + f', load_dttm'

        values_tmp = list(business_keys.values()) 
        values_tmp.append(source_id)
        values_tmp.append(load_dttm)
        values = tuple(values_tmp)
        cursor.execute(f""" 
            INSERT INTO {self.schema_name}.hub_{hub_name}({columns})
            VALUES {values}
            ON CONFLICT ({columns.split(",")[0]}) DO NOTHING
            RETURNING {surrogat_key};
        """)
        self.con.commit()


    def load_satellite(self, hub_name: str, satellite_name: str, hub_name_surrogat_key: str, business_key_hub: dict, description_value: dict, source_id: int, load_dttm: datetime) -> NoReturn: 

        cursor = self.con.cursor()

        sql_variable = []
        count_variable = 0

        for key in list(business_key_hub.keys()):
            if count_variable == 0:
                if isinstance(business_key_hub[key], str):
                    sql_variable.append("WHERE {field} = '{value}'".format(field=key, value=business_key_hub[key]))
                else: 
                    sql_variable.append("WHERE {field} = {value}".format(field=key, value=business_key_hub[key]))
                count_variable += 1
            else: 
                if isinstance(business_key_hub[key], str):
                    sql_variable.append("{field} = '{value}'".format(field=key, value=business_key_hub[key]))
                else: 
                    sql_variable.append("WHERE {field} = {value}".format(field=key, value=business_key_hub[key]))

        try: 
            cursor.execute(f""" 
                SELECT "{hub_name_surrogat_key}"
                FROM {self.schema_name}.hub_{hub_name}
                {' AND '.join(sql_variable)}
            """)
            print(f""" 
                SELECT "{hub_name_surrogat_key}"
                FROM {self.schema_name}.hub_{hub_name}
                {' AND '.join(sql_variable)}
            """)
            fk_hub_to_sat = cursor.fetchall()[0][0]
            print(fk_hub_to_sat)
        except Exception as e: 
            print("Error - Not Found Foreign Key for Satellite - ", e)

        columns = ",".join(description_value.keys()) + f', source_id' + f', load_dttm' + f', fk_{hub_name_surrogat_key}'

        values_tmp = list(description_value.values()) 
        values_tmp.append(source_id)
        values_tmp.append(load_dttm)
        values_tmp.append(fk_hub_to_sat)
        values = tuple(values_tmp)

        cursor.execute(f""" 
            INSERT INTO {self.schema_name}.sat_{satellite_name}({columns})
            VALUES {values}
        """)
        self.con.commit()


    def load_link(self): 
        pass 


if __name__ == '__main__': 
    dv_model_load = LoadDataVaultModelPostgreSQL(host="localhost", database="FactTable", user="newuser", password="postgres", schema_name='public')

    dv_model_load.load_hub(
            surrogat_key="test_id_1", 
            hub_name="test", 
            business_keys={
                "test": 1, 
                "test2": "test_1", 
                "test3": datetime.now().strftime('%Y-%m-%d')}, 
            source_id=120, 
            load_dttm=datetime.now().strftime('%Y-%m-%d'))
    
    dv_model_load.load_satellite(
        hub_name="test", 
        satellite_name="test_sattelit", 
        hub_name_surrogat_key="test_id_1", 
        business_key_hub={"test": 1, "test2": "test_1"}, 
        description_value={"test": 123, "test2": "test", "test3": datetime.now().strftime('%Y-%m-%d')},
        source_id=120, 
        load_dttm=datetime.now().strftime('%Y-%m-%d')
    )