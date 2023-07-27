import psycopg
import shutil
from os.path import exists
import yaml

if not exists("../config.yaml"):
    shutil.copy("../default.yaml", "config.yaml")
    print("Please edit config.yaml to represent your current configuration!")
    print("App will now quit.")
    exit(0)
else:
    with open("../config.yaml", 'r', encoding='utf-8') as in_file:
        yaml_content = in_file.read()
        cfg_items = yaml.load(yaml_content)


def create_db():
    try:
        db_conn = psycopg.connect(cfg_items["database"]["connection_str"])
        cursor = db_conn.cursor()

        cursor.execute("""
                create table weather_data
            (
                index_id           bigserial      not null primary key,
                localdatetime      timestamp(0)   not null,
                tempindoor         numeric(8, 2)  not null,
                humindoor          integer        not null,
                tempoutdoor        numeric(8, 2)  not null,
                humoutdoor         integer        not null,
                dewindoor          numeric(8, 2)  not null,
                dewoutdoor         numeric(8, 2)  not null,
                "WindChill"        numeric(8, 2)  not null,
                heatindex          numeric(8, 2)  not null,
                temphumidwindindex numeric(8, 2)  not null,
                barometer          numeric(10, 2) not null,
                windspd            numeric(8, 2)  not null,
                highwindspd        numeric(8, 2)  not null,
                winddirection      smallint       not null,
                avgwindspd         numeric(8, 2)  not null,
                avgwinddir         numeric(8, 2)  not null,
                rainrate           numeric(8, 2)  not null,
                raindaily          numeric(8, 2)  not null,
                solarrad           numeric(12, 2) not null,
                uvindex            numeric(8, 2)  not null,
                batterystate       varchar(32)    not null,
                heat               numeric(8, 2)  not null
            );
            
            alter table weather_data
                owner to postgres;
            CREATE INDEX "idx_ldt" ON "public"."weather_data" USING btree (
            "localdatetime");
            CREATE INDEX "idx_id" ON "public"."weather_data" USING btree (
            "index_id");
            
            grant delete, insert, references, select, trigger, truncate, update on weather_data to weatherman;
            grant ALL PRIVILEGES on ALL SEQUENCES IN SCHEMA public TO weatherman;
            """)
    except:
        print("DB Operation failed!")
        print("Please check if you have configured the database connection in config.yaml!")
