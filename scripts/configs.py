import shutil
from os.path import exists

import yaml
import yaml.loader as ldr


class Config:
    def __init__(self):
        if exists("config.yaml"):
            with open("config.yaml", 'r', encoding='utf-8') as self.in_file:
                self.yaml_content = self.in_file.read()
                self.cfg = yaml.safe_load(self.yaml_content)  # Use safe_load for a single document
                self.init_ok = True
        else:
            shutil.copy("default.yaml", "config.yaml")
            print("Please edit config.yaml to represent your current configuration!")
            print("App will now quit.")
            self.init_ok = False


GlobalConfig: Config = Config()


def check_config():
    if not exists("config.yaml"):
        shutil.copy("default.yaml", "config.yaml")
        print("Please edit config.yaml to represent your current configuration!")
        print("App will now quit.")
        exit(0)
    else:
        with open("config.yaml", 'r', encoding='utf-8') as in_file:
            yaml_content = in_file.read()
            cfg_items = yaml.load(yaml_content)


legendName = [
    "0.0-0.2 m/s",
    "0.3-1.5 m/s",
    "1.6-3.3 m/s",
    "3.4-5.4 m/s",
    "5.5-7.9 m/s",
    "8.0-10.7 m/s",
    "10.8-13.8 m/s",
    "13.9-17.1 m/s",
    "17.2-20.7 m/s",
    "20.8-24.4 m/s",
    "24.5-28.4 m/s",
    "28.5-32.6 m/s",
    ">32.6 m/s"
]

solar_window_size = 15
rain_window_size = 60
barometer_window_size = 30
