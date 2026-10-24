import uvicorn
import yaml
import shutil
from os.path import exists

if __name__ == "__main__":

    if exists("config.yaml"):
        with open("config.yaml", 'r', encoding='utf-8') as in_file:
            yaml_content = in_file.read()
            cfg_items = yaml.load(yaml_content, Loader=yaml.FullLoader)
    else:
        shutil.copy("default.yaml", "config.yaml")
        print("Please edit config.yaml to represent your current configuration!")
        print("App will now quit.")

    uvicorn.run("main:app", host=cfg_items["server"]["host"], port=)