# Personal Weather Station Site

## Summary

This is a replacement for the weathercloud online service. You can set up locally. 

This website is not for commercial use.

I used fastapi/uvicorn to run the backend, vue.js/echarts-vue/echarts.js/bootstrap to run the front end.
There are some fonts included. They are not proposed for any commercial use. 
You may want to replace them if you don't want to use them.

## How to use

- You need an internal DNS server to intercept the API calls going out to weathercloud.net
- You need a server with python and postgresql installed.
- Windows instructions are not included but it can work. 

### For Ubuntu 20.04 dependency install:
```shell
apt install python3.8 python3-pip postgresql libpq-dev
update-alternatives --install /usr/bin/python python /usr/bin/python2.7 10 # if you have python 2.7
update-alternatives --install /usr/bin/python python /usr/bin/python3.6 20 # if you have python 3.6
update-alternatives --install /usr/bin/python python /usr/bin/python3.8 30
update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 10
update-alternatives --config python  # and then select python 3.8
```

## Python packages install
```shell
pip install -r requirements.txt
```

## Database setup
- Configure a database role in `config.yaml` with the required table permissions.
- For a new database only, run `python create_db.py` to create the storage table.

## Run the site
You have 2 options to run the website:
1. run `./run.sh` and it will listen on 0.0.0.0 at port 8000 (behind nginx).
2. run `python main.py` and it should pull in configurations from config.yaml. 

## Recovery and validation

Database failures return HTTP 503 and save the normalized record to
`var/failed-records/`. See [recovery instructions](docs/recovery.md) for listing
and replaying individual records, permissions, and duplicate handling.

Copy `default.yaml` to `config.yaml` and configure it before starting. Keep
`config.yaml`, recovery files, and local backups out of Git. The station's
existing GET parameter names, integer scaling, sentinel filtering, and
server-generated observation timestamps are preserved.

For regression tests, install `httpx` in a development environment, then run
`python -m unittest discover -s tests -v`. Tests do not access the live database.
The explicit `python tests/integration_postgres.py` test requires database schema
creation permission and creates and removes only an isolated temporary schema.

See [review findings](docs/review.md) for the failure diagnosis and validation.

## Other tasks to do

- ~~Not support switching between units. For example, Celsius to Fahrenheit or m/s to km/h to knots.~~
- Now supported switching temperature units in detaildata.html.
- May need more language support
- Add humidity curve.
