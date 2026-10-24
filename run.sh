#!/bin/bash
cd /var/www/fakeweather
source nix_venv/bin/activate
exec uvicorn main:app --host 0.0.0.0 --port 8000

