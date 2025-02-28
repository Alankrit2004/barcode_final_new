#!/bin/bash

/opt/render/project/src/.venv/bin/gunicorn -w 4 -b 0.0.0.0:8000 barcode_gen:app

