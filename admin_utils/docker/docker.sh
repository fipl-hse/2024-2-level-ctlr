#!/bin/bash

export PYTHONPATH="/app:$PYTHONPATH"

apt update

python -m venv venv
source venv/bin/activate

python -m pip install --upgrade pip

pip install -r /app/requirements_qa.txt
pip install -r /app/requirements.txt

apt-get install aspell aspell-en aspell-ru
