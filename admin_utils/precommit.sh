set -ex

echo $1
if [[ "$1" == "smoke" ]]; then
  DIRS_TO_CHECK=(
    "config"
    "seminars"
    "admin_utils"
    "core_utils"
  )
else
  DIRS_TO_CHECK=(
    "config"
    "seminars"
    "admin_utils"
    "core_utils"
  )
fi

export PYTHONPATH=$(pwd)

python -m black "${DIRS_TO_CHECK[@]}"

python -m pylint "${DIRS_TO_CHECK[@]}"

mypy "${DIRS_TO_CHECK[@]}"

python config/static_checks/check_docstrings.py

python -m flake8 "${DIRS_TO_CHECK[@]}"

python config/static_checks/check_init.py

if [[ "$1" != "smoke" ]]; then
  python -m pytest -m "mark10 and lab_5_scrapper"
  python -m pytest -m "mark10 and lab_6_pipeline"
fi

