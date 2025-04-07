set -ex

source venv/bin/activate
export PYTHONPATH=$(pwd):$PYTHONPATH

export TARGET_SCORE=$(jq -r '.target_score' lab_5_scraper/settings.json)

if [[ ${TARGET_SCORE} == 0 ]]; then
  echo "Skip stage"
else
  python admin_utils/config_param_changer.py --config_path="lab_5_scraper/scraper_config.json"

  echo "Changed config params"

  python lab_5_scraper/scraper.py

  if [[ $? -ne 0 ]]; then
    echo "Check failed."
    exit 1
  fi

  echo "Check passed. Dataset collected."

  ls -la tmp/articles
fi
