set -ex

echo -e '\n'
echo "Check processing on student dataset"

export TARGET_SCORE=$(jq -r '.target_score' lab_6_pipeline/settings.json)

source venv/bin/activate
export PYTHONPATH=$(pwd):$PYTHONPATH

if [[ ${TARGET_SCORE} != 0 ]]; then
  python admin_utils/unpack_archived_dataset.py lab_6_pipeline
  python lab_6_pipeline/pipeline.py
  ls -la tmp/articles
else
  echo "Skip stage"
fi
