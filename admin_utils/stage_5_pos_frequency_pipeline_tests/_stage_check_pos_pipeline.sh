set -ex

echo -e '\n'
echo "Check files processing on student dataset"

source venv/bin/activate
export PYTHONPATH=$(pwd):$PYTHONPATH

export TARGET_SCORE=$(jq -r '.target_score' lab_6_pipeline/settings.json)

if [[ ${TARGET_SCORE} != 0 ]]; then
  python admin_utils/unpack_archived_dataset.py lab_6_pipeline

  if [[ ${TARGET_SCORE} == 10 ]]; then
    python lab_6_pipeline/pos_frequency_pipeline.py
  fi

  echo "POSFrequencyPipeline is checked. Done"
  echo "Your solution is accepted! Proceed to further tasks from your lecturer."
else
  echo "Skip stage"
fi
