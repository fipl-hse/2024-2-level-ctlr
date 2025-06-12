"""
Final project implementation.
"""

# pylint: disable=unused-import
import json
from pathlib import Path
from lab_6_pipeline.pipeline import UDPipeAnalyzer


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    base_dir = Path(__file__).parent.parent
    input_dir = base_dir / "final_project" / "assets" / "axmatova"
    output_dir = base_dir / "final_project" / "dist"
    output_file = output_dir / "auto_annotated.conllu"

    texts = [txt.read_text(encoding="utf-8") for txt in sorted(input_dir.glob("*.txt"))]
    combined_text = "\n".join(texts) + ("\n" if texts else "")
    analyzed_data = UDPipeAnalyzer().analyze([combined_text])[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file.write_text(analyzed_data, encoding="utf-8")

    with open(output_file, "r", encoding="utf-8") as file:
        conllu_data = file.read()
    token_freq = {}
    for line in conllu_data.split('\n'):
        if line.strip() and not line.startswith('#'):
            token = line.split('\t')[1]
            token_freq[token] = token_freq.get(token, 0) + 1

    with open(base_dir / "final_project" / "token_frequencies.json", "w", encoding="utf-8") as f:
        json.dump(sorted(token_freq.items(),
                         key=lambda x: x[1], reverse=False), f, ensure_ascii=False, indent=2)
        f.write('\n')


if __name__ == "__main__":
    main()
