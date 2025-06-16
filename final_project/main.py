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
    project_dir = Path(__file__).parent
    assets_dir = project_dir / "assets" / "axmatova"
    output_dir = project_dir / "dist"
    output_file = output_dir / "auto_annotated.conllu"

    output_dir.mkdir(parents=True, exist_ok=True)

    combined_text = ""
    for txt_file in sorted(assets_dir.glob("*.txt")):
        combined_text += txt_file.read_text(encoding="utf-8") + "\n"
    analyzer = UDPipeAnalyzer()
    conllu_data = analyzer.analyze([combined_text])[0]
    conllu_str = str(conllu_data)
    output_file.write_text(conllu_str, encoding="utf-8")

    token_freq = {}
    for line in conllu_str.split("\n"):
        if line.strip() and not line.startswith("#"):
            token = line.split("\t")[1]
            token_freq[token] = token_freq.get(token, 0) + 1

    with open(project_dir / "token_frequencies.json", "w", encoding="utf-8") as f:
        json.dump(
            sorted(token_freq.items(), key=lambda x: x[1], reverse=True),
            f,
            ensure_ascii=False,
            indent=2,)
        f.write("\n")


if __name__ == "__main__":
    main()
