"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path

from lab_6_pipeline.pipeline import UDPipeAnalyzer


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    project_dir = Path(__file__).parent

    assets_dir = project_dir / "assets/blok"
    output_dir = project_dir / "dist"
    output_file = output_dir / "auto_annotated.conllu"

    analyzer = UDPipeAnalyzer()
    all_conllu = []

    for txt in assets_dir.glob("*.txt"):
        with open(txt, "r", encoding="utf-8") as file:
            text = file.read().strip()

        conllu_data = analyzer.analyze([text])[0]
        all_conllu.append(conllu_data)

    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("\n\n".join(all_conllu))

if __name__ == "__main__":
    main()
