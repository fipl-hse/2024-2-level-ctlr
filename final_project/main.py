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

    combined_txt = ""
    for txt in assets_dir.glob("*.txt"):
        with open(txt, "r", encoding="utf-8") as file:
            combined_txt += file.read() + "\n"
    analyzer = UDPipeAnalyzer()
    conllu_data = analyzer.analyze([combined_txt])[0]
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(str(conllu_data))

if __name__ == "__main__":
    main()
