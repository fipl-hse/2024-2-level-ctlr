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
    project_path = Path(__file__).parent
    assets_path = project_path / "assets/silverage"
    output_path = project_path / "dist"
    output_file = output_path / "auto_annotated.conllu"
    output_path.mkdir(parents=True, exist_ok=True)
    texts = ""

    for entry in assets_path.iterdir():
        if entry.is_file() and entry.suffix == ".txt":
            with open(entry, "r", encoding="utf-8") as file:
                texts += file.read() + "\n"

    analyzer = UDPipeAnalyzer()
    conllu = analyzer.analyze([texts])[0]

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(conllu)
        file.write("\n")


if __name__ == "__main__":
    main()
