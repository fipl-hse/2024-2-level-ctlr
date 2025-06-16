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
    combined_poems = []
    assets_dir = Path(__file__).parent / "assets"
    for poem_path in assets_dir.glob('*.txt'):
        with open(assets_dir / poem_path.name, encoding='utf-8') as file:
            combined_poems.append(file.read())

    combined_text = '\n\n\n'.join(combined_poems)

    udpipe_analyzer = UDPipeAnalyzer()
    conllu = udpipe_analyzer.analyze([combined_text])[0]
    conllu_dir = Path(__file__).parent / "dist"
    conllu_dir.mkdir()
    with open(conllu_dir / "auto_annotated.conllu", 'w', encoding='utf-8') as file:
        file.write(str(conllu))


if __name__ == "__main__":
    main()
