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
    root = Path(__file__).parent
    assets_path = root / 'assets' / 'pasternak'
    dist_path = root / 'dist'
    file_path = dist_path / 'auto_annotated.conllu'
    dist_path.mkdir(parents=True, exist_ok=True)
    text = ''
    for file in sorted(assets_path.iterdir()):
        if file.suffix == '.txt':
            with open(file, 'r', encoding='utf-8') as f:
                text += f.read()
                text += '\n'
    analyzer = UDPipeAnalyzer()
    analyzed = analyzer.analyze([text])

    with open(str(file_path), 'w', encoding='utf-8') as file:
        file.write(analyzed[0])
        file.write('\n')


if __name__ == "__main__":
    main()
