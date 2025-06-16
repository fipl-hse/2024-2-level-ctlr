"""
Final project implementation.
"""


# pylint: disable=unused-import
from pathlib import Path

from lab_6_pipeline.pipeline import UDPipeAnalyzer

BUNIN = Path(__file__).parent / "assets/bunin"
RAW_CONLLU = Path(__file__).parent / "dist" / "auto_annotated.conllu"

def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    all_txt = ""
    for text in BUNIN.glob("*.txt"):
        with open(text, "r", encoding="utf-8") as f:
            all_txt += f.read() + "\n"
    udpipe_analyzer = UDPipeAnalyzer()
    with open(RAW_CONLLU, "w", encoding="utf-8") as f:
        f.write(str(udpipe_analyzer.analyze([all_txt])[0]))
        f.write('\n')


if __name__ == "__main__":
    main()
