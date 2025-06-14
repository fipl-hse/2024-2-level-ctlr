"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path
from typing import Union

from lab_6_pipeline.pipeline import UDPipeAnalyzer


def unite_texts(corpus: Union[Path, str], result: Union[Path, str]) -> None:
    """
    Combine all text files from a directory in one file

    Args:
        corpus: path to the directory of texts
        result: path to the final file
    """
    with open(result, "w", encoding="utf-8") as final_file:
        for text in corpus.iterdir():
            with open(text, "r", encoding="utf-8") as add_file:
                add = add_file.read()
                final_file.write(add+"\n\n")


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    ROOT = Path(__file__).parent
    path_mand = ROOT / "assets" / "mandelstamm"
    path_united = ROOT / "assets" / "mandl-all.txt"
    path_conllu = ROOT / "assets" / "mandl-auto_annotated.conllu"

    # Lock the following line if the file with all texts exists
    unite_texts(path_mand, path_united)

    udpipe_analyzer = UDPipeAnalyzer()
    with open(path_united, "r", encoding="utf-8") as read_file:
        auto_annotated = udpipe_analyzer.analyze([read_file.read()])
    # Lock the 2 following lines if the .conllu file exists
    with open(path_conllu, "w", encoding="utf-8") as conllu_file:
        conllu_file.write(auto_annotated[0])


if __name__ == "__main__":
    main()
