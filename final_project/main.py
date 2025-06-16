"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path
from typing import Union

from lab_6_pipeline.pipeline import UDPipeAnalyzer


def unite_texts(corpus: Union[Path, str], result: Union[Path, str]) -> str:
    """
    Combine all text files from a directory in one file

    Args:
        corpus: path to the directory of texts
        result: path to the final file
    Returns:
        str: the text written to the file
    """
    to_write = ""
    for text in corpus.iterdir():
        with open(text, "r", encoding="utf-8") as add_file:
            to_write += add_file.read() + "\n\n"
    with open(result, "w", encoding="utf-8") as final_file:
        final_file.write(to_write)
    return to_write


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    root = Path(__file__).parent
    path_mand = root / "assets" / "mandelstamm"
    path_united = root / "assets" / "mandl-all.txt"
    path_conllu = root / "dist" / "auto_annotated.conllu"

    # Lock the following line if the file with all texts exists
    united = unite_texts(path_mand, path_united)

    # Lock the 2 following lines if the file with all texts needs to be rewritten
    # with open(path_united, "r", encoding="utf-8") as read_file:
    #     united = read_file.read()

    udpipe_analyzer = UDPipeAnalyzer()
    auto_annotated = str(udpipe_analyzer.analyze([united])[0])

    # Lock the 2 following lines if the .conllu file exists
    with open(path_conllu, "w", encoding="utf-8") as conllu_file:
        conllu_file.write(auto_annotated + "\n")


if __name__ == "__main__":
    main()
