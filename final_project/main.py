"""
Final project implementation.
"""

import json
from collections import Counter

# pylint: disable=unused-import
from pathlib import Path

import spacy_udpipe
from spacy_conll.parser import ConllParser

from core_utils.constants import PROJECT_ROOT
from lab_6_pipeline.pipeline import UDPipeAnalyzer


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    project_path = Path(__file__).parent
    assets_path = project_path / "assets" / "cvetaeva"
    dist_path = project_path / "dist"
    conllu_path = dist_path / "auto_annotated.conllu"
    dist_path.mkdir(parents=True, exist_ok=True)
    data_path = project_path / "data"

    single_file = []

    for file_path in Path(assets_path).glob("cvet*.txt"):
        with open(file_path, 'r', encoding='utf-8') as input_file:
            single_file.append(input_file.read())

    file_to_analyze = ''.join(single_file)

    udpipe_analyzer = UDPipeAnalyzer()
    analyzed_file = udpipe_analyzer.analyze([file_to_analyze])

    with open(conllu_path, "w", encoding="utf-8") as annotation_file:
        annotation_file.write(analyzed_file[0])
        annotation_file.write('\n')


if __name__ == "__main__":
    main()
