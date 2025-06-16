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
    conllu_path.write_text(analyzed_file[0], encoding="utf-8")

    model = spacy_udpipe.load_from_path(lang="ru",
                                        path=str(PROJECT_ROOT / "lab_6_pipeline" / "assets" /
                                                 "model" /
                                                 "russian-syntagrus-ud-2.0-170801.udpipe"))
    model.add_pipe(
        "conll_formatter",
        last=True,
        config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True},
    )

    parsed_doc = ConllParser(model).parse_conll_text_as_spacy(analyzed_file[0].strip('\n'))

    tokens_frequency = dict(sorted(Counter([token.text.lower() for token in parsed_doc]).items(),
                            key=lambda x: x[1]))

    with open(data_path / "frequencies.json", 'w', encoding='utf-8') as freq_file:
        json.dump(tokens_frequency, freq_file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
