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

    with open(PROJECT_ROOT / "final_project" / "assets" / "output-file.txt", 'w',
              encoding='utf-8') as output_file:
        for file_path in Path(PROJECT_ROOT / "final_project" / "assets").glob("cvet*.txt"):
            with open(file_path, 'r', encoding='utf-8') as input_file:
                output_file.write(input_file.read() + '\n')

    with open(PROJECT_ROOT / "final_project" / "assets" / "output-file.txt", 'r',
              encoding='utf-8') as file:
        file_to_analyze = file.read()

    udpipe_analyzer = UDPipeAnalyzer()
    analyzed_file = udpipe_analyzer.analyze([file_to_analyze])
    print(analyzed_file)

    base_path = Path(PROJECT_ROOT / "final_project" / "dist")
    base_path.mkdir(parents=True, exist_ok=True)

    with open(base_path / "auto_annotated.conllu", "w", encoding="utf-8") as annotation_file:
        annotation_file.write('\n'.join([str(elem) for elem in analyzed_file]))
        annotation_file.write("\n")

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

    frequency = dict(sorted(Counter([token.text.lower() for token in parsed_doc
                                     if token.pos_ != 'PUNCT']).items(), key=lambda x: x[1]))
    with open(PROJECT_ROOT / "final_project" / "data" / "frequencies.json", 'w',
              encoding='utf-8') as freq_file:
        json.dump(frequency, freq_file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
