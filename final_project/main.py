"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path
from lab_6_pipeline.pipeline import UDPipeAnalyzer
from collections import defaultdict
from spacy_conll.parser import ConllParser
from core_utils.constants import PROJECT_ROOT
import spacy_udpipe
import json


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
    with open(conllu_dir / "auto_annotated.conllu", 'w', encoding='utf-8') \
            as file:
        file.write(conllu)

    model_path = Path(PROJECT_ROOT) / "lab_6_pipeline" / \
                 "assets" / "model" / "russian-syntagrus-ud-2.0-170801.udpipe"
    model = spacy_udpipe.load_from_path(
        lang='ru',
        path=str(model_path)
    )
    model.add_pipe(
        "conll_formatter",
        last=True,
        config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True},
    )

    conllu_doc = ConllParser(model).parse_conll_text_as_spacy(conllu.strip('\n'))
    lemmas_frequencies = defaultdict(int)
    for token in conllu_doc:
        lemmas_frequencies[token.text.lower()] += 1
    lemmas_frequencies_sorted = {k: v for k, v in sorted(lemmas_frequencies.items(),
                                                         key=lambda item: item[1], reverse=True)}
    with open(Path(__file__).parent / "data" / "tokens_frequencies.json", 'w',
              encoding='utf-8') \
            as file:
        json.dump(lemmas_frequencies_sorted, file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
