"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path
import pyconll
import shutil
import json
from lab_6_pipeline.pipeline import UDPipeAnalyzer
from core_utils.constants import PROJECT_ROOT

class Text_Modifyer:

    def __init__(self, path: Path):
        self.path = path
        self.txt = ''
        self.path_to_corpus = None
        self._analyser = UDPipeAnalyzer()

    def text_join(self) -> None:
        corpus_text = ''
        for file in self.path.iterdir():
            if str(file.stem).startswith('cern'):
                f = open(file, 'r', encoding='UTF-8')
                file_text = f.read()
                corpus_text += f'\n\n {file_text}'
        self.txt = corpus_text

    def save_text(self) -> None:
        self.path_to_corpus = self.path / "corpus.txt"
        f = open(self.path_to_corpus, 'w', encoding="UTF-8")
        f.write(self.txt)

    def conllu_analysis(self, path_to_save: Path) -> None:
        with open(self.path_to_corpus, 'r', encoding="UTF-8") as f:
            texts = [f.read()]
            conllu_data = self._analyser.analyze(texts=texts)
        final_path = path_to_save / f'auto_annotated.conllu'
        conllu_file = open(final_path, 'w', encoding="UTF-8")
        conllu_file.write('\n'.join(conllu_data))
        conllu_file.write('\n')
        conllu_file.close()

    def connlu_freq(self, file_path: Path) -> dict:
        freq = {}
        true_path = file_path / "auto_annotated.conllu"
        corpus = pyconll.load_from_file(true_path)
        for sentence in corpus:
            for token in sentence:
                lemma = str(token.lemma)
                if lemma not in list(freq.keys()):
                    freq[lemma] = 1
                else:
                    freq[lemma] += 1
        freq = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        freq_new = {line[0]: line[1] for line in freq}
        return freq_new


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    path = PROJECT_ROOT / "final_project" / "assets" / "cerny"
    text_modifyer = Text_Modifyer(path)
    text_modifyer.text_join()
    text_modifyer.save_text()
    path_to_conllu = PROJECT_ROOT / "final_project" / "dist"
    if path_to_conllu.exists():
        shutil.rmtree(path_to_conllu)
    path_to_conllu.mkdir()
    text_modifyer.conllu_analysis(path_to_conllu)
    frequency_dictionary = text_modifyer.connlu_freq(path_to_conllu)
    path_to_freq = PROJECT_ROOT / "final_project" / "data" / "table_work" / "frequency_dictionary.json"
    with open(path_to_freq, 'w', encoding="UTF-8") as f:
        json.dump(frequency_dictionary, f, indent=4, ensure_ascii=False, separators=(",", ": "))
    print("Done!")


if __name__ == "__main__":
    main()
