"""
Final project implementation.
"""

import shutil

# pylint: disable=unused-import
from pathlib import Path

from core_utils.constants import PROJECT_ROOT
from lab_6_pipeline.pipeline import UDPipeAnalyzer


class TextModifyer:
    """
    Text Modifyer instance
    """
    def __init__(self, path: Path):
        self.path = path
        self.txt = ''
        self.path_to_corpus = self.path / "corpus.txt"
        self._analyser = UDPipeAnalyzer()

    def text_join(self) -> None:
        """

        Returns: None

        """
        corpus_text = ''
        for file in self.path.iterdir():
            if str(file.stem).startswith('cern'):
                with open(file, 'r', encoding='UTF-8') as f:
                    file_text = f.read()
                    corpus_text += f'\n\n {file_text}'
        self.txt = corpus_text

    def save_text(self) -> None:
        """

        Returns: None

        """
        with open(self.path_to_corpus, 'w', encoding="UTF-8") as f:
            f.write(self.txt)

    def conllu_analysis(self, path_to_save: Path) -> None:
        """

        Args:
            path_to_save: Path

        Returns: None

        """
        with open(self.path_to_corpus, 'r', encoding="UTF-8") as f:
            texts = [f.read()]
            conllu_data = self._analyser.analyze(texts=texts)
        final_path = path_to_save / 'auto_annotated.conllu'
        with open(final_path, 'w', encoding="UTF-8") as conllu_file:
            list_data = list(map(str, conllu_data))
            conllu_file.write('\n'.join(list_data))
            conllu_file.write('\n')
            conllu_file.close()

def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    path = PROJECT_ROOT / "final_project" / "assets" / "cerny"
    text_modifyer = TextModifyer(path)
    text_modifyer.text_join()
    text_modifyer.save_text()
    path_to_conllu = PROJECT_ROOT / "final_project" / "dist"
    if path_to_conllu.exists():
        shutil.rmtree(path_to_conllu)
    path_to_conllu.mkdir()
    text_modifyer.conllu_analysis(path_to_conllu)
    print("Done!")


if __name__ == "__main__":
    main()
