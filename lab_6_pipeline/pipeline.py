"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib

from networkx import DiGraph
from string import punctuation
from pathlib import Path
from typing import List
import spacy_udpipe
from typing import cast
from spacy.tokens import Doc
Doc.set_extension("conll_str", getter=lambda doc: "")

from core_utils.constants import UDPIPE_MODEL_PATH
from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import to_cleaned
from core_utils.pipeline import (
    AbstractCoNLLUAnalyzer,
    CoNLLUDocument,
    LibraryWrapper,
    PipelineProtocol,
    StanzaDocument,
    TreeNode,
    UDPipeDocument,
    UnifiedCoNLLUDocument,
)

class InconsistentDatasetError(Exception):
    """
    Is raised when IDs contain slips, number of meta and raw files is not equal, files are empty
    """

class EmptyDirectoryError(Exception):
    """
    Is raised when directory is empty
    """

class EmptyFileError(Exception):
    """
    Is raised when an article file is empty
    """

class CorpusManager:
    """
    Work with articles and store them.
    """

    def __init__(self, path_to_raw_txt_data: pathlib.Path) -> None:
        """
        Initialize an instance of the CorpusManager class.

        Args:
            path_to_raw_txt_data (pathlib.Path): Path to raw txt data
        """
        self._storage = {}
        self.path_to_raw_txt_data = path_to_raw_txt_data
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path_to_raw_txt_data.exists():
            raise FileNotFoundError("File does not exist")

        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError("Path does not lead to directory")

        if not any(self.path_to_raw_txt_data.iterdir()):
            raise EmptyDirectoryError("Directory is empty")

        raws: List[Path] = [doc for doc in self.path_to_raw_txt_data.glob("*_raw.txt")]
        metas: List[Path] = [doc for doc in self.path_to_raw_txt_data.glob("*_meta.json")]

        raw, meta = [], []

        for f in raws:
            try:
                n_id = int(f.name.split("_")[0])
                raw.append(n_id)
            except (ValueError, IndexError):
                continue
            if f.stat().st_size == 0:
                raise InconsistentDatasetError("File is empty")

        for f in metas:
            try:
                n_id = int(f.name.split("_")[0])
                meta.append(n_id)
            except (ValueError, IndexError):
                continue
            if f.stat().st_size == 0:
                raise InconsistentDatasetError("Meta file is empty")

        if len(raw) != len(meta):
            raise InconsistentDatasetError(
                f"The amounts of meta and raw files are not equal: {len(meta)} != {len(raw)}")

        raw.sort()
        meta.sort()

        gaps = False
        for elem in range(1, len(raw)):
            if raw[elem] != raw[elem - 1] + 1:
                gaps = True
        if gaps:
            raise InconsistentDatasetError("Dataset has slips")

        gaps = False
        for elem in range(1, len(meta)):
            if meta[elem] != meta[elem - 1] + 1:
                gaps = True
        if gaps:
            raise InconsistentDatasetError("Dataset has slips")

        if sorted(raw) != list(range(1, len(raw) + 1)):
            raise InconsistentDatasetError("Number of raw files is not equal")

        if sorted(meta) != list(range(1, len(meta) + 1)):
            raise InconsistentDatasetError("Number of meta files is not equal")

        if raw != meta:
            raise InconsistentDatasetError("IDs of raw and meta files do not match")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for file in self.path_to_raw_txt_data.glob("*_raw.txt"):
            n_id = int(file.name.split("_")[0])

            if not file.name.endswith("_raw.txt"):
                continue

            with open(file, encoding="utf-8") as f:
                raw_txt = f.read()

            article = Article(url=None, article_id=n_id)

            article.text = raw_txt

            self._storage[n_id] = article

    def get_articles(self) -> dict:
        """
        Get storage params.

        Returns:
            dict: Storage params
        """
        return self._storage


class TextProcessingPipeline(PipelineProtocol):
    """
    Preprocess and morphologically annotate sentences into the CONLL-U format.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper | None = None
    ) -> None:
        """
        Initialize an instance of the TextProcessingPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper | None): Analyzer instance
        """
        self._corpus = corpus_manager
        self._analyzer =  analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        for i, val in self._corpus.get_articles().items():
            val.text = val.text.lower()
            for char in punctuation:
                val.text = val.text.replace(char, "")
            val.text = val.text.replace("NBSP", "")
            to_cleaned(val)



class UDPipeAnalyzer(LibraryWrapper):
    """
    Wrapper for udpipe library.
    """

    #: Analyzer
    _analyzer: AbstractCoNLLUAnalyzer

    def __init__(self) -> None:
        """
        Initialize an instance of the UDPipeAnalyzer class.
        """
        super().__init__()
        self._analyzer = self._bootstrap()

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the UDPipe model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """
        if not Path(UDPIPE_MODEL_PATH).exists():
            raise FileNotFoundError("UDpipe model is not found in expected place")

        nlp = spacy_udpipe.load_from_path(lang="ru", path=str(UDPIPE_MODEL_PATH))
        nlp.add_pipe("conll_formatter", last=True)

        return cast(AbstractCoNLLUAnalyzer, nlp)


    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """
        # results = []
        # for text in texts:
        #     processed_text: Doc = cast(Doc, self._analyzer(text))
        #     results.append(processed_text._.conll_str)
        # return results



        # return [cast(Doc, self._analyzer(text))._.conll_str for text in texts]

        results = []
        for i, text in enumerate(texts, start=1):
            doc = cast(Doc, self._analyzer(text))
            conllu_lines = [f"# sent_id = {i}", f"# text = {text}"]
            conllu_lines.append(doc._.conll_str)
            results.append("\n".join(conllu_lines))
        return results

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        conllu_inf = article.get_conllu_info()

        if not conllu_inf:
            raise ValueError("ConLLU not found")

        path = article.get_file_path(kind=ArtifactType.UDPIPE_CONLLU)

        with open(path, "w", encoding="utf-8") as f:
            f.write(str(path))


    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        path = article.get_file_path(kind=ArtifactType.UDPIPE_CONLLU)

        with open(path, "r", encoding="utf-8") as f:
            info = f.read()

        return info

    def get_document(self, doc: UDPipeDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (UDPipeDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Dictionary of token features within document sentences
        """
        sentences = []

        if hasattr(doc, 'sents'):
            for sent in doc.sents:
                tokens = []
                for token in sent:
                    tokens.append({
                        "id": str(token.i + 1),
                        "form": token.text,
                        "lemma": token.lemma_,
                        "xpos": "_",
                        "upos": token.pos_,
                        "feats": str(token.morph),
                        "head": str(token.head.i + 1),
                        "deprel": token.dep_,
                        "deps": "_",
                        "misc": "_"
                    })
                sentences.append(tokens)
        return cast(UnifiedCoNLLUDocument, sentences)


class StanzaAnalyzer(LibraryWrapper):
    """
    Wrapper for stanza library.
    """

    #: Analyzer
    _analyzer: AbstractCoNLLUAnalyzer

    def __init__(self) -> None:
        """
        Initialize an instance of the StanzaAnalyzer class.
        """

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the Stanza model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """

    def analyze(self, texts: list[str]) -> list[StanzaDocument]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[StanzaDocument]: List of documents
        """

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """

    def from_conllu(self, article: Article) -> StanzaDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            StanzaDocument: Document ready for parsing
        """

    def get_document(self, doc: StanzaDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (StanzaDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Document of token features within document sentences
        """


class POSFrequencyPipeline:
    """
    Count frequencies of each POS in articles, update meta info and produce graphic report.
    """

    def __init__(self, corpus_manager: CorpusManager, analyzer: LibraryWrapper) -> None:
        """
        Initialize an instance of the POSFrequencyPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
        """

    def _count_frequencies(self, article: Article) -> dict[str, int]:
        """
        Count POS frequency in Article.

        Args:
            article (Article): Article instance

        Returns:
            dict[str, int]: POS frequencies
        """

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """


class PatternSearchPipeline(PipelineProtocol):
    """
    Search for the required syntactic pattern.
    """

    def __init__(
        self, corpus_manager: CorpusManager, analyzer: LibraryWrapper, pos: tuple[str, ...]
    ) -> None:
        """
        Initialize an instance of the PatternSearchPipeline class.

        Args:
            corpus_manager (CorpusManager): CorpusManager instance
            analyzer (LibraryWrapper): Analyzer instance
            pos (tuple[str, ...]): Root, Dependency, Child part of speech
        """

    def _make_graphs(self, doc: CoNLLUDocument) -> list[DiGraph]:
        """
        Make graphs for a document.

        Args:
            doc (CoNLLUDocument): Document for patterns searching

        Returns:
            list[DiGraph]: Graphs for the sentences in the document
        """

    def _add_children(
        self, graph: DiGraph, subgraph_to_graph: dict, node_id: int, tree_node: TreeNode
    ) -> None:
        """
        Add children to TreeNode.

        Args:
            graph (DiGraph): Sentence graph to search for a pattern
            subgraph_to_graph (dict): Matched subgraph
            node_id (int): ID of root node of the match
            tree_node (TreeNode): Root node of the match
        """

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """


def main() -> None:
    """
    Entrypoint for pipeline module.
    """


if __name__ == "__main__":
    main()
