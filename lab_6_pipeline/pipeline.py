"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
import re
from builtins import FileNotFoundError, NotADirectoryError

import spacy
import spacy_udpipe
from networkx import DiGraph

from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import from_raw, to_cleaned
from core_utils.constants import ASSETS_PATH, PROJECT_ROOT
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
    Raised when the dataset is inconsistent.
    """


class EmptyDirectoryError(Exception):
    """
    Raised when a directory is empty.
    """


class EmptyFileError(Exception):
    """
    Raised when an article file is empty
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
        self.path_to_raw_txt_data = path_to_raw_txt_data
        self._storage = {}
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

        files = list(self.path_to_raw_txt_data.iterdir())
        if not files:
            raise EmptyDirectoryError("Directory is empty")

        meta_pattern = re.compile(r'^(\d+)_meta\.json$')
        raw_pattern = re.compile(r'^(\d+)_raw\.txt$')
        meta_files = {}
        raw_files = {}

        for file in files:
            if match := meta_pattern.match(file.name):
                idx = int(match.group(1))
                meta_files[idx] = file
            elif match := raw_pattern.match(file.name):
                idx = int(match.group(1))
                raw_files[idx] = file

        if not meta_files:
            raise InconsistentDatasetError("No meta files found")
        if not raw_files:
            raise InconsistentDatasetError("No raw files found")

        if len(meta_files.keys()) != len(raw_files.keys()):
            raise InconsistentDatasetError("Number of meta and raw files is not equal")
        if sorted(meta_files.keys()) != list(range(1, len(meta_files) + 1)):
            raise InconsistentDatasetError("IDs contain slips")

        for file in list(meta_files.values()) + list(raw_files.values()):
            if file.stat().st_size == 0:
                raise InconsistentDatasetError(f"File {file.name} is empty")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        raw_pattern = re.compile(r'^(\d+)_raw\.txt$')
        raw_files = {}
        for file in self.path_to_raw_txt_data.iterdir():
            if match := raw_pattern.match(file.name):
                idx = int(match.group(1))
                raw_files[idx] = file

        for idx, raw_file in raw_files.items():
            article = Article(url=None, article_id=idx)
            from_raw(raw_file, article)
            self._storage[idx] = article

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
        self._analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        articles = self._corpus.get_articles()
        for article in articles.values():
            if self._analyzer is not None:
                connlu_info = self._analyzer.analyze([article.text])
                if connlu_info is not None:
                    article.set_conllu_info(connlu_info[0])
                    self._analyzer.to_conllu(article)

            # regex removes all except (^...) letters and digits (\w), spaces (\s)
            article.text = re.sub(r'[^\w\s]', '', article.text, flags=re.UNICODE).lower()
            to_cleaned(article)


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
        self._analyzer = self._bootstrap()

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the UDPipe model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """
        model_path = PROJECT_ROOT / "core_utils" / "udpipe" / "russian-syntagrus-ud-2.0-170801.udpipe"
        nlp = spacy_udpipe.load_from_path("ru", str(model_path))
        nlp.add_pipe("conll_formatter", last=True, config={"include_headers": True}, )
        return nlp

    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """
        return [self._analyzer(text)._.conll_str for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        with open(article.get_file_path(kind=ArtifactType.UDPIPE_CONLLU), "w",
                  encoding="utf-8") as conllu_file:
            conllu_file.write(article.get_conllu_info())
            conllu_file.write("\n")

    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """

    def get_document(self, doc: UDPipeDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (UDPipeDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Dictionary of token features within document sentences
        """


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
    corpus_manager = CorpusManager(path_to_raw_txt_data=ASSETS_PATH)
    udpipe_analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(corpus_manager, udpipe_analyzer)
    pipeline.run()


if __name__ == "__main__":
    main()
