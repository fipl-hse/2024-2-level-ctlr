"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
import re

from networkx import DiGraph

from core_utils.article.article import Article
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
    Raised when IDs contain slips, number of meta and raw files is not equal, files are empty.
    """


class EmptyDirectoryError(Exception):
    """
    Raised when directory is empty.
    """


class EmptyFileError(Exception):
    """
    Raised when file is empty.
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
        self._path_to_raw_txt_data = path_to_raw_txt_data
        self._storage = {}
        self._scan_dataset()
        self._validate_dataset()
    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self._path_to_raw_txt_data.exists():
            raise FileNotFoundError(f"File '{self._path_to_raw_txt_data}' does not exist")
        if not self._path_to_raw_txt_data.is_dir():
            raise NotADirectoryError(f"Path '{self._path_to_raw_txt_data}' does not lead to directory")
        dir_of_raw_files = list(self._path_to_raw_txt_data.glob('*_raw.txt'))
        dir_of_meta_files = list(self._path_to_raw_txt_data.glob('*_meta.json'))
        if not dir_of_raw_files and not dir_of_meta_files:
            raise EmptyDirectoryError(f'Directory is empty: {self._path_to_raw_txt_data} :(')

        all_raw_ids = set()
        for raw in dir_of_raw_files:
            if raw.stat().st_size == 0:
                raise InconsistentDatasetError(f'The file {raw} is empty')
            file_match = re.match(r'(\d+)_raw\.txt', raw.name)
            if file_match:
                raw_id = int(file_match.group(1))
                all_raw_ids.add(raw_id)

        all_meta_ids = set()
        for raw in dir_of_meta_files:
            if raw.stat().st_size == 0:
                raise InconsistentDatasetError(f'The file {raw} is empty')
            file_match = re.match(r'(\d+)_meta\.json', raw.name)
            if file_match:
                raw_id = int(file_match.group(1))
                all_meta_ids.add(raw_id)

        if not (all_raw_ids and all_meta_ids):
            raise InconsistentDatasetError('Some files are empty or not exists')
        if all_raw_ids != all_meta_ids:
            raise InconsistentDatasetError('Raw and meta file IDs are not match')
        if sorted(all_raw_ids) != list(range(1, len(all_raw_ids) + 1)):
            raise InconsistentDatasetError('IDs are inconsistent')
    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for file in self._path_to_raw_txt_data.glob('*_raw.txt'):
            file_match = re.match(r'(\d+)_raw\.txt', file.name)
            if not file_match:
                continue
            article_id = int(file_match.group(1))

            with open(file, 'r', encoding='utf-8') as f:
                text = f.read()

            article = Article(url=None, article_id=article_id)
            article.text = text
            self._storage[article_id] = article
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
        for article in self._corpus.get_articles().values():
            path_to_raw_text = self._corpus._path_to_raw_txt_data / f'{article.article_id}_raw.txt'
            raw_text = path_to_raw_text.read_text(encoding='utf-8')
            text_to_work = re.sub(r'[^\w\s]', '', raw_text.lower())

            if len(text_to_work.strip()) < 50:
                continue

            article._cleaned_text = text_to_work
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

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the UDPipe model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """

    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """

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


if __name__ == "__main__":
    main()
