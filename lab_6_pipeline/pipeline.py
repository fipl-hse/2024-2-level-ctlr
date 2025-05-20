"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from pathlib import Path
from string import punctuation
from core_utils.constants import ASSETS_PATH
import spacy_udpipe
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


class EmptyDirectoryError(Exception):
    """
    Raised when dataset directory is empty.
    """


class InconsistentDatasetError(Exception):
    """
    Raised when the dataset is inconsistent: IDs contain slips, number of meta and raw files is not equal, files are empty.
    """


class EmptyFileError(Exception):
    """
    Raised when an article file is empty.
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
        self.path = path_to_raw_txt_data
        self._storage = {}
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path.exists():
            raise FileNotFoundError(f" Directory {self.path} does not exist")
        if not self.path.is_dir():
            raise NotADirectoryError(f" {self.path} is not a directory")
        if not any(self.path.iterdir()):
            raise EmptyDirectoryError

        meta_files = []
        raw_files = []
        for filepath in self.path.iterdir():
            if filepath.name.endswith('_meta.json'):
                if filepath.stat().st_size == 0:
                    raise InconsistentDatasetError(f"Empty metainfo file: {filepath.name}")
                meta_files.append(filepath)
            elif filepath.name.endswith('_raw.txt'):
                if filepath.stat().st_size == 0:
                    raise InconsistentDatasetError(f"Empty text file: {filepath.name}")
                raw_files.append(filepath)
        if len(meta_files) != len(raw_files):
            raise InconsistentDatasetError(f"Number of meta and raw files is not equal:"
                                           f"{len(raw_files)} texts != {len(meta_files)} jsons")
        meta_ids = set()
        for m in meta_files:
            if '_' not in m.stem:
                continue
            file_id = m.stem.split('_')[0]
            if not file_id.isdigit():
                continue
            meta_ids.add(int(file_id))
        raw_ids = set()
        for r in raw_files:
            if '_' not in r.stem:
                continue
            file_id = r.stem.split('_')[0]
            if not file_id.isdigit():
                continue
            raw_ids.add(int(file_id))
        expected_ids = set(range(1, len(meta_files) + 1))
        if meta_ids != expected_ids or raw_ids != expected_ids:
            missing_meta = expected_ids - meta_ids
            missing_raw = expected_ids - raw_ids
            raise InconsistentDatasetError(f"Inconsistent IDs."
                                           f"Missing meta: {missing_meta}, missing raw: {missing_raw}")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for filepath in self.path.glob("*_raw.txt"):
            file_name_elements = filepath.stem.split('_')
            if len(file_name_elements) >= 1 and file_name_elements[0].isdigit():
                article_id = int(file_name_elements[0])
                if filepath.stat().st_size > 0:
                    with open(filepath, 'r', encoding='utf-8') as file:
                        raw_text = file.read()
                        article = Article(url=None, article_id=article_id)
                        article.text = raw_text
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
        self.corpus_manager = corpus_manager
        self._analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        for article_id, article in self.corpus_manager.get_articles().items():
            lowered_text = article.text.lower()
            no_punctuation_text = ''.join(el if el not in punctuation else ' ' for el in lowered_text)
            article.text = no_punctuation_text.replace('NBSP', '')
            to_cleaned(article)

        if self._analyzer:
            articles = list(self.corpus_manager.get_articles().values())
            analyzed_texts = self._analyzer.analyze([article.text for article in articles])
            if isinstance(analyzed_texts, (list, tuple)) and len(analyzed_texts) != len(articles):
                for article, analyzed in zip(articles, analyzed_texts):
                    article.set_conllu_info(analyzed)
                    self._analyzer.to_conllu(article)


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
    repo_root = Path(__file__).parent.parent
    path = repo_root / "tmp" / "articles"
    corpus_manager = CorpusManager(path)
    pipeline = TextProcessingPipeline(corpus_manager)
    udpipe_analyzer = UDPipeAnalyzer()


if __name__ == "__main__":
    main()
