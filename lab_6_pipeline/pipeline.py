"""
Pipeline for CONLL-U formatting.
"""
import pathlib

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import re
from pathlib import Path

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


class EmptyDirectoryError(Exception):
    """
    Exception for empty directory.
    """

class InconsistentDatasetError(Exception):
    """
    Exception for inconsistent dataset.
    """

class EmptyFileError(Exception):
    """
    Exception for empty file.
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
            raise FileNotFoundError("Directory does not exist")
        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError("Path is not a directory")

        json_count = [file for file in self.path_to_raw_txt_data.iterdir() if '.json' in str(file)]
        meta_files = sorted(json_count, key=lambda m: int(str(m.stem).split('_', maxsplit=1)[0]))
        article_count = [file for file in self.path_to_raw_txt_data.iterdir()
                         if 'raw.txt' in str(file)]
        raw_files = sorted(article_count, key=lambda m: int(str(m.stem).split('_', maxsplit=1)[0]))

        if not meta_files or not raw_files:
            raise EmptyDirectoryError("Directory is empty")

        for raw, meta in zip(raw_files, meta_files):
            if len(raw_files) != len(meta_files):
                raise InconsistentDatasetError(f"ID mismatch between {raw.name} and {meta.name}")
            if raw.stat().st_size == 0:
                raise InconsistentDatasetError(f"Empty raw file: {raw.name}")
            if meta.stat().st_size == 0:
                raise InconsistentDatasetError(f"Empty meta file: {meta.name}")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        ids = set()
        for file in Path(self.path_to_raw_txt_data).rglob('*.txt'):
            if not file.name.endswith('_raw.txt'):
                continue
            pattern = re.match(r'(\d+)_raw\.txt$', file.name)
            if not pattern:
                continue
            article_id = int(pattern.group(1))
            if article_id in ids:
                continue
            article = from_raw(file)
            self._storage[article_id] = article
            ids.add(article_id)

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
        articles = self.corpus_manager.get_articles()
        for article in articles.values():
            article.text = article.text.replace('\u00A0', ' ')
            to_cleaned(article)

        if self._analyzer is not None:
            texts_to_analyze = [article.text for article in articles.values()]
            analyzed_texts = self._analyzer.analyze(texts_to_analyze)

            if len(analyzed_texts) == len(texts_to_analyze):
                for i, article in enumerate(articles.values()):
                    article.set_conllu_info(analyzed_texts[i])
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
        model_path = (PROJECT_ROOT / "lab_6_pipeline" / "assets" / "model" /
                      "russian-syntagrus-ud-2.0-170801.udpipe")
        if not model_path.exists():
            raise FileNotFoundError(f"UDPipe model not found at {model_path}")
        model = spacy_udpipe.load_from_path(lang='ru', path=str(model_path))
        if model is None:
            raise RuntimeError("Failed to load UDPipe model")
        model.add_pipe(
            "conll_formatter",
            last=True,
            config={
                "conversion_maps": {"XPOS": {"": "_"}},
                "include_headers": True,
            },
        )
        return model

    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """
        return [str(self._analyzer(text)._.conll_str) for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(article.get_conllu_info() + '\n')

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
    analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(corpus_manager, analyzer=analyzer)
    pipeline.run()


if __name__ == "__main__":
    main()
