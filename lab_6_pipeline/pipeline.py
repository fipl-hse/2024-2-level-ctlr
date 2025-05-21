"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
import re
from pathlib import Path

import spacy_udpipe
from conllu import parse
from networkx import DiGraph
from spacy_conll.parser import ConllParser

from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import from_raw, to_cleaned, to_meta
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
from core_utils.visualizer import visualize


class EmptyDirectoryError(Exception):
    """
    This class checks directory is empty.
    """


class InconsistentDatasetError(Exception):
    """
    Check IDs contain slips, number of meta and raw files is not equal, files are empty
    """


class EmptyFileError(Exception):
    """
    Check when an article file is empty.
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
        path = Path(self.path_to_raw_txt_data)
        if not path.exists():
            raise FileNotFoundError(f"The specified path does not exist: {path}")
        if not path.is_dir():
            raise NotADirectoryError(f"The specified path is not a directory: {path}")

        if not any(self.path_to_raw_txt_data.iterdir()):
            raise EmptyDirectoryError

        raw_files = list(path.glob('**/*_raw.txt'))
        meta_files = list(path.glob('**/*_meta.json'))
        raw_ids = set()
        meta_ids = set()

        for file in raw_files:
            filename = file.name
            parts = filename.split('_')
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            file_id = int(parts[0])
            if file.stat().st_size == 0:
                raise InconsistentDatasetError(f"File is empty: {filename}")
            raw_ids.add(file_id)

        for file in meta_files:
            filename = file.name
            parts = filename.split('_')
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            file_id = int(parts[0])
            if file.stat().st_size == 0:
                raise InconsistentDatasetError(f"File is empty: {filename}")
            meta_ids.add(file_id)

        if raw_ids != meta_ids or not raw_ids or not meta_ids:
            raise InconsistentDatasetError("Number of meta and raw files is not equal")
        if sorted(raw_ids) != list(range(min(sorted(raw_ids)), max(sorted(raw_ids)) + 1)):
            raise InconsistentDatasetError("IDs contain slips or are inconsistent.")

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
        self._corpus_manager = corpus_manager
        self._analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        articles = self._corpus_manager.get_articles().values()
        analyzed = self._analyzer.analyze([article.text for article in articles])
        for ind, article in enumerate(articles):
            to_cleaned(article)
            article.set_conllu_info(analyzed[ind])
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
        model = PROJECT_ROOT / "core_utils" / "udpipe" / "russian-syntagrus-ud-2.0-170801.udpipe"
        if not model.exists() or not model.is_file():
            raise FileNotFoundError(f"UDPipe model not found or path is empty: {model}")

        nlp = spacy_udpipe.load_from_path(
            lang="ru",
            path=str(model)
        )

        nlp.add_pipe(
            "conll_formatter",
            last=True,
            config={
                "conversion_maps": {"XPOS": {"": "_"}},
                "include_headers": True
            }
        )

        return nlp

    def analyze(self, texts: list[str]) -> list[UDPipeDocument | str]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[UDPipeDocument | str]: List of documents
        """
        return [f'{self._analyzer(text)._.conll_str}\n' for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        conllu = article.get_conllu_info()
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(conllu)

    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        if not path.stat().st_size:
            raise EmptyFileError
        with open(path, 'r', encoding='utf-8') as file:
            conllu_str = file.read()
        parser = ConllParser(self._analyzer)
        result: UDPipeDocument = parser.parse_conll_text_as_spacy(conllu_str.rstrip('\n'))
        return result

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
        self._corpus = corpus_manager
        self._analyzer = analyzer

    def _count_frequencies(self, article: Article) -> dict[str, int]:
        """
        Count POS frequency in Article.

        Args:
            article (Article): Article instance

        Returns:
            dict[str, int]: POS frequencies
        """
        conllu_path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(conllu_path, "r", encoding="utf-8") as f:
            data = f.read()
        sentences = parse(data)

        pos_counts = {}
        for sentence in sentences:
            for token in sentence:
                pos = token.get('upostag')
                if pos:
                    pos_counts[pos] = pos_counts.get(pos, 0) + 1

        return pos_counts

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        articles = self._corpus.get_articles().values()
        for article in articles:

            frequencies = self._count_frequencies(article)
            if not frequencies:
                raise EmptyFileError(f"Empty POS frequencies "
                                     f"for article {article.article_id}")

            article.set_pos_info(frequencies)
            to_meta(article)

            image = (article.get_file_path(ArtifactType.CLEANED).parent /
                     f"{article.article_id}_image.png")
            visualize(article=article, path_to_save=image)


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
    pipeline = TextProcessingPipeline(corpus_manager=corpus_manager)
    pipeline.run()


if __name__ == "__main__":
    main()
