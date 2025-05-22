"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib

import spacy_udpipe
from networkx import DiGraph
from spacy_conll import ConllParser

from core_utils.article import io
from core_utils.article.article import Article, ArtifactType
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
        self.path = path_to_raw_txt_data
        self._storage = {}
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path.exists():
            raise FileNotFoundError('File does not exist')
        if not self.path.is_dir():
            raise NotADirectoryError('Path does not lead to directory')
        raw_files = [raw.name for raw in self.path.iterdir() if raw.name.endswith('_raw.txt')]
        metas = [meta.name for meta in self.path.iterdir() if meta.name.endswith('_meta.json')]
        for file_id in range(1, len(raw_files) + 1):
            if not any(f'{str(file_id)}_raw.txt' == raw for raw  in raw_files):
                raise InconsistentDatasetError('Raw IDs contain slips')
        for meta_id in range(1, len(metas) + 1):
            if not any(f'{str(meta_id)}_meta.json' == meta for meta in metas):
                raise InconsistentDatasetError('Meta IDs contain slips')
        if len(raw_files) != len(metas):
            raise InconsistentDatasetError('Number of meta and raw files is not equal')
        for file in self.path.iterdir():
            if file.name.endswith('_raw.txt') or file.name.endswith('_meta.json'):
                if len(file.read_text(encoding='utf-8', errors='replace')) == 0:
                    raise InconsistentDatasetError('Files are empty')
        if not raw_files and not metas:
            raise EmptyDirectoryError('The directory is empty')

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for raw_file in [raw.name for raw in self.path.iterdir() if '_raw.txt' in raw.name]:
            self._storage[int(raw_file[:-8])] = io.from_raw(
                self.path.joinpath(raw_file), Article(url=None, article_id=int(raw_file[:-8])))

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
        conllu = self._analyzer.analyze([article.text for article
                                         in list(self._corpus.get_articles().values())])
        for article in list(self._corpus.get_articles().values()):
            article.get_cleaned_text()
            io.to_cleaned(article)
            article.set_conllu_info(conllu[list(self._corpus.get_articles().
                                                values()).index(article)])
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
        model = spacy_udpipe.load_from_path(lang="ru", path=str(model_path))
        model.add_pipe(
            "conll_formatter",
            last=True,
            config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True},
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
        return [f'{self._analyzer(text)._.conll_str}\n' for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(article.get_conllu_info())

    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        if len(path.read_text(encoding='utf-8')) == 0:
            raise EmptyFileError('An article file is empty')
        with open(path, 'r', encoding='utf-8') as file:
            document = file.read()
        parsed: UDPipeDocument = (ConllParser(self._analyzer).parse_conll_text_as_spacy
                                  (document.strip('\n')))
        return parsed

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
        pos_frequency = {}
        article_conllu = self._analyzer.from_conllu(article)
        for token in article_conllu:
            if token.pos_ not in pos_frequency:
                pos_frequency.update({token.pos_: 1})
            else:
                pos_frequency[token.pos_] += 1
        return pos_frequency

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for article in self._corpus.get_articles().values():
            io.from_meta(article.get_meta_file_path(), article)
            pos_freq = self._count_frequencies(article)
            article.set_pos_info(pos_freq)
            io.to_meta(article)
            visualize(article=article, path_to_save=ASSETS_PATH / '1_image.png')


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
