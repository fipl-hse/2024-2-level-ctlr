"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from typing import cast

import spacy_udpipe
from networkx import DiGraph
from spacy_conll import ConllParser  # type: ignore[import-not-found, import-untyped]

from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import from_meta, from_raw, to_cleaned, to_meta
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
    IDs contain slips, number of meta and raw files is not equal, files are empty
    """


class EmptyFileError(Exception):
    """
    Raised when an article file is empty.
    """


class EmptyDirectoryError(Exception):
    """
    Directory is empty
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
            raise FileNotFoundError('not existent path')
        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError('path does not lead to directory')
        ids = {int(file_path.name.split('_')[0])
               for file_path in self.path_to_raw_txt_data.glob('*_raw.txt')}
        if not ids:
            raise EmptyDirectoryError('directory is empty')
        expected_ids = set(range(1, len(ids) + 1))
        if ids != expected_ids:
            raise InconsistentDatasetError('ids contain slips')
        for file in self.path_to_raw_txt_data.glob('*_raw.txt'):
            if file.stat().st_size == 0:
                raise InconsistentDatasetError('file is empty')
        if (len(list(self.path_to_raw_txt_data.glob('*_meta.json'))) !=
            len(list(self.path_to_raw_txt_data.glob('*_raw.txt')))):
            raise InconsistentDatasetError('numbers of meta and txt are not equal')

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        path_list = sorted(self.path_to_raw_txt_data.glob('*_raw.txt'),
                      key=lambda x: int(x.stem.split('_')[0]))
        for i, path in enumerate(path_list, start=1):
            self._storage[i] = from_raw(path,
                                        Article(url=None,
                                                article_id=i))


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
        add_punct = ['—', '«', '»']
        articles = self.corpus_manager.get_articles()
        for article in articles.values():
            article.text = article.text.replace('\u00A0', ' ')
            for x in add_punct:
                article.text = article.text.replace(x, '')
            to_cleaned(article)
        analyzed = self._analyzer.analyze([article.text
                                           for article
                                           in articles.values()])
        for i, article in enumerate(articles.values()):
            article.set_conllu_info(analyzed[i])
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
        model_path = (PROJECT_ROOT
                        / 'lab_6_pipeline'
                        / 'assets'
                        / 'model'
                        / 'russian-syntagrus-ud-2.0-170801.udpipe'
                        )
        model = spacy_udpipe.load_from_path(lang='ru',
                                            path=str(model_path))
        model.add_pipe(
            factory_name='conll_formatter',
            last=True,
            config={'conversion_maps': {'XPOS': {'': '_'}},
                    'include_headers': True},
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
        with open(article.get_file_path(ArtifactType.UDPIPE_CONLLU),
                  'w',
                  encoding='utf-8') as file:
            file.write(article.get_conllu_info())
            file.write('\n')

    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        if pathlib.Path(path).stat().st_size == 0:
            raise EmptyFileError('no conllu file')
        with open(path,
                  "r",
                  encoding="utf-8") as f:
            conllu_text = f.read()
            parser = ConllParser(self._analyzer)
            doc = cast(UDPipeDocument,
                       parser.parse_conll_text_as_spacy(conllu_text.strip()))
            return doc

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
        self._corpus_manager = corpus_manager
        self._analyzer = analyzer

    def _count_frequencies(self, article: Article) -> dict[str, int]:
        """
        Count POS frequency in Article.

        Args:
            article (Article): Article instance

        Returns:
            dict[str, int]: POS frequencies
        """
        freq_pos = {}
        for token in self._analyzer.from_conllu(article):
            freq_pos[f'{token.pos_}'] = freq_pos.get(f'{token.pos_}', 0) + 1
        return freq_pos

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for i, art in self._corpus_manager.get_articles().items():
            from_meta(art.get_meta_file_path(), art)
            art.set_pos_info(self._count_frequencies(art))
            to_meta(art)
            visualize(art, pathlib.Path(ASSETS_PATH) / f"{i}_image.png")


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
    corman = CorpusManager(ASSETS_PATH)
    udpipe_analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(corman, udpipe_analyzer)
    pipeline.run()
    visualizer = POSFrequencyPipeline(corman, udpipe_analyzer)
    visualizer.run()


if __name__ == "__main__":
    main()
