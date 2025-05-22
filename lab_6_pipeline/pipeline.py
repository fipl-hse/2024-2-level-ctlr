"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib

import matplotlib.pyplot as plt
import spacy_udpipe
from networkx import DiGraph
from stanza.utils.conll import CoNLL

from core_utils.article.article import Article, ArtifactType, get_article_id_from_filepath
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
    Raises when dataset files are inconsistent
    """


class EmptyDirectoryError(Exception):
    """
    Raises when the directory is empty
    """


class EmptyFileError(Exception):
    """
    Raises when the file is empty
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
            raise FileNotFoundError

        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError

        if not any(self.path_to_raw_txt_data.iterdir()):
            raise EmptyDirectoryError

        meta_files = sorted(self.path_to_raw_txt_data.glob('*_meta.json'),
                            key=get_article_id_from_filepath)
        txt_files = sorted(self.path_to_raw_txt_data.glob('*_raw.txt'),
                           key=get_article_id_from_filepath)

        if len(txt_files) != len(meta_files):
            raise InconsistentDatasetError

        for meta, raw in zip(meta_files, txt_files):
            meta_id = get_article_id_from_filepath(meta)
            raw_id = get_article_id_from_filepath(raw)

            if meta_id != raw_id or not meta.stat().st_size or not raw.stat().st_size:
                raise InconsistentDatasetError

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        txt_files = sorted(self.path_to_raw_txt_data.glob('*_raw.txt'))
        self._storage = {
            get_article_id_from_filepath(raw):
                from_raw(path=raw, article=Article(None, get_article_id_from_filepath(raw)))
            for raw in txt_files}

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
        analyzed_texts = self._analyzer.analyze([article.text for article
                                                 in self._corpus.get_articles().values()])
        for article_id, article in enumerate(self._corpus.get_articles().values()):
            for char in ['–', '—', '−', '…']:
                article.text = article.text.replace(char, '')
            to_cleaned(article)
            article.set_conllu_info(analyzed_texts[article_id])
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
        model_path = (PROJECT_ROOT / 'lab_6_pipeline' / 'assets' / 'model'
                      / 'russian-syntagrus-ud-2.0-170801.udpipe')

        model = spacy_udpipe.load_from_path(
            lang="ru",
            path=str(model_path))

        model.add_pipe("conll_formatter", last=True,
                       config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True})

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
        conllu_info = article.get_conllu_info()
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(conllu_info)


    def from_conllu(self, article: Article) -> UDPipeDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            UDPipeDocument: Document ready for parsing
        """
        path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)

        if not path.exists() or path.stat().st_size == 0:
            raise EmptyFileError

        return CoNLL.conll2doc(input_file=path)


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
        pos_dict = {}
        for pos_tag in self._analyzer.from_conllu(article).get('upos'):
            pos_dict[pos_tag] = pos_dict.get(pos_tag, 0) + 1
        return pos_dict

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        articles = self._corpus.get_articles()
        for article in articles.values():
            meta_path = ASSETS_PATH / f"{article.article_id}_meta.json"
            meta_article = from_meta(meta_path)

            article.title = meta_article.title
            article.author = meta_article.author
            article.date = meta_article.date
            article.topics = meta_article.topics
            article.url = meta_article.url

            pos_freq = self._count_frequencies(article)
            article.set_pos_info(pos_freq)
            to_meta(article)

            output_path = ASSETS_PATH / f'{article.article_id}_image.png'
            visualize(article=article, path_to_save=output_path)
            plt.close()



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
    pos_pipeline = POSFrequencyPipeline(corpus_manager, udpipe_analyzer)
    pos_pipeline.run()


if __name__ == "__main__":
    main()
