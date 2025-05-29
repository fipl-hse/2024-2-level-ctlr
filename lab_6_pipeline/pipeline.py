"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from pathlib import Path

import spacy_udpipe
from networkx import DiGraph
from spacy_conll import ConllParser  # type: ignore

from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import from_raw, to_meta
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
    Raised when files are empty, files' IDs contain slips
    or number of raw and meta files is not equal.
    """


class EmptyFileError(Exception):
    """
    Raised when file is empty.
    """


class EmptyDirectoryError(Exception):
    """
    Raised when directory is empty.
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
        self.path_to_raw = path_to_raw_txt_data
        self.articles: dict[int, Article] = {}
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path_to_raw.exists():
            raise FileNotFoundError(f"Path {self.path_to_raw} does not exist")
        if not self.path_to_raw.is_dir():
            raise NotADirectoryError(f"Path {self.path_to_raw} is not a directory")

        files = list(self.path_to_raw.glob("*"))
        if not files:
            raise EmptyDirectoryError(f"Directory {self.path_to_raw} is empty")

        meta_files = list(self.path_to_raw.glob("*_meta.json"))
        raw_files = list(self.path_to_raw.glob("*_raw.txt"))
        if not meta_files or not raw_files:
            raise InconsistentDatasetError("Missing meta or text files")

        meta_ids = {f.stem.replace('_meta', '') for f in meta_files}
        raw_ids = {f.stem.replace('_raw', '') for f in raw_files}

        if meta_ids != raw_ids:
            raise InconsistentDatasetError("Mismatched meta and text file sets")

        for file in raw_files:
            if file.read_text(encoding='utf-8').strip() == '':
                raise InconsistentDatasetError(f"Empty text file detected: {file.name}")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for raw_file in self.path_to_raw.glob("*.txt"):
            article = from_raw(raw_file)
            self.articles[article.article_id] = article

    def get_articles(self) -> dict:
        """
        Get storage params.

        Returns:
            dict: Storage params
        """
        return self.articles


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
        self.analyzer = analyzer

    def run(self) -> None:
        """
        Perform basic preprocessing and write processed text to files.
        """
        articles = self.corpus_manager.get_articles()
        if not articles:
            print("[WARNING] No articles found.")
            return

        texts = [article.text for article in articles.values()]
        if not texts:
            print("[WARNING] Articles contain no text.")
            return

        docs = self.analyzer.analyze(texts)

        for article, doc in zip(articles.values(), docs):
            article.set_conllu_info(doc)
            self.analyzer.to_conllu(article)


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
        model_path = PROJECT_ROOT / 'lab_6_pipeline' / 'assets' / 'model' / 'russian-syntagrus-ud-2.0-170801.udpipe'
        model = spacy_udpipe.load_from_path(lang='ru', path=str(model_path))
        model.add_pipe(
            factory_name='conll_formatter',
            last=True,
            config={'conversion_maps': {'XPOS': {'': '_'}}, 'include_headers': True},
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
        return [self._analyzer(text)._.conllu_str for text in texts]
        

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        with open(article.get_file_path(ArtifactType.UDPIPE_CONLLU), 'w', encoding='utf-8') as f:
            f.write(article.get_conllu_info())
            f.write('\n')

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
        with open(path, "r", encoding="utf-8") as f:
            conllu_text = f.read()
            parser = ConllParser(self._analyzer)
            doc = parser.parse_conll_text_as_spacy(conllu_text.strip())
            return doc

def get_document(self, doc: UDPipeDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (UDPipeDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Dictionary of token features within document sentences
        """
        return doc.to_unified()


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
        doc = self._analyzer.from_conllu(article)
        unified_doc = self._analyzer.get_document(doc)
        pos_tags = [token["upos"] for sentence in unified_doc.values() for token in sentence]

        freq = {}
        for tag in pos_tags:
            if tag in freq:
                freq[tag] += 1
            else:
                freq[tag] = 1
        return freq

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        articles = self._corpus.get_articles()
        for article in articles.values():
            freqs = self._count_frequencies(article)
            article.set_pos_info(freqs)
            to_meta(article)



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

path = Path('lab_6_pipeline/assets')
meta_ids = {f.stem.replace('_meta', '') for f in path.glob('*_meta.json')}
raw_ids = {f.stem.replace('_raw', '') for f in path.glob('*_raw.txt')}

print("Only in meta:", meta_ids - raw_ids)
print("Only in raw:", raw_ids - meta_ids)

def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    corpus = CorpusManager(ASSETS_PATH)
    analyzer = UDPipeAnalyzer()

    processing = TextProcessingPipeline(corpus, analyzer)
    processing.run()

    pos_pipeline = POSFrequencyPipeline(corpus, analyzer)
    pos_pipeline.run()


if __name__ == "__main__":
    main()
