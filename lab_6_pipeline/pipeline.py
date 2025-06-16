"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from collections import defaultdict
from dataclasses import asdict

import spacy_udpipe
import stanza
from networkx import DiGraph, to_dict_of_lists
from networkx.algorithms.isomorphism import categorical_node_match, GraphMatcher
from spacy_conll.parser import ConllParser
from stanza.models.common.doc import Document
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


class EmptyDirectoryError(Exception):
    """
    Raised when dataset directory is empty.
    """


class InconsistentDatasetError(Exception):
    """
    Raised when the dataset is inconsistent: IDs contain slips,
    number of meta and raw files is not equal, files are empty.
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
            raise FileNotFoundError(f'File {self.path} does not exist.')
        if not self.path.is_dir():
            raise NotADirectoryError(f'{self.path} is not a directory.')
        if not any(self.path.iterdir()):
            raise EmptyDirectoryError(f'Directory {self.path} is empty.')
        meta = [filepath.name for filepath in self.path.glob('*_meta.json')]
        raw = [filepath.name for filepath in self.path.glob('*_raw.txt')]
        if len(meta) != len(raw):
            raise InconsistentDatasetError(
                f'The amounts of meta and raw files are not equal: {len(meta)} != {len(raw)}.')
        meta_template = [f'{count}_meta.json' for count in range(1, len(meta) + 1)]
        if set(meta) != set(meta_template):
            raise InconsistentDatasetError('IDs of meta files are inconsistent.')
        raw_template = [f'{count}_raw.txt' for count in range(1, len(raw) + 1)]
        if set(raw) != set(raw_template):
            raise InconsistentDatasetError('IDs of raw files are inconsistent.')
        for mask in ['*_raw.txt', '*_meta.json']:
            if any(filepath.stat().st_size == 0 for filepath in self.path.glob(mask)):
                raise InconsistentDatasetError('Some files are empty.')

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        self._storage = {get_article_id_from_filepath(filepath): from_raw(filepath)
                         for filepath in self.path.glob('*_raw.txt')}

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
        conllu = self._analyzer.analyze([article.text for article
                                         in self.corpus_manager.get_articles().values()])
        for idx, article in enumerate(self.corpus_manager.get_articles().values()):
            article.text = article.text.replace('\u00A0', '')
            to_cleaned(article)
            article.set_conllu_info(conllu[idx])
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
        model_path = pathlib.Path(PROJECT_ROOT) /"lab_6_pipeline" /\
                     "assets" / "model" / "russian-syntagrus-ud-2.0-170801.udpipe"
        if not model_path.exists():
            raise FileNotFoundError("Path to model does not exists or is invalid.")
        model = spacy_udpipe.load_from_path(
            lang='ru',
            path=str(model_path)
        )
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
        conllu_path = article.get_file_path(ArtifactType.UDPIPE_CONLLU)
        if pathlib.Path(conllu_path).stat().st_size == 0:
            raise EmptyFileError(f'File {conllu_path} is empty.')
        with open(conllu_path, encoding='utf-8') as file:
            conllu = file.read()
        parsed_conllu: UDPipeDocument = ConllParser(
            self._analyzer).parse_conll_text_as_spacy(conllu.strip('\n'))
        return parsed_conllu

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
        self._analyzer = self._bootstrap()

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Load and set up the Stanza model.

        Returns:
            AbstractCoNLLUAnalyzer: Analyzer instance
        """
        language = "ru"
        processors = "tokenize,pos,lemma,depparse"
        stanza.download(lang=language, processors=processors, logging_level="INFO")
        stanza_analyzer: AbstractCoNLLUAnalyzer = stanza.Pipeline(
            lang=language, processors=processors, logging_level="INFO", download_method=None
        )
        return stanza_analyzer

    def analyze(self, texts: list[str]) -> list[StanzaDocument]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[StanzaDocument]: List of documents
        """
        return self._analyzer.process([Document([], text=text) for text in texts])

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        CoNLL.write_doc2conll(article.get_conllu_info(),
                              article.get_file_path(ArtifactType.STANZA_CONLLU))

    def from_conllu(self, article: Article) -> StanzaDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            StanzaDocument: Document ready for parsing
        """
        conllu_path = article.get_file_path(ArtifactType.STANZA_CONLLU)
        if not pathlib.Path(conllu_path).exists():
            raise FileNotFoundError(f'File {conllu_path} does not exist.')
        if pathlib.Path(conllu_path).stat().st_size == 0:
            raise EmptyFileError(f'File {conllu_path} is empty.')
        parsed_conllu: StanzaDocument = CoNLL.conll2doc(conllu_path)
        return parsed_conllu

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
        article_conllu = self._analyzer.from_conllu(article)
        pos_frequencies = defaultdict(int)
        if isinstance(self._analyzer, UDPipeAnalyzer):
            for token in article_conllu:
                pos_frequencies[token.pos_] += 1
        else:
            for word in article_conllu.iter_words():
                pos_frequencies[word.upos] += 1
        return pos_frequencies

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for idx, article in self._corpus.get_articles().items():
            if pathlib.Path(meta_path := article.get_meta_file_path()).stat().st_size == 0:
                raise EmptyFileError(f'File {meta_path} is empty.')
            from_meta(meta_path, article)
            article.set_pos_info(self._count_frequencies(article))
            to_meta(article)
            visualize(article, pathlib.Path(ASSETS_PATH) / f"{idx}_image.png")


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
        self._corpus = corpus_manager
        self._analyzer = analyzer
        self._node_labels = pos

    def _make_graphs(self, doc: CoNLLUDocument) -> list[DiGraph]:
        """
        Make graphs for a document.

        Args:
            doc (CoNLLUDocument): Document for patterns searching

        Returns:
            list[DiGraph]: Graphs for the sentences in the document
        """
        graphs = []
        if isinstance(doc, Document):
            for sentence in doc.sentences:
                graph = DiGraph()
                for word in sentence.words:
                    graph.add_node(word.id, label=word.upos)
                    graph.add_edge(word.head, word.id, label=word.deprel)
                graphs.append(graph)
            return graphs
        for sentence in doc.sents:
            graph = DiGraph()
            for word in sentence:
                graph.add_node(word.i, label=word.pos_)
                graph.add_edge(word.head, word.i, label=word.dep_)
            graphs.append(graph)
        return graphs


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
        children = graph.successors(node_id)
        if not children:
            return
        for child in children:
            child_info = graph.nodes()[child]
            child_tree_node = TreeNode(child_info['upos'], child_info['text'], [])
            tree_node.children.append(child_tree_node)
            self._add_children(graph, subgraph_to_graph, child, child_tree_node)

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """
        matches = {}
        pattern_graph = DiGraph()
        pattern_graph.add_nodes_from((idx, {"label": label})
                                     for idx, label in enumerate(self._node_labels))
        for sentence_id, graph in enumerate(doc_graphs):
            matcher = GraphMatcher(graph, pattern_graph,
                                   node_match=categorical_node_match('label', ''))
            pattern_nodes = []

            for match in matcher.subgraph_isomorphisms_iter():
                isomorphic_graph = graph.subgraph(match.keys()).copy()
                for node in isomorphic_graph.nodes():
                    tree_node = TreeNode(isomorphic_graph.nodes[node].get('label'),
                                         isomorphic_graph.nodes[node].get('text'),
                                         [])
                    self._add_children(
                        graph, to_dict_of_lists(pattern_graph), node, tree_node
                    )
                    pattern_nodes.append(tree_node)

            if pattern_nodes:
                matches[sentence_id] = pattern_nodes
        return matches

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """
        for article in list(self._corpus.get_articles().values())[:1]:
            article_conllu = self._analyzer.from_conllu(article)
            article_graphs = self._make_graphs(article_conllu)
            article_matches = self._find_pattern(article_graphs)
            article_matches_dict = {
                idx: [asdict(m) for m in article_match]
                for idx, article_match in article_matches.items()
            }
            article.set_patterns_info(article_matches_dict)
            to_meta(article)


def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    path = pathlib.Path(ASSETS_PATH)
    corpus_manager = CorpusManager(path)
    udpipe_analyzer = UDPipeAnalyzer()

    text_pipeline = TextProcessingPipeline(corpus_manager, udpipe_analyzer)
    pos_pipeline = POSFrequencyPipeline(corpus_manager, udpipe_analyzer)

    text_pipeline.run()
    pos_pipeline.run()


if __name__ == "__main__":
    main()
