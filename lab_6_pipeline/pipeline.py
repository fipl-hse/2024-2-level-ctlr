"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from collections import defaultdict

import spacy_udpipe
from networkx import DiGraph
from networkx.algorithms.isomorphism.vf2userfunc import GraphMatcher
from spacy_conll import ConllParser

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


class EmptyDirectoryError(Exception):
    """
    Given directory is empty
    """


class InconsistentDatasetError(Exception):
    """
    Dataset numeration is inconsistent
    """


class EmptyFileError(Exception):
    """
    Given file is empty
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
            raise FileNotFoundError('File cannot be found')
        if not self.path.is_dir():
            raise NotADirectoryError('Given path does not lead to a directory')
        if not any(self.path.iterdir()):
            raise EmptyDirectoryError('Given directory is empty')
        meta = set([file.name for file in self.path.iterdir() if file.name.endswith('_meta.json')])
        raw = set([file.name for file in self.path.iterdir() if file.name.endswith('_raw.txt')])
        if len(meta) != len(raw):
            raise InconsistentDatasetError('Dataset numeration is inconsistent')
        true_meta = {f'{n}_meta.json' for n in range(1, len(meta) + 1)}
        true_raw = {f'{n}_raw.txt' for n in range(1, len(raw) + 1)}
        if meta != true_meta or raw != true_raw:
            raise InconsistentDatasetError('Dataset numeration is inconsistent')

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for file in self.path.iterdir():
            if not file.name.endswith('_raw.txt'):
                continue
            article = from_raw(file)
            self._storage[article.article_id] = article

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
        articles = self._corpus.get_articles().values()
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
        model_path = PROJECT_ROOT / "core_utils" / "udpipe" / "russian-syntagrus-ud-2.0-170801.udpipe"
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
        return [self._analyzer(text)._.conll_str for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        with open(article.get_file_path(ArtifactType.UDPIPE_CONLLU), 'w', encoding='UTF-8') as file:
            file.write(article.get_conllu_info())
            file.write("\n")

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
        parser = ConllParser(self._analyzer)
        with open(path, 'r', encoding='UTF-8') as file:
            conllu = file.read()
        data: UDPipeDocument = parser.parse_conll_text_as_spacy(conllu[:-1])
        return data

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
        pos_dict = defaultdict(int)
        sentences = self._analyzer.from_conllu(article).sents
        for sentence in sentences:
            for word in sentence:
                pos_dict[word.pos_] += 1
        return pos_dict

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for article in self._corpus.get_articles().values():
            article_path = article.get_meta_file_path()
            from_meta(article_path, article)
            article.set_pos_info(self._count_frequencies(article))
            to_meta(article)
            visualize(article, ASSETS_PATH / f'{article.article_id}_image.png')


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
        for sentence in doc.sents:
            graph = DiGraph()
            for token in sentence.as_doc():
                graph.add_node(token.i, label=token.pos_, word=token.text)
                graph.add_edge(token.head.i, token.i, label=token.dep_)
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
        for child in graph.neighbors(node_id):
            if child in subgraph_to_graph:
                child_node = TreeNode(child['label'], child['word'], [])
                tree_node.children.append(child_node)
                self._add_children(graph, subgraph_to_graph, child, child_node)

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """
        results = {}
        for index, graph in enumerate(doc_graphs):
            target_graph = DiGraph()
            for node in graph.nodes():
                if graph.nodes[node]['label'] in self._node_labels:
                    target_graph.add_node(
                        node,
                        label=graph.nodes[node]['label']
                    )
            for edge in graph.edges():
                if edge[0] in target_graph.nodes and edge[1] in target_graph.nodes:
                    target_graph.add_edge(edge[0], edge[1])
            matcher = GraphMatcher(
                graph,
                target_graph,
                node_match=lambda node_1, node_2: node_1['label'] == node_2['label'])
            if matcher.is_isomorphic():
                matched_nodes = []
                for isomorph in matcher.subgraph_isomorphisms_iter():
                    matched_subgraph = graph.subgraph(isomorph)
                    tree_nodes = [node for node in matched_subgraph.nodes() if
                                  len(matched_subgraph.in_edges(node)) == 0]
                    for tree_node in tree_nodes:
                        tree_node_tn = TreeNode(graph.nodes[tree_node]['label'], graph.nodes[tree_node]['word'], [])
                        graph_dict = {node: list(matched_subgraph.neighbors(node)) for node in matched_subgraph.nodes()}
                        self._add_children(graph, graph_dict, tree_node, tree_node_tn)
                        matched_nodes.append(tree_node_tn)
                if matched_nodes:
                    results[index] = matched_nodes
        return results

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """
        for article in self._corpus.get_articles():
            conllu = self._analyzer.from_conllu(article)
            graphs = self._make_graphs(conllu)
            patterns = self._find_pattern(graphs)
            article.set_patterns_info(patterns)
            to_meta(article)


def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    corpus_manager = CorpusManager(path_to_raw_txt_data=ASSETS_PATH)
    analyzer = UDPipeAnalyzer()
    pipeline = TextProcessingPipeline(corpus_manager, analyzer)
    pipeline.run()
    visualizer = POSFrequencyPipeline(corpus_manager, analyzer)
    visualizer.run()

if __name__ == "__main__":
    main()
