"""
Pipeline for CONLL-U formatting.
"""

# pylint: disable=too-few-public-methods, undefined-variable, too-many-nested-blocks
import pathlib
from typing import cast

import spacy_udpipe
import stanza
from networkx import DiGraph
from networkx.algorithms.isomorphism import DiGraphMatcher
from spacy_conll import ConllParser
from stanza.utils.conll import CoNLL
from torch.utils.benchmark import Language

from core_utils.article.article import Article, ArtifactType
from core_utils.article.io import from_meta, from_raw, to_cleaned, to_meta
from core_utils.constants import ASSETS_PATH, PROJECT_ROOT
from core_utils.pipeline import (
    AbstractCoNLLUAnalyzer,
    CoNLLUDocument,
    ConLLUSentence,
    ConLLUWord,
    LibraryWrapper,
    PipelineProtocol,
    StanzaDocument,
    TreeNode,
    UDPipeDocument,
    UnifiedCoNLLUDocument,
)
from core_utils.visualizer import visualize

UDPIPE_MODEL_PATH = (PROJECT_ROOT /
                     "lab_6_pipeline" /
                     "assets" / "model" /
                     "russian-syntagrus-ud-2.0-170801.udpipe")


class InconsistentDatasetError(Exception):
    """
    Is raised when IDs contain slips, number of meta and raw files is not equal, files are empty
    """

class EmptyDirectoryError(Exception):
    """
    Is raised when directory is empty
    """

class EmptyFileError(Exception):
    """
    Is raised when an article file is empty
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
        self._storage = {}
        self.path_to_raw_txt_data = path_to_raw_txt_data
        self._validate_dataset()
        self._scan_dataset()

    def _validate_dataset(self) -> None:
        """
        Validate folder with assets.
        """
        if not self.path_to_raw_txt_data.exists():
            raise FileNotFoundError("File does not exist")

        if not self.path_to_raw_txt_data.is_dir():
            raise NotADirectoryError("Path does not lead to directory")

        if not any(self.path_to_raw_txt_data.iterdir()):
            raise EmptyDirectoryError("Directory is empty")

        raw, meta = [], []

        for f in self.path_to_raw_txt_data.glob('*'):
            if f.name.endswith('_raw.txt'):
                n_id = int(f.name.split("_")[0])
                raw.append(n_id)
                if f.stat().st_size == 0:
                    raise InconsistentDatasetError("File is empty")
            elif f.name.endswith('_meta.json'):
                n_id = int(f.name.split("_")[0])
                meta.append(n_id)
                if f.stat().st_size == 0:
                    raise InconsistentDatasetError("Meta file is empty")

        if len(raw) != len(meta):
            raise InconsistentDatasetError(
                "The amounts of meta and raw files are not equal")

        for ids in (sorted(raw), sorted(meta)):
            for i in range(1, len(ids)):
                if ids[i] != ids[i - 1] + 1:
                    raise InconsistentDatasetError("Dataset has slips")

    def _scan_dataset(self) -> None:
        """
        Register each dataset entry.
        """
        for file in self.path_to_raw_txt_data.glob("*_raw.txt"):
            if not file.name.endswith("_raw.txt"):
                continue

            n_id = int(file.name.split("_")[0])

            art = from_raw(file, Article(url=None, article_id=n_id))

            self._storage[n_id] = art

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
        for ind, article in enumerate(articles):
            to_cleaned(article)
            if self._analyzer:
                analyzed = self._analyzer.analyze([article.text for article in articles])
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
        model = spacy_udpipe.load_from_path(lang="ru", path=str(UDPIPE_MODEL_PATH))
        model.add_pipe(
            "conll_formatter",
            last=True,
            config={"conversion_maps": {"XPOS": {"": "_"}}, "include_headers": True},
        )
        return model # type: ignore

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
        parser = ConllParser(cast(Language, self._analyzer))
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
        unified_document = {}

        for sent_idx, sentence in enumerate(doc.sents, start=1):
            conllu_sentence = []

            for word in sentence:
                conll_data = word._.conll

                conllu_word = ConLLUWord(
                    id=conll_data["ID"],
                    upos=conll_data["UPOS"],
                    head=conll_data["HEAD"],
                    deprel=conll_data["DEPREL"],
                    text=conll_data["FORM"]
                )
                conllu_sentence.append(conllu_word)

            unified_document[sent_idx] = ConLLUSentence(words=conllu_sentence)

        return unified_document # type: ignore


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
        lang = "ru"
        processors = "tokenize,lemma,pos,depparse"
        stanza.download(lang=lang, processors=processors)

        model = stanza.Pipeline(
            lang=lang,
            processors=processors,
            download_method=None
        )

        return model # type: ignore

    def analyze(self, texts: list[str]) -> list[StanzaDocument]:
        """
        Process texts into CoNLL-U formatted markup.

        Args:
            texts (list[str]): Collection of texts

        Returns:
            list[StanzaDocument]: List of documents
        """
        return [self._analyzer(text) for text in texts]

    def to_conllu(self, article: Article) -> None:
        """
        Save content to ConLLU format.

        Args:
            article (Article): Article containing information to save
        """
        conllu_info = article.get_conllu_info()

        with open(article.get_file_path(ArtifactType.STANZA_CONLLU), 'a', encoding='UTF-8'):
            CoNLL.write_doc2conll(conllu_info,
                                  str(article.get_file_path(ArtifactType.STANZA_CONLLU)))

    def from_conllu(self, article: Article) -> StanzaDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            StanzaDocument: Document ready for parsing
        """
        path = article.get_file_path(ArtifactType.STANZA_CONLLU)
        if not path.stat().st_size:
            raise EmptyFileError(path)

        return CoNLL.conll2doc(input_file=path)

    def get_document(self, doc: StanzaDocument) -> UnifiedCoNLLUDocument:
        """
        Present ConLLU document's sentence tokens as a unified structure.

        Args:
            doc (StanzaDocument): ConLLU document

        Returns:
            UnifiedCoNLLUDocument: Document of token features within document sentences
        """
        unified_document = {}

        for sent_idx, sentence in enumerate(doc.sentences, start=1):
            conllu_sentence = []

            for word in sentence.words:
                conllu_word = ConLLUWord(
                    id=str(word.id),
                    upos=word.upos,
                    head=str(word.head),
                    deprel=word.deprel,
                    text=word.text
                )
                conllu_sentence.append(conllu_word)

            unified_document[sent_idx] = ConLLUSentence(words=conllu_sentence)

        return unified_document  # type: ignore


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
        pos_dict = {}
        unified_doc = self._analyzer.get_document(self._analyzer.from_conllu(article))
        for sent in unified_doc.values():
            for word in sent.words:
                pos = word.upos
                if pos not in pos_dict:
                    pos_dict[pos] = 1
                else:
                    pos_dict[pos] += 1
        return pos_dict

    def run(self) -> None:
        """
        Visualize the frequencies of each part of speech.
        """
        for article in self._corpus_manager.get_articles().values():
            article_path = article.get_meta_file_path()
            from_meta(article_path)
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
        self._corpus_manager = corpus_manager
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
        unified_doc = self._analyzer.get_document(doc)

        for sentence in unified_doc.values():
            current_graph = DiGraph()

            for word in sentence.words:
                current_graph.add_node(
                    int(word.id),
                    label=word.upos,
                    text=word.text
                )

            for word in sentence.words:
                if int(word.head) != int(word.id):
                    current_graph.add_edge(
                        u_of_edge=int(word.head),
                        v_of_edge=int(word.id),
                        label=word.deprel
                    )

            graphs.append(current_graph)

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
        cur_node = subgraph_to_graph[node_id]

        for child_node in graph.successors(cur_node):
            if child_node in subgraph_to_graph.values():
                pattern_child_id = next(
                    k for k, v in subgraph_to_graph.items()
                    if v == child_node
                )

                child_attrs = graph.nodes[child_node]
                child_tree_node = TreeNode(
                    upos=child_attrs['label'],
                    text=child_attrs.get('text', ''),
                    children=[]
                )

                tree_node.children.append(child_tree_node)
                self._add_children(graph, subgraph_to_graph, pattern_child_id, child_tree_node)

    def _find_pattern(self, doc_graphs: list) -> dict[int, list[TreeNode]]:
        """
        Search for the required pattern.

        Args:
            doc_graphs (list): A list of graphs for the document

        Returns:
            dict[int, list[TreeNode]]: A dictionary with pattern matches
        """
        pattern = DiGraph()
        pattern.add_node(1, label=self._node_labels[0])
        pattern.add_node(2, label=self._node_labels[1])
        pattern.add_node(3, label=self._node_labels[2])
        pattern.add_edge(1, 2)
        pattern.add_edge(2, 3)

        result = {}

        for sent_idx, sent_graph in enumerate(doc_graphs):
            matches = []

            matcher = DiGraphMatcher(
                sent_graph,
                pattern,
                node_match=lambda n1, n2: n1.get('label', '') == n2.get('label', '')
            )

            for mapping in matcher.subgraph_isomorphisms_iter():
                if 1 in mapping.values():
                    pattern_to_graph = {v: k for k, v in mapping.items()}
                else:
                    pattern_to_graph = mapping

                if not (sent_graph.has_edge(pattern_to_graph[1], pattern_to_graph[2]) and
                        sent_graph.has_edge(pattern_to_graph[2], pattern_to_graph[3])):
                    continue

                verb_tree = TreeNode(
                    upos=sent_graph.nodes[pattern_to_graph[1]]['label'],
                    text=sent_graph.nodes[pattern_to_graph[1]].get('text', ''),
                    children=[]
                )

                propn_tree = TreeNode(
                    upos=sent_graph.nodes[pattern_to_graph[2]]['label'],
                    text=sent_graph.nodes[pattern_to_graph[2]].get('text', ''),
                    children=[]
                )

                adp_tree = TreeNode(
                    upos=sent_graph.nodes[pattern_to_graph[3]]['label'],
                    text=sent_graph.nodes[pattern_to_graph[3]].get('text', ''),
                    children=[]
                )

                propn_tree.children.append(adp_tree)
                verb_tree.children.append(propn_tree)
                matches.append(verb_tree)

            if matches:
                result[sent_idx] = matches

        return result

    def run(self) -> None:
        """
        Search for a pattern in documents and writes found information to JSON file.
        """
        for article in self._corpus_manager.get_articles().values():
            conllu = self._analyzer.from_conllu(article=article)
            doc_graph = self._make_graphs(doc=conllu)

            if self._find_pattern(doc_graphs=doc_graph):
                serializable_patterns = {}
                for sent_idx, matches in self._find_pattern(doc_graphs=doc_graph).items():
                    serializable_matches = []
                    for match in matches:
                        stack: list[tuple[TreeNode, dict | None]] = [(match, None)]
                        root_dict = None
                        while stack:
                            current_node, parent_dict = stack.pop()
                            current_dict = {
                                'upos': current_node.upos,
                                'text': current_node.text,
                                'children': []
                            }
                            if parent_dict is None:
                                root_dict = current_dict
                            else:
                                parent_dict['children'].append(current_dict)

                            for child in reversed(current_node.children):
                                stack.append((child, cast(dict, current_dict)))

                        serializable_matches.append(root_dict)

                    serializable_patterns[sent_idx] = serializable_matches

                article.set_patterns_info(pattern_matches=serializable_patterns)
                to_meta(article=article)


def main() -> None:
    """
    Entrypoint for pipeline module.
    """
    corpus = CorpusManager(path_to_raw_txt_data=ASSETS_PATH)



    udpipe_analyzer = UDPipeAnalyzer()

    udpipe_text_pipeline = TextProcessingPipeline(
        corpus_manager=corpus,
        analyzer=udpipe_analyzer
    )
    udpipe_text_pipeline.run()

    udpipe_pos_pipeline = POSFrequencyPipeline(
        corpus_manager=corpus,
        analyzer=udpipe_analyzer
    )
    udpipe_pos_pipeline.run()

    udpipe_pattern_pipeline = PatternSearchPipeline(
        corpus_manager=corpus,
        analyzer=udpipe_analyzer,
        pos=("VERB", "PROPN", "ADP")
    )
    udpipe_pattern_pipeline.run()



    stanza_analyzer = StanzaAnalyzer()

    stanza_text_pipeline = TextProcessingPipeline(
        corpus_manager=corpus,
        analyzer=stanza_analyzer
    )
    stanza_text_pipeline.run()

    stanza_pos_pipeline = POSFrequencyPipeline(
        corpus_manager=corpus,
        analyzer=stanza_analyzer
    )
    stanza_pos_pipeline.run()

    stanza_pattern_pipeline = PatternSearchPipeline(
        corpus_manager=corpus,
        analyzer=stanza_analyzer,
        pos=("VERB", "PROPN", "ADP")
    )
    stanza_pattern_pipeline.run()


if __name__ == "__main__":
    main()
