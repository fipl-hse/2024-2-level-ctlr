"""
Interface definitions for text processing pipelines.
"""

# pylint: disable=too-few-public-methods, unused-argument
from dataclasses import dataclass
from typing import Protocol

from core_utils.article.article import Article, ArtifactType


class PipelineProtocol(Protocol):
    """
    Interface definition for pipeline.
    """

    def run(self) -> None:
        """
        Key API method.
        """


class UDPipeDocument(Protocol):
    """
    Utility class to mimic UDPipe Doc.
    """


class StanzaDocument(Protocol):
    """
    Utility class to mimic Stanza Document.
    """


class CoNLLUDocument(UDPipeDocument, StanzaDocument, Protocol):
    """
    Utility class to mimic analyzer document classes.
    """


class AbstractCoNLLUAnalyzer(Protocol):
    """
    Mock definition of library-specific entity.
    """

    def process(self, docs: list[StanzaDocument]) -> list[StanzaDocument]:
        """
        Process given document.

        Args:
            docs (list[StanzaDocument]): Collection of original documents.

        Returns:
            list[StanzaDocument]: Collection of resulting documents.
        """

    def __call__(self, text: str) -> UDPipeDocument:
        """
        Run analyzer as a function.

        Args:
            text (str): Raw document content.

        Returns:
            UDPipeDocument: Output document.
        """


class LibraryWrapper(Protocol):
    """
    Interface definition for text analyzers.
    """

    _analyzer: AbstractCoNLLUAnalyzer

    def _bootstrap(self) -> AbstractCoNLLUAnalyzer:
        """
        Bootstrap analyzer with required models and settings.

        Returns:
            AbstractCoNLLUAnalyzer: Instance of analyzer.
        """

    def analyze(self, texts: list[str]) -> list[CoNLLUDocument | str]:
        """
        Analyze given texts.

        Args:
            texts (list[str]): Texts to analyze.

        Returns:
            list[CoNLLUDocument | str]: Collection of processed documents.
        """

    def to_conllu(self, article: Article) -> None:
        """
        Write ConLLU content to a file.

        Args:
            article (Article): Article to save
        """

    def from_conllu(self, article: Article) -> CoNLLUDocument:
        """
        Load ConLLU content from article stored on disk.

        Args:
            article (Article): Article to load

        Returns:
            CoNLLUDocument: Document ready for parsing
        """

    def get_artifact_type(self) -> ArtifactType:
        """
        Get kind of analyzer.

        Returns:
            ArtifactType: Kind of analyzer
        """


@dataclass
class TreeNode:
    """
    Interface definition for node in the graph.
    """

    upos: str
    text: str
    children: list["TreeNode"]
