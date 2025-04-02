from core_utils.pipeline import StanzaDocument, UDPipeDocument

class Pipeline:
    def __init__(
        self, lang: str, processors: str, logging_level: str, download_method: None = None
    ) -> None:
        """
        Initializes instance.
        """

    def process(self, text: StanzaDocument) -> list[StanzaDocument]:
        """
        Explicit processing.
        """
    # This is very strange to have one library to be leaked to another.
    # This item has to be addressed in the future.
    def __call__(self, text: str) -> UDPipeDocument:
        """
        Call instance as a function to process.
        """
