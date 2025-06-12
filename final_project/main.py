"""
Final project implementation.
"""

# pylint: disable=unused-import
from pathlib import Path
from lab_6_pipeline.pipeline import UDPipeAnalyzer


def main() -> None:
    """
    Generate conllu file for provided corpus of texts.
    """
    base_dir = Path(__file__).parent.parent
    input_dir = base_dir / "final_project" / "assets" / "axmatova"
    output_dir = base_dir / "final_project" / "dist"
    output_file = output_dir / "auto_annotated.conllu"

    output_dir.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(input_dir.glob("*.txt"))
    for txt_file in txt_files:
        original_text = txt_file.read_text(encoding="utf-8")
        if original_text and not original_text.endswith('\n'):
            txt_file.write_text(original_text + '\n', encoding="utf-8")

    texts = [f.read_text(encoding="utf-8") for f in txt_files]
    analyzer = UDPipeAnalyzer()
    conllu_results = analyzer.analyze(texts)

    result_text = "\n".join(conllu_results)
    if not result_text.endswith('\n'):
        result_text += '\n'
    output_file.write_text(result_text, encoding="utf-8")


if __name__ == "__main__":
    main()
