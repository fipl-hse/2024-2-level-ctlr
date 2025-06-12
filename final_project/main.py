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

    texts = [txt.read_text(encoding="utf-8") for txt in sorted(input_dir.glob("*.txt"))]
    combined_text = "\n".join(texts) + ("\n" if texts else "")

    analyzed_data = UDPipeAnalyzer().analyze([combined_text])[0]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file.write_text(analyzed_data, encoding="utf-8")


if __name__ == "__main__":
    main()
