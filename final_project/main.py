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
    project_dir = Path(__file__).parent
    assets_dir = project_dir / "assets/blok"
    output_dir = project_dir / "dist"
    output_file = output_dir / "auto_annotated.conllu"

    analyzer = UDPipeAnalyzer()
    all_conllu: list[str] = []

    sentence_counter = 1
    for txt in assets_dir.glob("*.txt"):
        with open(txt, "r", encoding="utf-8") as file:
            text = file.read().strip()

        conllu_data = analyzer.analyze([text])[0]

        conllu_modified = []
        for line in str(conllu_data).split("\n"):
            if line.startswith("# sent_id = "):
                line = f"# sent_id = {sentence_counter}"
                sentence_counter += 1
            conllu_modified.append(line)

        all_conllu.append("\n".join(conllu_modified).strip())

    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("\n\n".join(all_conllu) + "\n")

if __name__ == "__main__":
    main()
