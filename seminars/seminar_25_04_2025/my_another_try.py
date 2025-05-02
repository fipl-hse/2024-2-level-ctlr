import pathlib
from datetime import datetime

if __name__ == '__main__':
    base_dir = pathlib.Path(__file__).parent
    file_txt = base_dir / 'file.txt'
    if not file_txt.exists():
        file_txt.write_text('File content')