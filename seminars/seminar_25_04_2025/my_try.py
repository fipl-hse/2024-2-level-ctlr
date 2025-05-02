import pathlib
import shutil
# shutil импортирует copy. и нужен для переноса в новый дерикторий

if __name__ == '__main__':
    print('File:', __file__)
    base_dir = pathlib.Path(__file__).parent
    file_txt = base_dir / 'file.txt'
    print ('BaseDir:', base_dir)
    print('FileTXT:', file_txt.exists())
    file_txt.write_text('File content:')
    print(file_txt.read_text())
    print(f"Absolute path to file.txt: {file_txt.absolute()}")
    print(f"Relative path to file.txt: {file_txt.relative_to(base_dir)}")

    #создать директорий и копирнуть файлы
    new_directory = base_dir / 'new_directory'
    if not new_directory.exists():
        new_directory.mkdir()
    #то, что сверху, для устранения ошибок повторного создания
    #копируем наши файлы в новый директорий
    shutil.copy(file_txt, new_directory / 'copied_file.txt')

    
