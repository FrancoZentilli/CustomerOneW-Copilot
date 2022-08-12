from pathlib import Path
import os

PATH_MAIN = Path(os.path.dirname(__file__))

if __name__ == '__main__':
    print("the main path is: {}".format(PATH_MAIN))
    local_vars = locals().copy()
    paths = {}
    for k, v in local_vars.items():
        if k.startswith("PATH_"):
            path = Path(v)
            if path.is_dir():
                print("directory {} already exists".format(v))
            else:
                os.mkdir(path)
                print("directory {} created".format(v))