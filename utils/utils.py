import pickle


def pickle_reader(path):
    with open(path, 'rb') as f:
        return pickle.load(f)
