"""
Implementation of POSFrequencyPipeline for score ten only.
"""
import json
import re

from constants import ASSETS_PATH
from core_utils.article import ArtifactType
from core_utils.visualizer import visualize
from pipeline import CorpusManager, validate_dataset


class EmptyFileError(Exception):
    """
    Custom error
    """


class POSFrequencyPipeline:
    def __init__(self, corpus_manager: CorpusManager):
        self.corpus_manager = corpus_manager

    def run(self):
        """
        Runs pipeline process scenario
        """
        for article in self.corpus_manager.get_articles().values():

            with open(article.get_file_path(ArtifactType.multiple_tagged), encoding='utf=8') as file:
                text = file.read()
            if not text:
                raise EmptyFileError

            pos_freq = {}
            pattern = re.compile(r'<([A-Z]+)')

            for pos in pattern.findall(text):
                if pos not in pos_freq:
                    pos_freq[pos] = 1
                else:
                    pos_freq[pos] += 1

            with open(ASSETS_PATH / article.get_meta_file_path(), encoding='utf-8') as file:
                meta = json.load(file)
            meta.update({'pos_frequencies': pos_freq})

            with open(ASSETS_PATH / article.get_meta_file_path(), 'w', encoding='utf-8') as meta_file:
                json.dump(meta, meta_file, sort_keys=False, indent=4, ensure_ascii=False, separators=(',', ':'))

            visualize(statistics=pos_freq, path_to_save=ASSETS_PATH / f'{article.article_id}_image.png')

            print(f'The POS frequency of article #{article.article_id} is counted. URL: {article.url}')


def main():
    validate_dataset(ASSETS_PATH)
    corpus_manager = CorpusManager(ASSETS_PATH)
    pipeline = POSFrequencyPipeline(corpus_manager=corpus_manager)
    pipeline.run()
    print('The POS frequency pipeline is completed')

if __name__ == "__main__":
    main()
