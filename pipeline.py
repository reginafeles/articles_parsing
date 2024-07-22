"""
Pipeline for text processing implementation
"""
from pathlib import Path
import re

import pymorphy2
from pymystem3 import Mystem

from constants import ASSETS_PATH
from core_utils.article import Article, ArtifactType


class EmptyDirectoryError(Exception):
    """
    No data to process
    """


class InconsistentDatasetError(Exception):
    """
    Corrupt data:
        - numeration is expected to start from 1 and to be continuous
        - a number of text files must be equal to the number of meta files
        - text files must not be empty
    """


class MorphologicalToken:
    """
    Stores language params for each processed token
    """

    def __init__(self, original_word):
        self.original_word = original_word
        self.normalized_form = ''
        self.tags_mystem = ''
        self.tags_pymorphy = ''

    def get_cleaned(self):
        """
        Returns lowercased original form of a token
        """
        return self.original_word.lower()

    def get_single_tagged(self):
        """
        Returns normalized lemma with MyStem tags
        """
        return f'{self.normalized_form}({self.tags_pymorphy})'

    def get_multiple_tagged(self):
        """
        Returns normalized lemma with PyMorphy tags
        """
        return f'{self.normalized_form}<{self.tags_mystem}>({self.tags_pymorphy})'


class CorpusManager:
    """
    Works with articles and stores them
    """

    def __init__(self, path_to_raw_txt_data: str):
        self.path_to_raw_txt_data = path_to_raw_txt_data
        self._storage = {}
        self._scan_dataset()

    def _scan_dataset(self):
        """
        Register each dataset entry
        """
        path = Path(self.path_to_raw_txt_data)
        pattern = re.compile(r'\d+')
        for file in path.glob('*'):
            if '_raw.txt' not in file.name:
                continue
            article_id = int(pattern.search(file.name)[0])
            self._storage[article_id] = Article(url=None, article_id=article_id)

    def get_articles(self):
        """
        Returns storage params
        """
        return self._storage


class TextProcessingPipeline:
    """
    Process articles from corpus manager
    """

    def __init__(self, corpus_manager: CorpusManager):
        self.corpus_manager = corpus_manager

    def run(self):
        """
        Runs pipeline process scenario
        """

        for article in self.corpus_manager.get_articles().values():
            article_raw_text = article.get_raw_text()

            cleaned_tokens = []
            single_tagged_tokens = []
            multiple_tagged_tokens = []

            for token in self._process(article_raw_text):
                cleaned_tokens.append(token.get_cleaned())
                single_tagged_tokens.append(token.get_single_tagged())
                multiple_tagged_tokens.append(token.get_multiple_tagged())

            article.save_as(' '.join(cleaned_tokens), ArtifactType.cleaned)
            article.save_as(' '.join(single_tagged_tokens), ArtifactType.single_tagged)
            article.save_as(' '.join(multiple_tagged_tokens), ArtifactType.multiple_tagged)

            print(f'The pipeline for article #{article.article_id} is completed. URL: {article.url}')

    def _process(self, raw_text: str):
        """
        Processes each token and creates MorphToken class instance
        """
        text = raw_text.replace('-\n', '').replace('\n', ' ')

        analyzed_text = Mystem().analyze(text)
        morph = pymorphy2.MorphAnalyzer()

        tokens = []
        for token in analyzed_text:
            if 'analysis' not in token or not token['analysis'] \
                    or 'lex' not in token['analysis'][0] \
                    or 'gr' not in token['analysis'][0]:
                continue

            morph_token = MorphologicalToken(original_word=token['text'])
            morph_token.normalized_form = token['analysis'][0]['lex']
            morph_token.tags_mystem = token['analysis'][0]['gr']

            parsed = morph.parse(token['text'])
            if not parsed:
                continue

            morph_token.tags_pymorphy = parsed[0].tag
            tokens.append(morph_token)
        return tokens


def validate_dataset(path_to_validate):
    """
    Validates folder with assets
    """
    path = Path(path_to_validate)

    if not path.exists():
        raise FileNotFoundError

    if not path.is_dir():
        raise NotADirectoryError

    if not list(path.iterdir()):
        raise EmptyDirectoryError

    numeration = []
    number_raw_txt = 0
    number_meta = 0

    pattern = re.compile(r'(\d+)')

    for file in path.glob('*'):
        new_pattern = pattern.match(file.name)
        if not new_pattern:
            raise InconsistentDatasetError

        number = int(new_pattern.group(1))
        numeration.append(number)

        if file.name.endswith('raw.txt'):
            number_raw_txt += 1
        if file.name.endswith('meta.json'):
            number_meta += 1

        if '_raw.txt' in file.name and file.stat().st_size == 0:
            raise InconsistentDatasetError

    sorted_numeration = sorted(set(numeration))
    if sorted_numeration[0] != 1:
        raise InconsistentDatasetError
    previous = 0
    for number in sorted_numeration:
        if number - previous != 1:
            raise InconsistentDatasetError
        previous = number

    if number_raw_txt != number_meta:
        raise InconsistentDatasetError


def main():
    validate_dataset(ASSETS_PATH)
    corpus_manager = CorpusManager(ASSETS_PATH)
    pipeline = TextProcessingPipeline(corpus_manager)
    pipeline.run()

    print('Pipeline is completed')


if __name__ == "__main__":
    main()
