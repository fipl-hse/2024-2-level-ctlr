"""
Tests for PatternSearchPipeline using Stanza.
"""

# pylint: disable=duplicate-code
import json
import shutil
import unittest

import pytest

from admin_utils.test_params import PIPE_TEST_FILES_FOLDER, TEST_PATH
from core_utils.article import article
from lab_6_pipeline import pipeline
from lab_6_pipeline.pipeline import CorpusManager, PatternSearchPipeline, StanzaAnalyzer
from lab_6_pipeline.tests.utils import pipeline_test_files_setup


class AdvancedPatternSearchPipelineTests(unittest.TestCase):
    """
    Tests for PatternSearchPipeline using Stanza realization.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Define start instructions for AdvancedPatternSearchPipelineTests class.
        """
        pipeline_test_files_setup()
        shutil.copyfile(
            PIPE_TEST_FILES_FOLDER / "reference_stanza_test.conllu",
            TEST_PATH / "1_stanza_conllu.conllu",
        )

        article.ASSETS_PATH = TEST_PATH
        pipeline.ASSETS_PATH = TEST_PATH

        cls.corpus_manager = CorpusManager(path_to_raw_txt_data=TEST_PATH)
        path_to_article = PIPE_TEST_FILES_FOLDER / "pattern_matches_stanza.json"
        with open(path_to_article, "r", encoding="utf-8") as meta_file:
            cls.patterns = json.load(meta_file)
        print(cls.patterns)

        pattern_searcher = PatternSearchPipeline(
            cls.corpus_manager, StanzaAnalyzer(), ("VERB", "NOUN", "ADP")
        )
        pattern_searcher.run()

    @pytest.mark.mark10
    @pytest.mark.stage_6_advanced_data_processing
    @pytest.mark.lab_6_pipeline
    def test_patterns_are_correct(self) -> None:
        """
        Ensure patterns are matched correctly.
        """
        one_article = self.corpus_manager.get_articles()[1]
        path = one_article.get_meta_file_path()
        print(path)
        with open(path, "r", encoding="utf-8") as meta_file:
            patterns = json.load(meta_file)["pattern_matches"]
        print(self.patterns)
        print(patterns)
        self.assertEqual(self.patterns, patterns, "Patterns were found incorrectly")

    @pytest.mark.skip(reason="Add check")
    @pytest.mark.mark10
    @pytest.mark.stage_6_advanced_data_processing
    @pytest.mark.lab_6_pipeline
    def test_json_structure_depth(self) -> None:
        """
        Ensure the json structure has exact structure.
        """

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Define final instructions for AdvancedPatternSearchPipelineTests class.
        """
        shutil.rmtree(TEST_PATH)
