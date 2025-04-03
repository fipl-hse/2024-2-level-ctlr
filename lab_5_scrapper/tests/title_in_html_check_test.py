"""
Title tests validation.
"""

import unittest

from lab_5_scrapper.tests.s2_5_raw_metadata_test import check_title_in_html


class TestCheckTitleIsCorrect(unittest.TestCase):
    """
    Title check tests.
    """

    sample_html = """
<!DOCTYPE html>
    <html>
        <body>
            <h1>Lorem ipsum dolor sit amet consectetuer</h1>
        </body>
</html>
"""

    def test_title_check_parses_non_breaking_space(self) -> None:
        """
        Ensure that non-breaking space is parsable.
        """
        title_name_code = "Lorem&nbsp;ipsum&nbsp;dolor&nbsp;sit&nbsp;amet&nbsp;"
        msg = "Title tests cannot parse name code of a non-breaking space: &nbsp;"
        self.assertTrue(check_title_in_html(title_name_code, self.sample_html), msg)

        title_decimal_code = "Lorem&#160;ipsum&#160;dolor&#160;sit&#160;amet&#160;"
        msg = "Title tests cannot parse decimal code of a non-breaking space: &#160;"
        self.assertTrue(check_title_in_html(title_decimal_code, self.sample_html), msg)

        title_hex_code = "Lorem&#xA0;ipsum&#xA0;dolor&#xA0;sit&#xA0;amet&#xA0;"
        msg = "Title tests cannot parse hex code of a non-breaking space: &#xA0;"
        self.assertTrue(check_title_in_html(title_hex_code, self.sample_html), msg)

    def test_title_check_parses_line_space(self) -> None:
        """
        Ensure that line space is parsable.
        """
        title_decimal_code = "Lorem&#9251;ipsum&#9251;dolor&#9251;sit&#9251;amet&#9251;"
        msg = "Title tests cannot parse decimal code of a line space: &#9251;"
        self.assertTrue(check_title_in_html(title_decimal_code, self.sample_html), msg)

        title_hex_code = "Lorem&#x2423;ipsum&#x2423;dolor&#x2423;sit&#x2423;amet&#x2423;"
        msg = "Title tests cannot parse hex code of a line space: &#x2423;"
        self.assertTrue(check_title_in_html(title_hex_code, self.sample_html), msg)

    def test_title_check_parses_symbol_for_space(self) -> None:
        """
        Ensure that space symbol is parsable.
        """
        title_decimal_code = "Lorem&#9248;ipsum&#9248;dolor&#9248;sit&#9248;amet&#9248;"
        msg = "Title tests cannot parse decimal code of a HTML space symbol: &#9248;"
        self.assertTrue(check_title_in_html(title_decimal_code, self.sample_html), msg)

        title_hex_code = "Lorem&#x2420;ipsum&#x2420;dolor&#x2420;sit&#x2420;amet&#x2420;"
        msg = "Title tests cannot parse hex code of a HTML space symbol: &#x2420;"
        self.assertTrue(check_title_in_html(title_hex_code, self.sample_html), msg)

    def test_title_check_parses_html_space(self) -> None:
        """
        Ensure that html space is parsable.
        """
        title_decimal_code = "Lorem&#32;ipsum&#32;dolor&#32;sit&#32;amet&#32;"
        msg = "Title tests cannot parse decimal code of a HTML space: &#32;"
        self.assertTrue(check_title_in_html(title_decimal_code, self.sample_html), msg)

        title_hex_code = "Lorem&#x20;ipsum&#x20;dolor&#x20;sit&#x20;amet&#x20;"
        msg = "Title tests cannot parse hex code of a HTML space: &#x20;"
        self.assertTrue(check_title_in_html(title_hex_code, self.sample_html), msg)

    def test_title_check_parses_python_space_symbols(self) -> None:
        """
        Ensure that python space symbols are parsable.
        """
        title = "\x0cLorem ipsum\ndolor\tsit\ramet\x0b"
        msg = r"Title tests cannot parse decimal code of a HTML space: \t, \n, \r, \x0b, \x0c"
        self.assertTrue(check_title_in_html(title, self.sample_html), msg)
