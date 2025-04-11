class IncorrectSeedURLError(Exception):
    """
     Raising an error when a seed URL does not match standard pattern "https?://(www.)?"
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
     Raising an error when total number of articles is out of range from 1 to 150
    """


class IncorrectNumberOfArticlesError(Exception):
    """
     Raising an error when total number of articles to parse is not integer or less than 0
    """


class IncorrectHeadersError(Exception):
    """
     Raising an error when headers are not in a form of dictionary
    """


class IncorrectEncodingError(Exception):
    """
     Raising an error when encoding is not specified as a string
    """


class IncorrectTimeoutError(Exception):
    """
     Raising an error when timeout value is not a positive integer less than 60
    """


class IncorrectVerifyError(Exception):
    """
     Raising an error when verify certificate value is not True or False
    """
