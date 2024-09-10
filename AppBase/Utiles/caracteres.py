import re
from unicodedata import normalize

import re

# Make a regular expression
# for validating an Email
regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'


# Define a function for
# for validating an Email
def check_email(email):
    # pass the regular expression
    # and the string into the fullmatch() method
    if (re.fullmatch(regex, email)):
        return True
    else:
        return False


def procesar_cadena(input):
    if len(input)<1:
        return ''

    str = clean_diacritical(input)

    str = str.replace("  ", " ")
    str = str.title().strip()

    return str

def clean_diacritical(input):
    s = re.sub(
        r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1",
        normalize("NFD", input), 0, re.I
    )

    # -> NFC
    s = normalize('NFC', s)
    return s


def clean_email(input):
    if input.__contains__('@'):
        inds = input.split('@')
        return clean_diacritical(inds[0].lower())
    else:
        return ''


def emmbe_arr(embed_model, input):
    xemb = embed_model.encode(input)
    arr = []
    for i in range(0, len(xemb)):
        arr.append(xemb[i])
    return arr
