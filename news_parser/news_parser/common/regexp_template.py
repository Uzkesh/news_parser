import re


class RegExp:
    space = re.compile(r"(\s)+")
    tag = re.compile(r"<[^<]+>")
