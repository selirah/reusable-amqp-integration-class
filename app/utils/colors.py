def color_text(text, code):
    """
    Returns the text wrapped in ANSI color codes.

    Args:
        text (str): The text to color.
        code (str): The ANSI color code to apply.

    Returns:
        str: The colored text.
    """
    return f"\033[{code}m{text}\033[0m"


def green(text): return color_text(text, "32")
def yellow(text): return color_text(text, "33")
def red(text): return color_text(text, "31")
def blue(text): return color_text(text, "34")
def magenta(text): return color_text(text, "35")
def italic(text): return f"\033[3m{text}\033[0m"
def white(text): return color_text(text, "37")
