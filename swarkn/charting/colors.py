import re
from itertools import cycle


def color_cfg(cfg: dict, col: str):
    for regex, color in cfg.items():
        if re.search(regex, col):
            return color
    return None

def cycle_colors(colors: list):
    cyc = cycle(colors)
    return lambda *_: next(cyc)

