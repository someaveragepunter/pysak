def str_replace(input: str, subs: dict) -> str:
    for src, tgt in subs.items():
        input = input.replace(src, tgt)
    return input
