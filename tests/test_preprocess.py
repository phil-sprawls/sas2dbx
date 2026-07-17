import os

from sas_migrate.preprocess import (
    expand_lets, preprocess, resolve_includes, split_steps,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def test_expand_lets_substitutes_and_strips():
    src = "%let cutoff = 2024-01-01;\nwhere d >= \"&cutoff.\"d and x > &cutoff;\n"
    out = expand_lets(src)
    assert '"2024-01-01"d' in out
    assert "x > 2024-01-01" in out
    assert "%let" not in out


def test_resolve_includes_inlines_file():
    src = "%include 'included_macro.sas';\ndata a; set b; run;\n"
    out = resolve_includes(src, base_dir=FIXTURES)
    assert "%macro dedupe" in out
    assert "%include" not in out


def test_split_steps_finds_boundaries_and_kinds():
    steps, _ = preprocess(os.path.join(FIXTURES, "simple_etl.sas"))
    kinds = [s.kind for s in steps]
    assert kinds == ["global", "data", "proc", "proc"]
    assert "create table work.summary" in steps[2].code
    assert steps[1].index == 1


def test_macro_block_is_one_step():
    with open(os.path.join(FIXTURES, "included_macro.sas")) as f:
        steps = split_steps(f.read())
    assert len(steps) == 1
    assert steps[0].kind == "macro"
    assert "%mend" in steps[0].code
