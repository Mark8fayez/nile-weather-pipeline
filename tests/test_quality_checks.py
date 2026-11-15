from pipeline.quality_checks import CHECKS, Check, Result, failed_errors

ERROR = Check("completeness", "error", "", "")
WARNING = Check("shared_grid_point", "warning", "", "")


def test_a_failed_error_check_fails_the_run():
    results = [Result(ERROR, ["Cairo is missing 2026-11-04"]), Result(WARNING, [])]
    assert [r.check.name for r in failed_errors(results)] == ["completeness"]


def test_a_failed_warning_check_does_not_fail_the_run():
    results = [Result(ERROR, []), Result(WARNING, ["Cairo and Giza share grid point 30.0, 31.2"])]
    assert failed_errors(results) == []


def test_no_problem_rows_means_passed():
    assert Result(ERROR, []).passed
    assert not Result(ERROR, ["something"]).passed


def test_every_check_has_a_known_severity_and_a_unique_name():
    assert {c.severity for c in CHECKS} <= {"error", "warning"}
    assert len({c.name for c in CHECKS}) == len(CHECKS)
