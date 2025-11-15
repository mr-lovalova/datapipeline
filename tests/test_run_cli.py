from datapipeline.cli.commands.run import _run_config_value
from datapipeline.config.run import RunConfig


def test_run_config_value_ignores_model_defaults():
    cfg = RunConfig.model_validate({"version": 1})

    assert _run_config_value(cfg, "visual_provider") is None
    assert _run_config_value(cfg, "progress_style") is None
    assert _run_config_value(cfg, "log_level") is None


def test_run_config_value_respects_explicit_overrides():
    cfg = RunConfig.model_validate(
        {"version": 1, "visual_provider": "rich", "progress_style": "bars", "log_level": "debug"}
    )

    assert _run_config_value(cfg, "visual_provider") == "RICH"
    assert _run_config_value(cfg, "progress_style") == "BARS"
    assert _run_config_value(cfg, "log_level") == "DEBUG"


def test_run_config_value_preserves_explicit_null():
    cfg = RunConfig.model_validate({"version": 1, "visual_provider": None, "progress_style": None})

    assert _run_config_value(cfg, "visual_provider") is None
    assert _run_config_value(cfg, "progress_style") is None
