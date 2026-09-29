def test_skeleton_imports() -> None:
    import main  # noqa: F401
    from app import (  # noqa: F401
        api,
        config,
        handlers,
        logging_setup,
        models,
        queue,
        store,
    )
