# tests/test_app.py
def test_app_creation(app):
    """
    Verifies that the application is created with the correct config.
    """
    assert app is not None
    assert app.testing is True
