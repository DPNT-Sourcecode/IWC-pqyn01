import pytest
from solutions.HLO.hello_solution import HelloSolution

@pytest.mark.parametrize("friend_name, expected_output", [
    ("World", "Hello, World!"),
    ("Alice", "Hello, Alice!"),
    ("Bob", "Hello, Bob!"),
    ("John", "Hello, John!"),
])
class TestHello():
    def test_hello(self, friend_name, expected_output):
        assert HelloSolution().hello(friend_name) == expected_output
