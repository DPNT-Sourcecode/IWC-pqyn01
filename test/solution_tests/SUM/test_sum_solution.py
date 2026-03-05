from solutions.SUM.sum_solution import SumSolution


class TestSum():
    def test_sum(self):
        assert SumSolution().compute(1, 2) == 3

    def test_sum_zeros(self):
        assert SumSolution().compute(0, 0) == 0

    def test_sum_boundary_upper(self):
        assert SumSolution().compute(100, 100) == 200

    def test_sum_mixed_boundaries(self):
        assert SumSolution().compute(0, 100) == 100
        assert SumSolution().compute(100, 0) == 100

    def test_sum_typical_values(self):
        assert SumSolution().compute(50, 50) == 100
        assert SumSolution().compute(25, 75) == 100

    def test_sum_returns_integer(self):
        result = SumSolution().compute(1, 2)
        assert isinstance(result, int)
