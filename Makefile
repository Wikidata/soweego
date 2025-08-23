.PHONY: test tests coverage clean

test:
	poetry run pytest

tests:
	poetry run pytest

coverage:
	poetry run pytest --cov-report=term-missing --cov-report=html

clean:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -f coverage.xml
	rm -f .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
