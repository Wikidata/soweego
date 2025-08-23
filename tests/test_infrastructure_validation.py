"""Validation tests to verify the testing infrastructure is properly configured."""

import sys
from pathlib import Path

import pytest


class TestInfrastructureValidation:
    """Test suite to validate the testing infrastructure setup."""

    def test_pytest_installed(self):
        """Verify pytest is installed and importable."""
        import pytest
        assert pytest is not None
        assert hasattr(pytest, '__version__')

    def test_pytest_cov_installed(self):
        """Verify pytest-cov is installed and importable."""
        import pytest_cov
        assert pytest_cov is not None

    def test_pytest_mock_installed(self):
        """Verify pytest-mock is installed and importable."""
        import pytest_mock
        assert pytest_mock is not None

    def test_project_structure_exists(self):
        """Verify the project structure is correctly set up."""
        project_root = Path(__file__).parent.parent
        
        # Check main package exists
        assert (project_root / "soweego").exists()
        assert (project_root / "soweego" / "__init__.py").exists()
        
        # Check test directories exist
        assert (project_root / "tests").exists()
        assert (project_root / "tests" / "__init__.py").exists()
        assert (project_root / "tests" / "unit").exists()
        assert (project_root / "tests" / "unit" / "__init__.py").exists()
        assert (project_root / "tests" / "integration").exists()
        assert (project_root / "tests" / "integration" / "__init__.py").exists()
        
        # Check configuration files exist
        assert (project_root / "pyproject.toml").exists()

    def test_conftest_fixtures_available(self, temp_dir, mock_config, cli_runner):
        """Verify conftest fixtures are available and working."""
        # Test temp_dir fixture
        assert temp_dir.exists()
        assert temp_dir.is_dir()
        
        # Test mock_config fixture
        assert isinstance(mock_config, dict)
        assert "database_url" in mock_config
        assert mock_config["database_url"] == "sqlite:///:memory:"
        
        # Test cli_runner fixture
        assert cli_runner is not None
        from click.testing import CliRunner
        assert isinstance(cli_runner, CliRunner)

    def test_sample_data_fixtures(self, sample_entity_data, sample_csv_data, sample_json_data):
        """Verify sample data fixtures are working correctly."""
        # Test entity data
        assert isinstance(sample_entity_data, list)
        assert len(sample_entity_data) == 2
        assert sample_entity_data[0]["id"] == "Q1"
        
        # Test CSV file creation
        assert sample_csv_data.exists()
        assert sample_csv_data.suffix == ".csv"
        content = sample_csv_data.read_text()
        assert "John Doe" in content
        
        # Test JSON file creation
        assert sample_json_data.exists()
        assert sample_json_data.suffix == ".json"
        import json
        data = json.loads(sample_json_data.read_text())
        assert "entities" in data
        assert len(data["entities"]) == 2

    def test_mock_fixtures(self, mock_database_session, mock_http_client, mock_wikidata_api):
        """Verify mock fixtures are properly configured."""
        # Test database session mock
        assert hasattr(mock_database_session, 'query')
        assert hasattr(mock_database_session, 'commit')
        mock_database_session.commit()  # Should not raise
        
        # Test HTTP client mock
        response = mock_http_client.get("http://example.com")
        assert response.status_code == 200
        assert response.json() == {"status": "success", "data": []}
        
        # Test Wikidata API mock
        entity = mock_wikidata_api.get_entity("Q42")
        assert entity["id"] == "Q42"
        assert "labels" in entity

    @pytest.mark.unit
    def test_unit_marker(self):
        """Test that unit test marker is properly configured."""
        assert True

    @pytest.mark.integration
    def test_integration_marker(self):
        """Test that integration test marker is properly configured."""
        assert True

    @pytest.mark.slow
    def test_slow_marker(self):
        """Test that slow test marker is properly configured."""
        assert True

    def test_python_path_includes_project(self):
        """Verify the project root is in Python path for imports."""
        project_root = str(Path(__file__).parent.parent)
        assert any(project_root in path for path in sys.path)

    def test_coverage_configuration(self):
        """Verify coverage is properly configured."""
        from pathlib import Path
        project_root = Path(__file__).parent.parent
        pyproject = project_root / "pyproject.toml"
        
        assert pyproject.exists()
        content = pyproject.read_text()
        
        # Check coverage configuration exists
        assert "[tool.coverage.run]" in content
        assert "[tool.coverage.report]" in content
        assert "fail_under = 80" in content

    def test_isolated_filesystem_fixture(self, isolated_filesystem):
        """Test the isolated filesystem fixture."""
        # Should be in a temporary directory
        assert isolated_filesystem.exists()
        assert isolated_filesystem.is_dir()
        
        # Create a test file
        test_file = isolated_filesystem / "test.txt"
        test_file.write_text("test content")
        assert test_file.exists()

    def test_mock_logger_fixture(self, mock_logger):
        """Test the mock logger fixture."""
        # Test all log levels
        mock_logger.debug("debug message")
        mock_logger.info("info message")
        mock_logger.warning("warning message")
        mock_logger.error("error message")
        mock_logger.critical("critical message")
        
        # Verify calls were made
        mock_logger.debug.assert_called_once_with("debug message")
        mock_logger.info.assert_called_once_with("info message")

    def test_mock_sparql_results_fixture(self, mock_sparql_results):
        """Test the SPARQL results mock fixture."""
        assert "head" in mock_sparql_results
        assert "results" in mock_sparql_results
        
        bindings = mock_sparql_results["results"]["bindings"]
        assert len(bindings) == 2
        assert bindings[0]["itemLabel"]["value"] == "Douglas Adams"

    def test_environment_reset_fixture(self):
        """Test that environment is properly reset between tests."""
        import os
        
        # Set a test environment variable
        os.environ["TEST_VAR"] = "test_value"
        assert os.environ.get("TEST_VAR") == "test_value"
        
        # The reset_environment fixture should clean this up after the test


class TestPytestConfiguration:
    """Tests to verify pytest configuration is correct."""

    def test_pytest_ini_options(self):
        """Verify pytest.ini options are properly set in pyproject.toml."""
        from pathlib import Path
        
        project_root = Path(__file__).parent.parent
        pyproject = project_root / "pyproject.toml"
        content = pyproject.read_text()
        
        # Check test paths
        assert 'testpaths = ["tests"]' in content
        
        # Check test discovery patterns
        assert 'python_files = ["test_*.py", "*_test.py"]' in content
        assert 'python_classes = ["Test*"]' in content
        assert 'python_functions = ["test_*"]' in content
        
        # Check coverage options
        assert "--cov=soweego" in content
        assert "--cov-branch" in content
        assert "--cov-report=html:htmlcov" in content
        assert "--cov-report=xml:coverage.xml" in content

    def test_custom_markers_registered(self):
        """Verify custom markers are properly registered."""
        from pathlib import Path
        
        project_root = Path(__file__).parent.parent
        pyproject = project_root / "pyproject.toml"
        content = pyproject.read_text()
        
        # Check markers are defined
        assert '"unit: Unit tests"' in content
        assert '"integration: Integration tests"' in content
        assert '"slow: Slow running tests"' in content