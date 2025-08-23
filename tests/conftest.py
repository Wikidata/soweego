"""Shared pytest fixtures and configuration for all tests."""

import os
import tempfile
from pathlib import Path
from typing import Dict, Generator, List
from unittest.mock import MagicMock, Mock

import pytest
from click.testing import CliRunner


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files.
    
    Yields:
        Path: Path to the temporary directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_file(temp_dir: Path) -> Generator[Path, None, None]:
    """Create a temporary file for testing.
    
    Args:
        temp_dir: The temporary directory fixture.
        
    Yields:
        Path: Path to the temporary file.
    """
    temp_path = temp_dir / "test_file.txt"
    temp_path.write_text("test content")
    yield temp_path


@pytest.fixture
def mock_config() -> Dict[str, str]:
    """Provide a mock configuration dictionary.
    
    Returns:
        Dict[str, str]: A dictionary with test configuration values.
    """
    return {
        "database_url": "sqlite:///:memory:",
        "api_key": "test_api_key",
        "api_secret": "test_api_secret",
        "batch_size": "100",
        "timeout": "30",
        "debug": "true",
        "log_level": "DEBUG",
        "output_dir": "/tmp/test_output",
    }


@pytest.fixture
def mock_database_session():
    """Create a mock database session.
    
    Returns:
        MagicMock: A mock SQLAlchemy session object.
    """
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None
    session.query.return_value.filter.return_value.all.return_value = []
    session.query.return_value.count.return_value = 0
    session.add = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client for API testing.
    
    Returns:
        Mock: A mock requests-like object.
    """
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    client.get.return_value = response
    client.post.return_value = response
    client.put.return_value = response
    client.delete.return_value = response
    return client


@pytest.fixture
def sample_entity_data() -> List[Dict]:
    """Provide sample entity data for testing.
    
    Returns:
        List[Dict]: A list of sample entity dictionaries.
    """
    return [
        {
            "id": "Q1",
            "label": "Test Entity 1",
            "description": "A test entity for unit tests",
            "aliases": ["TE1", "Entity One"],
            "properties": {
                "P31": "Q5",  # instance of human
                "P569": "1990-01-01",  # date of birth
            },
        },
        {
            "id": "Q2",
            "label": "Test Entity 2",
            "description": "Another test entity",
            "aliases": ["TE2", "Entity Two"],
            "properties": {
                "P31": "Q5",
                "P569": "1985-06-15",
            },
        },
    ]


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI test runner.
    
    Returns:
        CliRunner: A Click test runner instance.
    """
    return CliRunner()


@pytest.fixture
def mock_wikidata_api():
    """Mock Wikidata API responses.
    
    Returns:
        Mock: A mock object simulating Wikidata API.
    """
    api = Mock()
    api.get_entity.return_value = {
        "id": "Q42",
        "labels": {"en": {"value": "Douglas Adams"}},
        "descriptions": {"en": {"value": "English writer"}},
        "claims": {},
    }
    api.search.return_value = {
        "search": [
            {"id": "Q42", "label": "Douglas Adams"},
            {"id": "Q43", "label": "Another Result"},
        ]
    }
    api.create_claim.return_value = {"success": True, "claim": {"id": "test_claim_id"}}
    return api


@pytest.fixture
def sample_csv_data(temp_dir: Path) -> Path:
    """Create a sample CSV file for testing.
    
    Args:
        temp_dir: The temporary directory fixture.
        
    Returns:
        Path: Path to the created CSV file.
    """
    csv_path = temp_dir / "test_data.csv"
    csv_content = """id,name,birth_date,occupation
1,John Doe,1990-01-01,Engineer
2,Jane Smith,1985-06-15,Scientist
3,Bob Johnson,1978-03-22,Artist
"""
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def sample_json_data(temp_dir: Path) -> Path:
    """Create a sample JSON file for testing.
    
    Args:
        temp_dir: The temporary directory fixture.
        
    Returns:
        Path: Path to the created JSON file.
    """
    import json
    
    json_path = temp_dir / "test_data.json"
    json_data = {
        "entities": [
            {"id": 1, "name": "Test 1", "type": "person"},
            {"id": 2, "name": "Test 2", "type": "organization"},
        ],
        "metadata": {
            "version": "1.0",
            "created": "2024-01-01",
        },
    }
    json_path.write_text(json.dumps(json_data, indent=2))
    return json_path


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables before each test.
    
    This fixture automatically runs before each test to ensure
    a clean environment state.
    """
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_logger():
    """Create a mock logger for testing logging behavior.
    
    Returns:
        Mock: A mock logger object.
    """
    logger = Mock()
    logger.debug = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.critical = Mock()
    return logger


@pytest.fixture
def isolated_filesystem(tmp_path: Path, monkeypatch) -> Path:
    """Create an isolated filesystem for testing.
    
    Args:
        tmp_path: pytest's built-in tmp_path fixture.
        monkeypatch: pytest's monkeypatch fixture.
        
    Returns:
        Path: Path to the isolated directory.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def mock_sparql_results():
    """Mock SPARQL query results.
    
    Returns:
        Dict: A dictionary simulating SPARQL results.
    """
    return {
        "head": {"vars": ["item", "itemLabel", "value"]},
        "results": {
            "bindings": [
                {
                    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q42"},
                    "itemLabel": {"type": "literal", "value": "Douglas Adams"},
                    "value": {"type": "literal", "value": "42"},
                },
                {
                    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q43"},
                    "itemLabel": {"type": "literal", "value": "Test Item"},
                    "value": {"type": "literal", "value": "123"},
                },
            ]
        },
    }


def pytest_configure(config):
    """Configure pytest with custom settings."""
    config.addinivalue_line(
        "markers", "network: mark test as requiring network access"
    )
    config.addinivalue_line(
        "markers", "database: mark test as requiring database access"
    )
    config.addinivalue_line(
        "markers", "wikidata: mark test as requiring Wikidata API access"
    )