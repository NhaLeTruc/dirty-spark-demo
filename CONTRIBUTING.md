# Contributing to Dirty Spark Pipeline

Thank you for your interest in contributing to the Dirty Spark Data Validation Pipeline! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Testing Guidelines](#testing-guidelines)
- [Code Quality Standards](#code-quality-standards)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)

## Code of Conduct

This project adheres to a code of professional conduct. By participating, you are expected to:

- Be respectful and inclusive in all interactions
- Focus on constructive feedback and collaboration
- Accept responsibility and apologize for mistakes
- Prioritize what's best for the community and project

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Java 11 (required for PySpark)
- Docker and Docker Compose (for local testing)
- PostgreSQL 16.2+ (or use Docker container)
- Git

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/dirty-spark-demo.git
   cd dirty-spark-demo
   ```
3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/dirty-spark-demo.git
   ```

## Development Setup

### 1. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
# Install package in editable mode with dev dependencies
pip install -e ".[dev]"
```

### 3. Set Up Pre-commit Hooks (Optional but Recommended)

```bash
pip install pre-commit
pre-commit install
```

### 4. Start Local Services

```bash
# Start PostgreSQL and other dependencies
docker-compose up -d

# Initialize database schema
python -m src.cli.admin_cli init-db
```

### 5. Verify Setup

```bash
# Run tests to verify everything works
pytest tests/unit/ -v
```

## Development Workflow

### Branch Naming Convention

- Feature branches: `feature/short-description`
- Bug fixes: `fix/short-description`
- Documentation: `docs/short-description`
- Refactoring: `refactor/short-description`

### Commit Message Guidelines

Follow conventional commits format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Example:**
```
feat(validation): add regex pattern validator

Implement regex-based field validation with pattern caching
for improved performance on repeated validations.

Closes #123
```

## Testing Guidelines

### Test Structure

The project uses a three-tier testing strategy:

1. **Unit Tests** (`tests/unit/`): Fast, isolated component tests
2. **Integration Tests** (`tests/integration/`): Tests with external dependencies (database, etc.)
3. **End-to-End Tests** (`tests/e2e/`): Full pipeline workflow tests

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/unit/ -v
pytest tests/integration/ -v
pytest tests/e2e/ -v

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_validators.py -v

# Run specific test function
pytest tests/unit/test_validators.py::test_required_field_validator -v
```

### Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.unit
def test_fast_unit_test():
    pass

@pytest.mark.integration
def test_database_integration():
    pass

@pytest.mark.e2e
def test_full_pipeline():
    pass

@pytest.mark.slow
def test_performance_benchmark():
    pass
```

### Writing Good Tests

1. **Follow AAA Pattern**: Arrange, Act, Assert
2. **Use Descriptive Names**: `test_validator_rejects_invalid_email_format`
3. **Test One Thing**: Each test should verify a single behavior
4. **Use Fixtures**: Leverage pytest fixtures for setup/teardown
5. **Mock External Dependencies**: Use mocks for external APIs, databases in unit tests

**Example:**
```python
import pytest
from src.core.validators.regex_validator import RegexValidator

def test_email_validator_accepts_valid_email():
    # Arrange
    validator = RegexValidator(
        field_name="email",
        pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )

    # Act
    result = validator.validate("user@example.com", {})

    # Assert
    assert result.is_valid is True
    assert len(result.errors) == 0
```

## Code Quality Standards

### Style Guidelines

- Follow [PEP 8](https://peps.python.org/pep-0008/) style guide
- Use **Google-style docstrings**
- Maximum line length: 100 characters
- Use type hints for all function signatures

### Code Formatting

The project uses **Black** for code formatting and **Ruff** for linting:

```bash
# Format code
black src/ tests/

# Check formatting
black --check src/ tests/

# Run linter
ruff check src/ tests/

# Auto-fix linting issues
ruff check src/ tests/ --fix
```

### Type Checking

The project uses **mypy** for static type checking:

```bash
# Run type checker
mypy src/ --ignore-missing-imports
```

### Documentation Standards

All public functions, classes, and modules must have docstrings:

```python
def validate_record(record: dict[str, Any], rules: list[ValidationRule]) -> ValidationResult:
    """Validate a single data record against a set of validation rules.

    Args:
        record: Dictionary containing the data record to validate
        rules: List of ValidationRule objects to apply

    Returns:
        ValidationResult containing validation status and any errors

    Raises:
        ValueError: If record is empty or rules list is None

    Example:
        >>> rules = [RequiredFieldRule("id"), TypeCheckRule("age", "int")]
        >>> result = validate_record({"id": "123", "age": 25}, rules)
        >>> result.is_valid
        True
    """
    # Implementation
```

## Pull Request Process

### Before Submitting

1. **Update your branch** with latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run the full test suite**:
   ```bash
   pytest tests/ -v
   ```

3. **Check code quality**:
   ```bash
   black --check src/ tests/
   ruff check src/ tests/
   mypy src/ --ignore-missing-imports
   ```

4. **Update documentation** if you've changed APIs or added features

5. **Add tests** for new functionality

### Submitting Pull Request

1. Push your changes to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Go to GitHub and create a Pull Request

3. Fill out the PR template with:
   - **Description**: What does this PR do?
   - **Motivation**: Why is this change needed?
   - **Testing**: How was this tested?
   - **Screenshots**: If applicable (UI changes)
   - **Related Issues**: Link to related issues

4. Request review from maintainers

### PR Review Process

- At least one maintainer approval required
- All CI checks must pass
- Code coverage should not decrease
- Follow-up on review comments promptly
- Keep PRs focused and reasonably sized (< 500 lines preferred)

## Release Process

Releases are managed by project maintainers using semantic versioning (SemVer):

- **MAJOR** version: Incompatible API changes
- **MINOR** version: Backwards-compatible functionality
- **PATCH** version: Backwards-compatible bug fixes

### Release Checklist (Maintainers Only)

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release branch: `release/v1.2.3`
4. Run full test suite
5. Create GitHub release with tag
6. Build and publish to PyPI (if applicable)

## Project Structure

```
dirty-spark-demo/
├── src/
│   ├── batch/              # Batch processing components
│   ├── cli/                # Command-line interfaces
│   ├── core/               # Core validation logic
│   │   ├── models/         # Pydantic data models
│   │   ├── rules/          # Validation rules engine
│   │   ├── schema/         # Schema management
│   │   └── validators/     # Field validators
│   ├── observability/      # Logging, metrics, lineage
│   ├── streaming/          # Streaming pipeline
│   │   ├── sinks/          # Streaming sinks
│   │   └── sources/        # Streaming sources
│   ├── utils/              # Utility functions
│   └── warehouse/          # Database operations
├── tests/
│   ├── unit/               # Unit tests
│   ├── integration/        # Integration tests
│   ├── e2e/                # End-to-end tests
│   └── fixtures/           # Test data fixtures
├── config/                 # Configuration files
├── docs/                   # Documentation
└── specs/                  # Feature specifications
```

## Getting Help

- **Documentation**: Check `docs/` directory and README.md
- **Issues**: Search existing issues or create a new one
- **Discussions**: Use GitHub Discussions for questions
- **Slack/Discord**: [Link to community chat if available]

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (MIT License).

---

**Thank you for contributing to Dirty Spark Pipeline!** 🚀