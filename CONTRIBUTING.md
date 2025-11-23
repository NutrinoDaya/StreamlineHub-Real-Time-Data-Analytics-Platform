# Contributing to StreamlineHub

Thank you for your interest in contributing to StreamlineHub! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/yourusername/StreamlineHub.git
   cd StreamlineHub
   ```
3. **Create a branch** for your feature:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites
- Docker 24.0+ and Docker Compose 2.21+
- Python 3.11+
- Node.js 20+ (for frontend development)
- Git

### Local Environment

1. Start the development environment:
   ```bash
   docker-compose up -d
   ```

2. Install Python dependencies for local development:
   ```bash
   cd backend
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Install frontend dependencies:
   ```bash
   cd frontend
   npm install
   ```

## Making Changes

### Code Style

**Python**
- Follow PEP 8 style guide
- Use type hints where applicable
- Maximum line length: 100 characters
- Use meaningful variable and function names

**JavaScript/TypeScript**
- Follow ESLint configuration
- Use TypeScript for new components
- Use functional components with hooks

**General**
- Write clear commit messages
- Add comments for complex logic
- Update documentation as needed

### Testing

Before submitting a pull request:

1. **Test your changes**:
   ```bash
   # Backend tests
   pytest backend/tests/
   
   # Frontend tests
   cd frontend && npm test
   ```

2. **Check code formatting**:
   ```bash
   # Python
   black backend/src/
   flake8 backend/src/
   
   # JavaScript/TypeScript
   cd frontend && npm run lint
   ```

3. **Verify the system works end-to-end**:
   ```bash
   docker-compose up -d
   # Test data generation
   docker exec streamlinehub-backend python3 /app/scripts/dynamic_kafka_producer.py --count 100
   # Verify in frontend at http://localhost:3000
   ```

## Pull Request Process

1. **Update documentation**:
   - Update README.md if you've changed functionality
   - Add comments to complex code
   - Update API documentation if endpoints changed

2. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```
   
   Use conventional commit messages:
   - `feat:` - New feature
   - `fix:` - Bug fix
   - `docs:` - Documentation only
   - `style:` - Code style changes
   - `refactor:` - Code refactoring
   - `test:` - Adding tests
   - `chore:` - Maintenance tasks

3. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create a Pull Request**:
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill in the PR template with:
     - Description of changes
     - Related issues
     - Testing done
     - Screenshots (if UI changes)

5. **Address review feedback**:
   - Make requested changes
   - Push updates to your branch
   - Respond to comments

## Areas for Contribution

### High Priority
- Performance optimizations for Spark jobs
- Additional data quality checks
- More comprehensive test coverage
- Documentation improvements

### Feature Ideas
- Real-time alerting system
- Additional data source connectors
- Machine learning model integration
- Advanced visualization components
- Data lineage tracking

### Bug Fixes
- Check the [Issues](https://github.com/yourusername/StreamlineHub/issues) page for bugs
- Look for issues labeled "good first issue" or "help wanted"

## Code Review Guidelines

All submissions require code review. We use GitHub pull requests for this purpose.

**For Reviewers:**
- Be constructive and respectful
- Focus on code quality, not personal preferences
- Test the changes locally if possible
- Provide specific, actionable feedback

**For Contributors:**
- Be responsive to feedback
- Don't take criticism personally
- Ask questions if feedback is unclear
- Update your PR based on feedback

## Documentation

When adding new features:
- Update the README.md with usage examples
- Add docstrings to Python functions/classes
- Add JSDoc comments to JavaScript functions
- Update API documentation
- Add architecture diagrams if needed

## Community Guidelines

- Be respectful and inclusive
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md)
- Help others learn and grow
- Give credit where credit is due

## Questions?

- Open an issue for bugs or feature requests
- Join our discussions for questions
- Email: support@streamlinehub.example.com

Thank you for contributing to StreamlineHub! 🎉
