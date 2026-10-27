# Contributing to Scalable Unstructured Data Pipeline

Thank you for your interest in contributing! Please read this guide before opening a PR.

## Development Setup

1. **Fork & clone** the repository and install local dependencies:

   ```bash
   git clone https://github.com/<your-handle>/pipeline.git
   cd pipeline
   python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Copy the environment template** and fill in your values:

   ```bash
   cp .env.example .env
   # edit .env with your keys — this file is git-ignored
   ```

3. **Set up pre-commit hooks** (optional but encouraged):

   ```bash
   pip install pre-commit
   pre-commit install
   ```

## Branch Naming

| Type | Pattern | Example |
|---|---|---|
| Feature | `feat/<short-desc>` | `feat/add-jpeg2000-support` |
| Bug fix | `fix/<short-desc>` | `fix/dlq-retry-logic` |
| Infra / Terraform | `infra/<short-desc>` | `infra/add-kms-encryption` |
| Docs | `docs/<short-desc>` | `docs/update-cost-table` |

## Code Standards

- **Python**: follow [PEP 8](https://peps.python.org/pep-0008/). Run `flake8 .` before committing.
- **Terraform**: run `terraform fmt` inside `terraform/` before committing.
- **No hardcoded credentials** — all secrets must come from environment variables.
- Add or update docstrings for any public function you touch.

## Running Tests

```bash
# Unit / integration (requires env vars — see .env.example)
python -m pytest test_pipeline.py -v
```

## Pull Request Checklist

- [ ] Code follows style guidelines above
- [ ] All tests pass locally
- [ ] No secrets or credentials committed
- [ ] README / docs updated if behaviour changes
- [ ] Terraform formatted (`terraform fmt`)

## Reporting Issues

Open a GitHub Issue with:
- A clear title describing the problem
- Steps to reproduce
- Expected vs actual behaviour
- Relevant log output (redact any sensitive data)
