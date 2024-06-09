# Development

The `dev.py` assists debugging and developing the library's methods.

## Installation

### Requirements

- Python 3.9 or above
- Poetry
- `.env` file

  ```dotenv
  DB_USER=
  DB_PASSWORD=
  DB_HOST=
  DB_NAME=
    
  ETANI_DB_USER=
  ETANI_DB_PASSWORD=
  ETANI_DB_SERVER=
  ETANI_DB_NAME=
  ```
- `queries.json`

#### Installing dependencies and `vetbiz_extractor` module

```bash
pip install poetry
poetry install
```

#### Building wheel file

```bash
poetry build
```

Built wheel file is located in `dist` folder

## Usage

Fetch full records

```bash
poetry run python dev.py
```

Fetch limited records

```bash
poetry run python dev.py --limit <int> # e.g 10000
```