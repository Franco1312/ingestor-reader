# Ingestor Reader v3

ETL project following Clean Architecture principles with plugin-based architecture.

## Project Structure

```
.
├── src/
│   ├── domain/              # Domain layer (interfaces/ports)
│   │   └── interfaces.py    # Extractor, Parser, Normalizer, Transformer, Loader
│   ├── application/         # Application layer (use cases)
│   │   ├── etl_use_case.py  # ETL orchestration
│   │   ├── plugin_registry.py  # Plugin registry
│   │   └── config_loader.py    # Configuration loader
│   └── infrastructure/      # Infrastructure layer (implementations)
│       ├── plugins/         # Plugin implementations
│       │   ├── extractors/  # Extractor plugins (http, file, etc.)
│       │   ├── parsers/     # Parser plugins (bcra_infomondia, etc.)
│       │   ├── normalizers/ # Normalizer plugins
│       │   ├── transformers/# Transformer plugins
│       │   └── loaders/     # Loader plugins
│       └── config_loader.py # YAML config loader implementation
├── tests/                   # Test files
├── config/                  # Configuration files
│   └── datasets/            # Dataset configurations (YAML)
└── main.py                  # Entry point
```

## Setup

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install in development mode:
```bash
pip install -e ".[dev]"
```

## Usage

```bash
python main.py
```

## Architecture

This project follows Clean Architecture principles with a plugin-based design:

- **Domain Layer**: Core interfaces (ports) for Extractor, Parser, Normalizer, Transformer, Loader
- **Application Layer**: 
  - Use cases (ETL orchestration)
  - Plugin registry for managing plugins
  - Configuration loader for dataset configs
- **Infrastructure Layer**: 
  - Plugin implementations organized by type
  - Concrete implementations of config loaders

## Plugin System

The project uses a plugin-based architecture where:
- **Extractors**: Modular data extraction (HTTP, file, etc.)
- **Parsers**: Modular data parsing (bcra_infomondia, etc.)
- **Normalizers**: Modular data normalization
- **Transformers**: Modular data transformation
- **Loaders**: Modular data loading

Plugins are registered in the `PluginRegistry` and can be dynamically loaded based on dataset configurations.
