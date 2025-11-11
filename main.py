"""Main entry point for the ETL application."""


def main():
    """Run the ETL process.
    
    Example implementation:
        from src.application.etl_use_case import ETLUseCase
        from src.domain.interfaces import ConfigLoader
        from src.infrastructure.config_loader import YamlConfigLoader
        
        config_loader: ConfigLoader = YamlConfigLoader()
        config = config_loader.load_dataset_config("bcra_infomondia_series")
        
        etl_use_case = ETLUseCase(...)
        etl_use_case.execute(config)
    """


if __name__ == "__main__":
    main()
