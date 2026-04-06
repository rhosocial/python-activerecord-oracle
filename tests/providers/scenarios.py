# tests/providers/scenarios.py
"""Oracle backend test scenario configuration mapping table"""

import os
from typing import Dict, Any, Tuple, Type
from rhosocial.activerecord.backend.impl.oracle import OracleBackend
from rhosocial.activerecord.backend.impl.oracle.config import OracleConnectionConfig

# Scenario name -> configuration dictionary mapping table (Oracle only)
SCENARIO_MAP: Dict[str, Dict[str, Any]] = {}


def register_scenario(name: str, config: Dict[str, Any]):
    """Register Oracle test scenario"""
    SCENARIO_MAP[name] = config


def get_scenario(name: str) -> Tuple[Type[OracleBackend], OracleConnectionConfig]:
    """
    Retrieves the backend class and a connection configuration object for a given
    scenario name. This is called by the provider to set up the database for a test.
    """
    if name not in SCENARIO_MAP:
        # 如果找不到指定的场景，使用第一个可用的场景作为后备
        if SCENARIO_MAP:
            name = next(iter(SCENARIO_MAP))
        else:
            raise ValueError("No scenarios registered")

    # Unpack the configuration dictionary into the dataclass constructor.
    config = OracleConnectionConfig(**SCENARIO_MAP[name])
    return OracleBackend, config


def get_enabled_scenarios() -> Dict[str, Any]:
    """
    Returns the map of all currently enabled scenarios. The testsuite's conftest
    uses this to parameterize the tests, causing them to run for each scenario.
    """
    return SCENARIO_MAP


def _load_scenarios_from_config():
    """
    Load scenarios from a configuration file with the following priority:
    1. Environment variable specified path (highest priority)
    2. Default path tests/config/oracle_scenarios.yaml (lowest priority)
    If no valid configuration file is found, terminate with an error.
    """
    import yaml
    
    # First, try to load from path specified in environment variable
    env_config_path = os.getenv("ORACLE_SCENARIOS_CONFIG_PATH")
    if env_config_path and os.path.exists(env_config_path):
        print(f"Loading Oracle scenarios from environment-specified path: {env_config_path}")
        config_path = env_config_path
    else:
        # Default path with lowest priority
        config_path = os.path.join(os.path.dirname(__file__), "..", "config", "oracle_scenarios.yaml")
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                "No Oracle scenarios configuration file found. "
                "Either set ORACLE_SCENARIOS_CONFIG_PATH environment variable to point to a valid YAML file "
                "or place oracle_scenarios.yaml in the tests/config directory."
            )
        print(f"Loading Oracle scenarios from default path: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        if 'scenarios' not in config_data:
            raise ValueError(f"Configuration file {config_path} does not contain 'scenarios' key")
        
        for scenario_name, config in config_data['scenarios'].items():
            register_scenario(scenario_name, config)
            
    except ImportError:
        raise ImportError("PyYAML is required to load Oracle scenario configuration files")


def _register_default_scenarios():
    """
    Registers the scenarios loaded from external configuration file.
    No hardcoded scenarios are registered in the code itself.
    """
    _load_scenarios_from_config()


# Register default scenarios during initialization
_register_default_scenarios()
