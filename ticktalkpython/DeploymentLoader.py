# Copyright 2021 The Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
DeploymentLoader - Generic deployment topology loader from YAML configuration.

Eliminates hardcoded device topology creation. Works with any deployment
scenario by loading device specifications from YAML files.

Usage:
    loader = DeploymentLoader('deployment_multi.yaml')
    ensembles = loader.load_ensembles()
"""

import yaml
from typing import Dict, List, Optional
from . import DebugLogger
from .Ensemble import TTEnsembleInfo

logger = DebugLogger.get_logger('DeploymentLoader')


class DeploymentLoader:
    """
    Loads device deployment topology from YAML configuration.
    
    Creates TTEnsembleInfo objects for each device in the deployment,
    making it easy to test different topologies without code changes.
    """
    
    def __init__(self, deployment_file: str):
        """
        Load deployment configuration from YAML file.
        
        Expected YAML format:
```yaml
        application:
          name: "my_deployment"
          description: "Description of deployment scenario"
        
        devices:
          - id: device_1
            type: jetson_nano
            location: "edge"
            components:
              camera: true
              gpio: true
          
          - id: device_2
            type: server_x86
            location: "cloud"
            components:
              gpu: true
              nvme: true
        
        network:
          - link: [device_1, device_2]
            type: wireless_5g
```
        
        :param deployment_file: Path to deployment YAML
        """
        self.deployment_file = deployment_file
        self.deployment_config = self._load_config()
        self.devices_config = self.deployment_config.get('devices', [])
        self.network_config = self.deployment_config.get('network', [])
        
        logger.info(f"DeploymentLoader initialized: {len(self.devices_config)} devices")
    
    def _load_config(self) -> Dict:
        """Load and validate YAML configuration."""
        try:
            with open(self.deployment_file, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config:
                logger.error(f"Empty deployment file: {self.deployment_file}")
                raise ValueError("Empty deployment configuration")
            
            if 'devices' not in config:
                logger.error(f"No 'devices' section in {self.deployment_file}")
                raise ValueError("Missing 'devices' section in deployment config")
            
            return config
        
        except FileNotFoundError:
            logger.error(f"Deployment file not found: {self.deployment_file}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in {self.deployment_file}: {e}")
            raise
    
    def load_ensembles(self) -> Dict[str, TTEnsembleInfo]:
        """
        Load all devices as TTEnsembleInfo objects.
        
        Creates ensemble dictionary compatible with UnifiedGraph.
        
        :return: Dict mapping device_id -> TTEnsembleInfo
        """
        ensembles = {}
        
        for device_spec in self.devices_config:
            device_id = device_spec.get('id')
            if not device_id:
                logger.warning(f"Device missing 'id' field, skipping: {device_spec}")
                continue
            
            ensemble_info = self._create_ensemble_info(device_spec)
            ensembles[device_id] = ensemble_info
            
            # Log device creation
            caps = [k for k, v in ensemble_info.components.items() if v]
            logger.debug(f"Loaded device: {device_id} ({', '.join(caps)})")
        
        logger.info(f"Loaded {len(ensembles)} devices from {self.deployment_file}")
        return ensembles
    
    def _create_ensemble_info(self, device_spec: Dict) -> TTEnsembleInfo:
        """
        Create TTEnsembleInfo from device specification.
        
        :param device_spec: Device dict from YAML
        :return: TTEnsembleInfo object
        """
        device_id = device_spec['id']
        
        # Generate address (use .local for local devices, actual for remote)
        location = device_spec.get('location', 'local')
        if location == 'cloud' or location == 'remote':
            address = f"{device_id}.remote"
        else:
            address = f"{device_id}.local"
        
        # Get components (capabilities)
        components = device_spec.get('components', {})
        
        # Create TTEnsembleInfo
        ensemble_info = TTEnsembleInfo(
            name=device_id,
            address=address,
            components=components
        )
        
        return ensemble_info
    
    def get_device_ids(self) -> List[str]:
        """
        Get list of all device IDs in deployment.
        
        :return: List of device ID strings
        """
        return [dev['id'] for dev in self.devices_config if 'id' in dev]
    
    def get_devices_by_capability(self, capability: str) -> List[str]:
        """
        Get device IDs that have a specific capability.
        
        Useful for test scenario generation.
        
        :param capability: Capability name (e.g., 'camera', 'gpu')
        :return: List of device IDs with that capability
        """
        devices = []
        for dev in self.devices_config:
            components = dev.get('components', {})
            if components.get(capability, False):
                devices.append(dev['id'])
        return devices
    
    def get_device_capabilities(self, device_id: str) -> Dict[str, bool]:
        """
        Get capabilities for a specific device.
        
        :param device_id: Device identifier
        :return: Components dict {capability: bool}
        """
        for dev in self.devices_config:
            if dev.get('id') == device_id:
                return dev.get('components', {})
        return {}
    
    def get_network_topology(self) -> List[Dict]:
        """
        Get network link information.
        
        :return: List of network link specifications
        """
        return self.network_config
    
    def validate_deployment(self) -> Dict:
        """
        Validate deployment configuration.
        
        Checks:
        - All devices have unique IDs
        - All devices have at least one capability
        - Network links reference valid devices
        
        :return: Validation results dict
        """
        issues = []
        warnings = []
        
        # Check for duplicate device IDs
        device_ids = [dev.get('id') for dev in self.devices_config]
        duplicates = [did for did in device_ids if device_ids.count(did) > 1]
        if duplicates:
            issues.append(f"Duplicate device IDs: {set(duplicates)}")
        
        # Check each device
        for dev in self.devices_config:
            device_id = dev.get('id', 'UNKNOWN')
            
            # Check for capabilities
            components = dev.get('components', {})
            if not components or not any(components.values()):
                warnings.append(f"Device {device_id} has no capabilities")
            
            # Check for required fields
            if 'id' not in dev:
                issues.append(f"Device missing 'id' field: {dev}")
        
        # Check network links reference valid devices
        valid_device_ids = set(device_ids)
        for link in self.network_config:
            link_devices = link.get('link', [])
            for device_id in link_devices:
                if device_id not in valid_device_ids:
                    issues.append(f"Network link references unknown device: {device_id}")
        
        validation = {
            'valid': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'device_count': len(self.devices_config),
            'network_links': len(self.network_config)
        }
        
        return validation
    
    def print_deployment_summary(self):
        """Print human-readable deployment summary."""
        print("\n" + "="*60)
        print("DEPLOYMENT CONFIGURATION")
        print("="*60)
        
        app_info = self.deployment_config.get('application', {})
        print(f"\nApplication: {app_info.get('name', 'Unnamed')}")
        if 'description' in app_info:
            print(f"Description: {app_info['description']}")
        
        print(f"\nDevices ({len(self.devices_config)}):")
        for dev in self.devices_config:
            device_id = dev.get('id', 'UNKNOWN')
            device_type = dev.get('type', 'unknown')
            location = dev.get('location', 'unknown')
            components = dev.get('components', {})
            caps = [k for k, v in components.items() if v]
            
            print(f"  {device_id:<20} ({device_type}, {location})")
            print(f"    Capabilities: {', '.join(caps) if caps else 'none'}")
        
        if self.network_config:
            print(f"\nNetwork Links ({len(self.network_config)}):")
            for link in self.network_config:
                devices = link.get('link', [])
                link_type = link.get('type', 'unknown')
                print(f"  {devices[0]} ↔ {devices[1]} ({link_type})")
        
        print("="*60)
    
    def print_validation_report(self):
        """Print validation report."""
        validation = self.validate_deployment()
        
        print("\n" + "="*60)
        print("DEPLOYMENT VALIDATION")
        print("="*60)
        
        if validation['valid']:
            print("\n✓ Deployment configuration is VALID")
        else:
            print("\n✗ Deployment configuration has ISSUES")
        
        if validation['issues']:
            print("\nIssues:")
            for issue in validation['issues']:
                print(f"  ✗ {issue}")
        
        if validation['warnings']:
            print("\nWarnings:")
            for warning in validation['warnings']:
                print(f"  ⚠ {warning}")
        
        print(f"\nDevices: {validation['device_count']}")
        print(f"Network links: {validation['network_links']}")
        print("="*60)
    
    def generate_test_scenarios(self) -> List[Dict]:
        """
        Auto-generate test scenarios based on deployment topology.
        
        Creates failure scenarios for devices with different capabilities.
        
        :return: List of test scenario dicts
        """
        scenarios = []
        
        # Scenario: Fail each device with unique capabilities
        capability_to_devices = {}
        for dev in self.devices_config:
            device_id = dev.get('id')
            components = dev.get('components', {})
            for cap, has_cap in components.items():
                if has_cap:
                    if cap not in capability_to_devices:
                        capability_to_devices[cap] = []
                    capability_to_devices[cap].append(device_id)
        
        # Create failure scenario for critical single-device capabilities
        for cap, devices in capability_to_devices.items():
            if len(devices) == 1:
                # Critical: only one device has this capability
                scenarios.append({
                    'type': 'device_failure',
                    'device': devices[0],
                    'description': f"Fail only device with {cap} capability",
                    'expected': 'graceful_degradation'
                })
            elif len(devices) == 2:
                # Test failover
                scenarios.append({
                    'type': 'device_failure',
                    'device': devices[0],
                    'description': f"Fail primary {cap} device (failover to backup)",
                    'expected': 'remapping'
                })
        
        # Scenario: Device recovery
        if self.devices_config:
            scenarios.append({
                'type': 'device_recovery',
                'device': self.devices_config[0]['id'],
                'description': 'Device comes back online',
                'expected': 'topology_update'
            })
        
        # Scenario: New device joins
        scenarios.append({
            'type': 'device_join',
            'device': 'new_device',
            'description': 'New device joins cluster',
            'expected': 'topology_update'
        })
        
        return scenarios


# Convenience function for simple usage
def load_deployment(deployment_file: str) -> Dict[str, TTEnsembleInfo]:
    """
    Convenience function: Load deployment and return ensembles in one call.
    
    :param deployment_file: Path to deployment YAML
    :return: Ensemble dict {device_id: TTEnsembleInfo}
    """
    loader = DeploymentLoader(deployment_file)
    return loader.load_ensembles()