# Copyright 2021 The Authors

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

'''
DeviceProfile - Device performance and power characteristics.

Uses 3-layer configuration system:
- device_types.yaml: Reusable hardware templates
- deployment.yaml: Application-specific device instances
- Resolves instance -> type -> specs

This design enables:
- Reusable device type definitions
- Application-specific deployments
- No hardcoded device names
- Scalable to large systems
'''

import yaml
from typing import Dict, Optional
from . import DebugLogger

logger = DebugLogger.get_logger('DeviceProfile')


class DeviceProfile:
    """
    Represents performance and power characteristics of a computing device.
    
    Used for realistic cost estimation in objective calculations.
    Different devices have different speeds (affecting execution time) and
    power consumption (affecting energy cost).
    """
    
    def __init__(self, name: str, cpu_speed: float, memory_size: int,
                 power_idle: float, power_active: float,
                 power_transmit: float, power_receive: float,
                 components: Optional[Dict] = None):
        """
        Create a device profile.
        
        :param name: Device identifier (e.g., 'pi', 'server', 'edge_device')
        :param cpu_speed: Processing speed factor relative to baseline (1.0 = baseline)
        :param memory_size: Available RAM in bytes
        :param power_idle: Idle power consumption in watts
        :param power_active: Active processing power in watts
        :param power_transmit: Power during network transmission in watts
        :param power_receive: Power during network reception in watts
        :param components: Optional dict of hardware components (e.g., {'camera': True})
        """
        self.name = name
        self.cpu_speed = cpu_speed
        self.memory_size = memory_size
        self.power_idle = power_idle
        self.power_active = power_active
        self.power_transmit = power_transmit
        self.power_receive = power_receive
        self.components = components or {}
        
        logger.debug(f"Created DeviceProfile: {name} (speed={cpu_speed}x, power={power_active}W)")
    
    def calculate_execution_time(self, base_time: float) -> float:
        """
        Calculate execution time on this device given a baseline time.
        
        Faster devices (higher cpu_speed) complete tasks quicker.
        
        :param base_time: Baseline execution time in seconds (on reference device)
        :return: Execution time on this device in seconds
        """
        return base_time / self.cpu_speed
    
    def calculate_execution_energy(self, execution_time: float) -> float:
        """
        Calculate energy consumed during execution.
        
        Energy = Power × Time
        
        :param execution_time: Execution duration in seconds
        :return: Energy consumed in joules
        """
        return self.power_active * execution_time
    
    def calculate_communication_energy(self, transfer_time: float, transmitting: bool) -> float:
        """
        Calculate energy consumed during network communication.
        
        :param transfer_time: Duration of transfer in seconds
        :param transmitting: True if device is transmitting, False if receiving
        :return: Energy consumed in joules
        """
        power = self.power_transmit if transmitting else self.power_receive
        return power * transfer_time
    
    def __repr__(self):
        return (f"DeviceProfile({self.name}: {self.cpu_speed}x speed, "
                f"{self.power_active}W active, {self.memory_size} bytes RAM)")


class DeviceProfileManager:
    """
    Manages device profiles using 3-layer configuration system.
    
    Layer 1: device_types.yaml - Reusable hardware templates
    Layer 2: deployment.yaml - Application-specific instances
    Layer 3: Resolved profiles (instance ID -> specs)
    """
    
    def __init__(self, device_types_path: Optional[str] = None,
                 deployment_path: Optional[str] = None):
        """
        Initialize device profile manager with 3-layer config.
        
        :param device_types_path: Path to device_types.yaml (global templates)
        :param deployment_path: Path to deployment.yaml (application instances)
        """
        self.device_types: Dict[str, Dict] = {}  # type_name -> specs
        self.profiles: Dict[str, DeviceProfile] = {}  # instance_id -> profile
        self.default_profile = self._create_default_profile()
        
        # Load types first
        if device_types_path:
            self.load_device_types(device_types_path)
        
        # Then resolve deployment instances
        if deployment_path:
            self.load_deployment(deployment_path)
        
        logger.info(f"DeviceProfileManager initialized: "
                   f"{len(self.device_types)} types, {len(self.profiles)} instances")
    
    def load_device_types(self, config_path: str):
        """
        Load device type templates from device_types.yaml.
        
        Expected format:
        ```yaml
        device_types:
          raspberry_pi_4:
            cpu_speed: 1.0
            memory_size: 4294967296
            power_idle: 2.5
            power_active: 5.0
            power_transmit: 3.0
            power_receive: 2.5
        ```
        
        :param config_path: Path to device_types.yaml
        """
        logger.info(f"Loading device types from {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'device_types' not in config:
                logger.warning(f"No 'device_types' key found in {config_path}")
                return
            
            self.device_types = config['device_types']
            logger.info(f"Loaded {len(self.device_types)} device types")
            
            for type_name in self.device_types.keys():
                logger.debug(f"  Type: {type_name}")
        
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}")
        except Exception as e:
            logger.error(f"Error loading device types: {e}")
    
    def load_deployment(self, deployment_path: str):
        """
        Load deployment configuration and resolve device instances.
        
        Expected format:
        ```yaml
        devices:
          - id: pi_kitchen
            type: raspberry_pi_4
          - id: server_main
            type: server_x86
        ```
        
        :param deployment_path: Path to deployment.yaml
        """
        logger.info(f"Loading deployment from {deployment_path}")
        
        try:
            with open(deployment_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'devices' not in config:
                logger.warning(f"No 'devices' key found in {deployment_path}")
                return
            
            # Resolve each device instance
            for device_config in config['devices']:
                device_id = device_config['id']
                device_type = device_config['type']
                
                if device_type not in self.device_types:
                    logger.warning(f"Unknown device type '{device_type}' for {device_id}, using default")
                    self.profiles[device_id] = self.default_profile
                    continue
                
                # Get specs from type
                type_specs = self.device_types[device_type]
                
                # Create profile for this instance
                profile = DeviceProfile(
                    name=device_id,
                    cpu_speed=type_specs.get('cpu_speed', 1.0),
                    memory_size=type_specs.get('memory_size', 1073741824),
                    power_idle=type_specs.get('power_idle', 2.0),
                    power_active=type_specs.get('power_active', 5.0),
                    power_transmit=type_specs.get('power_transmit', 3.0),
                    power_receive=type_specs.get('power_receive', 2.5),
                    components=device_config.get('components', {})
                )
                
                self.profiles[device_id] = profile
                logger.debug(f"Resolved {device_id} -> {device_type} -> {profile}")
            
            logger.info(f"Resolved {len(self.profiles)} device instances")
        
        except FileNotFoundError:
            logger.warning(f"Deployment file not found: {deployment_path}")
        except Exception as e:
            logger.error(f"Error loading deployment: {e}")
    
    def get_profile(self, device_name: str) -> DeviceProfile:
        """
        Get device profile by name, with fallback to default.
        
        :param device_name: Device identifier
        :return: DeviceProfile for the device
        """
        if device_name in self.profiles:
            return self.profiles[device_name]
        else:
            logger.debug(f"No profile for '{device_name}', using default")
            return self.default_profile
    
    def _create_default_profile(self) -> DeviceProfile:
        """
        Create a default device profile for unknown devices.
        
        Uses conservative middle-ground values.
        
        :return: Default DeviceProfile
        """
        return DeviceProfile(
            name="default",
            cpu_speed=1.0,          # Baseline speed
            memory_size=1073741824, # 1GB
            power_idle=2.0,         # 2W
            power_active=5.0,       # 5W
            power_transmit=3.0,     # 3W
            power_receive=2.5,      # 2.5W
            components={}
        )
    
    def add_profile(self, profile: DeviceProfile):
        """
        Manually add a device profile.
        
        :param profile: DeviceProfile to add
        """
        self.profiles[profile.name] = profile
        logger.debug(f"Added profile: {profile}")
    
    def list_profiles(self):
        """Print all loaded device profiles."""
        print(f"\nLoaded Device Profiles ({len(self.profiles)}):")
        for name, profile in self.profiles.items():
            print(f"  {profile}")


# Singleton instance for global access
_profile_manager = None


def get_profile_manager(device_types_path: Optional[str] = None,
                        deployment_path: Optional[str] = None) -> DeviceProfileManager:
    """
    Get or create the global DeviceProfileManager instance.
    
    :param device_types_path: Path to device_types.yaml (only used on first call)
    :param deployment_path: Path to deployment.yaml (only used on first call)
    :return: Global DeviceProfileManager instance
    """
    global _profile_manager
    if _profile_manager is None:
        _profile_manager = DeviceProfileManager(device_types_path, deployment_path)
    return _profile_manager


def initialize_profiles(device_types_path: str, deployment_path: str):
    """
    Initialize device profiles from 3-layer configuration.
    
    Should be called once at startup before creating UnifiedPlacementGraph.
    
    :param device_types_path: Path to device_types.yaml (global templates)
    :param deployment_path: Path to deployment.yaml (application instances)
    """
    global _profile_manager
    _profile_manager = DeviceProfileManager(device_types_path, deployment_path)
    logger.info(f"Device profiles initialized: {len(_profile_manager.profiles)} instances")