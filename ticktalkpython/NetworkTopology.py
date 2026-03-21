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

'''
NetworkTopology - Network connectivity and performance characteristics.

Uses 3-layer configuration system:
- network_types.yaml: Reusable link templates  
- deployment.yaml: Application-specific network connections
- Resolves link instances -> type -> specs

This design enables:
- Reusable network type definitions
- Application-specific topologies
- No hardcoded device names in network config
- Scalable to large systems
'''

import yaml
from typing import Dict, Tuple, Optional
from . import DebugLogger

logger = DebugLogger.get_logger('NetworkTopology')


class NetworkTopology:
    """
    Represents network connectivity between devices using 3-layer config.
    
    Layer 1: network_types.yaml - Reusable link templates
    Layer 2: deployment.yaml - Application-specific connections
    Layer 3: Resolved topology (device_pair -> latency/bandwidth)
    """
    
    def __init__(self, network_types_path: Optional[str] = None,
                 deployment_path: Optional[str] = None):
        """
        Initialize network topology with 3-layer config.
        
        :param network_types_path: Path to network_types.yaml (global templates)
        :param deployment_path: Path to deployment.yaml (application connections)
        """
        self.network_types: Dict[str, Dict] = {}  # type_name -> specs
        self.latency: Dict[Tuple[str, str], float] = {}  # (dev_a, dev_b) -> seconds
        self.bandwidth: Dict[Tuple[str, str], float] = {}  # (dev_a, dev_b) -> bytes/sec
        
        # Default values for unknown links
        self.default_latency = 0.010
        self.default_bandwidth = 12500000
        
        # Load types first
        if network_types_path:
            self.load_network_types(network_types_path)
        
        # Then resolve deployment connections
        if deployment_path:
            self.load_deployment(deployment_path)
        
        logger.info(f"NetworkTopology initialized: "
                   f"{len(self.network_types)} types, {len(self.latency)//2} links")
    
    def load_network_types(self, config_path: str):
        """
        Load network type templates from network_types.yaml.
        
        Expected format:
        ```yaml
        network_types:
          wifi_good:
            latency: 0.005
            bandwidth: 62500000
          ethernet_1g:
            latency: 0.001
            bandwidth: 125000000
        ```
        
        :param config_path: Path to network_types.yaml
        """
        logger.info(f"Loading network types from {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'network_types' not in config:
                logger.warning(f"No 'network_types' key found in {config_path}")
                return
            
            self.network_types = config['network_types']
            
            # Update defaults if specified
            if 'default_latency' in config:
                self.default_latency = config['default_latency']
            if 'default_bandwidth' in config:
                self.default_bandwidth = config['default_bandwidth']
            
            logger.info(f"Loaded {len(self.network_types)} network types")
            
            for type_name in self.network_types.keys():
                logger.debug(f"  Type: {type_name}")
        
        except FileNotFoundError:
            logger.warning(f"Config file not found: {config_path}")
        except Exception as e:
            logger.error(f"Error loading network types: {e}")
    
    def load_deployment(self, deployment_path: str):
        """
        Load deployment network configuration and resolve links.
        
        Expected format:
        ```yaml
        network:
          - link: [pi_kitchen, pi_bedroom]
            type: wifi_good
          - link: [pi_kitchen, server_main]
            type: ethernet_1g
        ```
        
        :param deployment_path: Path to deployment.yaml
        """
        logger.info(f"Loading network deployment from {deployment_path}")
        
        try:
            with open(deployment_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if 'network' not in config:
                logger.warning(f"No 'network' key found in {deployment_path}")
                return
            
            # Resolve each link
            for link_config in config['network']:
                link_devices = link_config['link']
                link_type = link_config['type']
                
                if len(link_devices) != 2:
                    logger.warning(f"Link must have exactly 2 devices: {link_config}")
                    continue
                
                device_a, device_b = link_devices
                
                # Get specs from type
                if link_type not in self.network_types:
                    logger.warning(f"Unknown network type '{link_type}', using defaults")
                    latency = self.default_latency
                    bandwidth = self.default_bandwidth
                else:
                    type_specs = self.network_types[link_type]
                    latency = type_specs.get('latency', self.default_latency)
                    bandwidth = type_specs.get('bandwidth', self.default_bandwidth)
                
                # Store bidirectional
                self.latency[(device_a, device_b)] = latency
                self.latency[(device_b, device_a)] = latency
                self.bandwidth[(device_a, device_b)] = bandwidth
                self.bandwidth[(device_b, device_a)] = bandwidth
                
                logger.debug(f"Resolved {device_a} <-> {device_b} -> {link_type} "
                           f"({latency*1000:.1f}ms, {bandwidth/1e6:.1f} Mbps)")
            
            logger.info(f"Resolved {len(self.latency)//2} network links")
        
        except FileNotFoundError:
            logger.warning(f"Deployment file not found: {deployment_path}")
        except Exception as e:
            logger.error(f"Error loading deployment network: {e}")
    
    def get_latency(self, src_device: str, dst_device: str) -> float:
        """Get network latency between two devices."""
        if src_device == dst_device:
            return 0.0
        
        key = (src_device, dst_device)
        if key in self.latency:
            return self.latency[key]
        else:
            logger.debug(f"No latency for {src_device}->{dst_device}, using default")
            return self.default_latency
    
    def get_bandwidth(self, src_device: str, dst_device: str) -> float:
        """Get network bandwidth between two devices."""
        if src_device == dst_device:
            return float('inf')
        
        key = (src_device, dst_device)
        if key in self.bandwidth:
            return self.bandwidth[key]
        else:
            logger.debug(f"No bandwidth for {src_device}->{dst_device}, using default")
            return self.default_bandwidth
    
    def calculate_transfer_time(self, src_device: str, dst_device: str,
                                data_size: int) -> float:
        """Calculate time to transfer data between devices."""
        if src_device == dst_device:
            return 0.0
        
        latency = self.get_latency(src_device, dst_device)
        bandwidth = self.get_bandwidth(src_device, dst_device)
        
        transfer_time = latency + (data_size / bandwidth)
        
        logger.debug(f"Transfer {src_device}->{dst_device} "
                    f"({data_size}B): {transfer_time*1000:.3f}ms")
        
        return transfer_time
    
    def add_link(self, device_a: str, device_b: str,
                 latency: float, bandwidth: float):
        """Manually add a network link (bidirectional)."""
        self.latency[(device_a, device_b)] = latency
        self.latency[(device_b, device_a)] = latency
        self.bandwidth[(device_a, device_b)] = bandwidth
        self.bandwidth[(device_b, device_a)] = bandwidth
        
        logger.debug(f"Added link: {device_a} <-> {device_b} "
                    f"({latency*1000:.1f}ms, {bandwidth/1e6:.1f} Mbps)")
    
    def get_neighbors(self, device: str) -> list:
        """Get list of devices directly connected to this device."""
        neighbors = set()
        for (src, dst) in self.latency.keys():
            if src == device:
                neighbors.add(dst)
        return list(neighbors)
    
    def print_topology(self):
        """Print network topology in human-readable format."""
        print(f"\nNetwork Topology ({len(self.latency)//2} links):")
        print(f"  Default: {self.default_latency*1000:.1f}ms, "
              f"{self.default_bandwidth/1e6:.1f} Mbps")
        print("\n  Links:")
        
        seen = set()
        for (device_a, device_b), latency in self.latency.items():
            link = tuple(sorted([device_a, device_b]))
            if link not in seen:
                seen.add(link)
                bandwidth = self.bandwidth[(device_a, device_b)]
                print(f"    {device_a} <-> {device_b}: "
                      f"{latency*1000:.1f}ms, {bandwidth/1e6:.1f} Mbps")


# Singleton instance
_topology = None


def get_network_topology(network_types_path: Optional[str] = None,
                         deployment_path: Optional[str] = None) -> NetworkTopology:
    """Get or create the global NetworkTopology instance."""
    global _topology
    if _topology is None:
        _topology = NetworkTopology(network_types_path, deployment_path)
    return _topology


def initialize_topology(network_types_path: str, deployment_path: str):
    """Initialize network topology from 3-layer configuration."""
    global _topology
    _topology = NetworkTopology(network_types_path, deployment_path)
    logger.info(f"Network topology initialized: {len(_topology.latency)//2} links")