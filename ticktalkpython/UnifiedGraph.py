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
UnifiedPlacementGraph represents the multi-application placement problem space.

The unified graph enumerates ALL feasible placements before any placement
decisions are made. It serves as input to coordinators (Phase 2) which will
select actual placements based on various heuristics.

Key Concepts:
- Nodes: Represent possible (SQ, Device, App) placements
- Edges: Represent dependencies between SQs that must be respected in placement
- This is a problem representation, not a solution
'''

from typing import List, Tuple, Dict, Any
from . import DebugLogger
from . import PlacementUtils

logger = DebugLogger.get_logger('UnifiedGraph')


class PlacementNode:
    """
    Represents one feasible placement option for an SQ.
    
    A PlacementNode indicates that a specific SQ from a specific application
    COULD be placed on a specific device (if that device meets the SQ's constraints).
    
    The unified placement graph will contain multiple PlacementNodes for each SQ,
    one for each compatible device.
    """
    
    def __init__(self, sq, device, app_id):
        """
        Create a placement node representing a feasible (SQ, Device, App) combination.
        
        :param sq: The scheduling quantum (task) to be placed
        :type sq: TTSQ
        
        :param device: The name of the ensemble (device) where SQ could be placed
        :type device: str
        
        :param app_id: The application identifier this SQ belongs to
        :type app_id: str
        """
        self.sq = sq                    # TTSQ object - the actual task
        self.device = device            # str - ensemble name
        self.app_id = app_id            # str - application identifier
        
        # Generate unique prefixed ID to avoid conflicts across applications
        self.sq_id = PlacementUtils.prefix_sq_name(app_id, sq.sq_name)
        
        # Phase 4: Cost attributes for objective modeling
        # Flow costs from TTSQ to this placement node
        self.execution_time = self._get_execution_time(sq, device)
        self.energy_cost = self._get_energy_cost(sq, device)
        self.memory_required = sq.memory_required if hasattr(sq, 'memory_required') else None
        
        # Resource requirements for capacity-aware placement
        if hasattr(sq, 'resource_requirements'):
            self.resource_requirements = sq.resource_requirements
        else:
            # Default resource requirements if not inferred
            self.resource_requirements = {
                'compute_slots': 1,
                'memory_mb': 64,
                'exclusive': False,
            }
    
    def _get_execution_time(self, sq, device):
        """
        Get execution time for this SQ on this device.
        
        Phase 4A: Uses 'default' from SQ estimates (all devices identical)
        Phase 4D: Will use device-specific values
        
        :return: Execution time in seconds, or None if not available
        :rtype: float or None
        """

        if sq.function_name.isupper():
            return 0.0

        if hasattr(sq, 'execution_time_estimates'):
            # Try device-specific, fall back to default
            estimates = sq.execution_time_estimates
            return estimates.get(device, estimates.get('default', None))
        return None
    
    def _get_energy_cost(self, sq, device):
        """
        Get energy cost for this SQ on this device.
        
        Phase 4A: Uses 'default' from SQ estimates (all devices identical)
        Phase 4D: Will use device-specific values
        
        :return: Energy cost in joules, or None if not available
        :rtype: float or None
        """
        if sq.function_name.isupper():
            return 0.0

        if hasattr(sq, 'energy_cost_estimates'):
            # Try device-specific, fall back to default
            estimates = sq.energy_cost_estimates
            return estimates.get(device, estimates.get('default', None))
        return None
    
    def __repr__(self):
        return f"PlacementNode({self.sq_id} -> {self.device})"
    
    def __eq__(self, other):
        if not isinstance(other, PlacementNode):
            return False
        return (self.sq_id == other.sq_id and 
                self.device == other.device and 
                self.app_id == other.app_id)
    
    def __hash__(self):
        return hash((self.sq_id, self.device, self.app_id))


class UnifiedPlacementGraph:
    """
    Represents the complete placement problem space for multiple applications.
    
    The unified graph enumerates:
    - All feasible (SQ, Device, App) placement options as nodes
    - All dependency relationships between SQs as edges
    
    This graph is the INPUT to coordinators in Phase 2, which will analyze it
    and select which nodes to "activate" (actual placements).
    """
    
    def __init__(self, applications: Dict[str, Dict], ensembles: Dict[str, Any]):
        """
        Construct the unified placement graph from applications and devices.
        
        :param applications: Dictionary of application metadata
                            Format: {app_id: {'graph': TTGraph}}
        :type applications: Dict[str, Dict]
        
        :param ensembles: Dictionary of available ensembles
                         Format: {ensemble_name: TTEnsembleInfo}
        :type ensembles: Dict[str, Any]
        """
        self.applications = applications
        self.ensembles = ensembles
        
        # Nodes: All feasible (SQ, Device, App) placement options
        self.nodes: List[PlacementNode] = []
        
        # Edges: Dependencies between SQs
        # Format: [(src_node, dst_node, symbol), ...]
        self.edges: List[Tuple[PlacementNode, PlacementNode, str]] = []
        
        logger.info(f"Building unified placement graph for {len(applications)} "
                   f"applications and {len(ensembles)} ensembles")
        
        # Build the graph structure
        self._build_nodes()
        self._build_edges()
        
        logger.info(f"Unified graph constructed: {len(self.nodes)} nodes, "
                   f"{len(self.edges)} edges")
    
    def _build_nodes(self):
        """
        Build all feasible placement nodes.
        
        For each application:
          For each SQ in that application:
            For each device compatible with SQ's constraints:
              Create PlacementNode(SQ, Device, App)
        
        This enumerates the complete "possibility space" of placements.
        """
        for app_id, app_data in self.applications.items():
            graph = app_data['graph']
            
            logger.debug(f"Building nodes for application '{app_id}' "
                        f"with {len(graph.sqs)} SQs")
            
            # For each SQ in this application's graph
            for sq in graph.sqs:
                # Find devices compatible with this SQ's constraints
                compatible_devices = PlacementUtils.get_compatible_ensembles(
                    sq, self.ensembles.values()
                )
                
                logger.debug(f"  SQ '{sq.sq_name}': {len(compatible_devices)} "
                            f"compatible devices: {compatible_devices}")
                
                # Create a placement node for each feasible (SQ, Device, App)
                for device in compatible_devices:
                    node = PlacementNode(sq, device, app_id)
                    self.nodes.append(node)
    
    def _build_edges(self):
        """
        Build dependency edges between placement nodes.
        
        For each application:
          Extract dependencies from the graph (which SQs depend on which)
          For each dependency (SQ_A -> SQ_B):
            Create edges between ALL placement options:
              For each node where SQ_A could be placed:
                For each node where SQ_B could be placed:
                  Create edge (node_A, node_B, symbol)
        
        This creates the "dependency mesh" showing all possible ways the
        dependencies could be satisfied depending on placement choices.
        """
        for app_id, app_data in self.applications.items():
            graph = app_data['graph']
            
            # Extract dependencies: list of (source_sq, dest_sq, symbol)
            dependencies = PlacementUtils.extract_dependencies(graph)
            
            logger.debug(f"Building edges for application '{app_id}' "
                        f"with {len(dependencies)} dependencies")
            
            # For each dependency in this application
            for source_sq, dest_sq, symbol in dependencies:
                # Find all placement nodes for the source SQ
                source_nodes = [
                    node for node in self.nodes
                    if node.sq == source_sq and node.app_id == app_id
                ]
                
                # Find all placement nodes for the destination SQ
                dest_nodes = [
                    node for node in self.nodes
                    if node.sq == dest_sq and node.app_id == app_id
                ]
                
                # Create edges between all combinations of placements
                # This represents: "if source is placed on device X and 
                # dest is placed on device Y, they must communicate"
                for src_node in source_nodes:
                    for dst_node in dest_nodes:
                        edge = (src_node, dst_node, symbol)
                        self.edges.append(edge)
                        
                        logger.debug(f"    Edge: {src_node.sq_id}@{src_node.device} "
                                   f"-> {dst_node.sq_id}@{dst_node.device} "
                                   f"(via {symbol})")
    
    def get_nodes_for_sq(self, app_id: str, sq_name: str) -> List[PlacementNode]:
        """
        Get all placement nodes for a specific SQ.
        
        Returns all devices where this SQ could potentially be placed.
        
        :param app_id: Application identifier
        :param sq_name: SQ name (unprefixed)
        :return: List of PlacementNodes for this SQ
        """
        prefixed_sq_id = PlacementUtils.prefix_sq_name(app_id, sq_name)
        return [node for node in self.nodes if node.sq_id == prefixed_sq_id]
    
    def get_nodes_for_app(self, app_id: str) -> List[PlacementNode]:
        """
        Get all placement nodes for a specific application.
        
        :param app_id: Application identifier
        :return: List of PlacementNodes for this application
        """
        return [node for node in self.nodes if node.app_id == app_id]
    
    def get_edges_for_app(self, app_id: str) -> List[Tuple[PlacementNode, PlacementNode, str]]:
        """
        Get all dependency edges for a specific application.
        
        :param app_id: Application identifier
        :return: List of edges for this application
        """
        return [
            edge for edge in self.edges
            if edge[0].app_id == app_id  # source node's app
        ]
    
    def summary(self) -> str:
        """
        Generate a summary string of the unified placement graph.
        
        :return: Human-readable summary
        """
        summary_lines = [
            "=== Unified Placement Graph Summary ===",
            f"Applications: {len(self.applications)}",
            f"Ensembles: {len(self.ensembles)}",
            f"Total Placement Nodes: {len(self.nodes)}",
            f"Total Dependency Edges: {len(self.edges)}",
            ""
        ]
        
        # Per-application breakdown
        for app_id in self.applications:
            app_nodes = self.get_nodes_for_app(app_id)
            app_edges = self.get_edges_for_app(app_id)
            summary_lines.append(
                f"  {app_id}: "
                f"{len(app_nodes)} nodes, {len(app_edges)} edges"
            )
        
        return "\n".join(summary_lines)