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
Utility functions for multi-application placement and coordination.
Includes constraint checking, dependency extraction, and helper functions
used by UnifiedPlacementGraph and coordination components.
'''

from typing import List, Tuple, Set, Dict, Any
from . import Query
from . import Constants
from . import DebugLogger

logger = DebugLogger.get_logger('PlacementUtils')


def check_sq_constraints(sq, ensemble_info) -> bool:
    """
    Check if an ensemble satisfies an SQ's constraints.
    
    Extracted from Mapper.static_mapping() to allow reuse by both
    the traditional mapper and the UnifiedPlacementGraph builder.
    
    :param sq: TTSQ object with constraints list
    :type sq: TTSQ
    
    :param ensemble_info: Ensemble information object
    :type ensemble_info: TTEnsembleInfo
    
    :return: True if ensemble meets all SQ constraints
    :rtype: bool
    """
    constraints = sq.constraints
    
    # No constraints means any ensemble is compatible
    if len(constraints) == 0:
        return True
    
    # Test constraints using Query mechanism
    query = Query.TTQuery(constraints, Query.QueryOp.AND)
    return query.test(ensemble_info)


def get_compatible_ensembles(sq, ensemble_infos) -> List[str]:
    """
    Get all ensembles that are compatible with an SQ's constraints.
    
    :param sq: TTSQ object with constraints
    :type sq: TTSQ
    
    :param ensemble_infos: List or dict values of TTEnsembleInfo objects
    :type ensemble_infos: List[TTEnsembleInfo] | dict.values()
    
    :return: List of compatible ensemble names
    :rtype: List[str]
    """
    compatible = [
        ens_info.name for ens_info in ensemble_infos
        if check_sq_constraints(sq, ens_info)
    ]
    
    if not compatible:
        logger.warning(
            f"No ensemble found satisfying constraints {sq.constraints} "
            f"for SQ {sq.sq_name}. Defaulting to RuntimeManager.")
        compatible = [Constants.RUNTIME_MANAGER_ENSEMBLE_NAME]
    
    return compatible


def extract_dependencies(graph) -> List[Tuple[Any, Any, str]]:
    """
    Extract dependency edges from a TTGraph.
    
    Dependencies are determined by matching output ports (opps) of source SQs
    with input ports (ipps) of destination SQs via symbol names (variable names).
    
    Based on the logic in Mapper.generate_mapping().
    
    :param graph: Compiled TTGraph object
    :type graph: TTGraph
    
    :return: List of (source_sq, dest_sq, symbol) tuples representing dependencies
    :rtype: List[Tuple[TTSQ, TTSQ, str]]
    """
    dependencies = []
    
    # Get mapping of symbols to consuming SQs
    # Format: {symbol: set of (sq, port_num)}
    ipp_to_sq = graph.get_ipp_to_sq_dict()
    
    # For each SQ in the graph
    for source_sq in graph.sqs:
        # For each output port of this SQ
        for opp in source_sq.get_opps():
            symbol = opp.data_name  # The variable name connecting SQs
            
            # Find which SQs consume this output
            if symbol in ipp_to_sq:
                dest_sqs_set = ipp_to_sq[symbol]  # Set of (dest_sq, port_num)
                
                # Create dependency edge for each consuming SQ
                for dest_sq, port_num in dest_sqs_set:
                    dependencies.append((source_sq, dest_sq, symbol))
    
    return dependencies


def prefix_sq_name(app_id: str, sq_name: str) -> str:
    """
    Generate a prefixed SQ name to avoid conflicts between applications.
    
    When multiple applications are deployed, SQs from different applications
    might have the same name. Prefixing with app_id ensures uniqueness.
    
    :param app_id: Application identifier
    :type app_id: str
    
    :param sq_name: Original SQ name
    :type sq_name: str
    
    :return: Prefixed name in format "app_id_sq_name"
    :rtype: str
    
    Example:
        prefix_sq_name("camera_app", "sensor_read") -> "camera_app_sensor_read"
    """
    return f"{app_id}_{sq_name}"

def check_device_capacity(device_name: str, resource_ledger: Dict, 
                          slots_required: int = 1, memory_required: int = 64) -> bool:
    """
    Check if a device has sufficient capacity for an SQ.
    
    :param device_name: Device identifier
    :param resource_ledger: Resource ledger from RuntimeManager
    :param slots_required: Compute slots needed by the SQ
    :param memory_required: Memory needed by the SQ (MB)
    :return: True if device has sufficient capacity
    """
    if device_name not in resource_ledger:
        logger.warning(f"Device {device_name} not in resource ledger")
        return False
    
    ledger = resource_ledger[device_name]
    
    has_slots = ledger['available_slots'] >= slots_required
    has_memory = ledger['available_memory_mb'] >= memory_required
    
    return has_slots and has_memory


def get_devices_with_capacity(resource_ledger: Dict, slots_required: int = 1, 
                               memory_required: int = 64) -> List[str]:
    """
    Get all devices that have sufficient capacity for an SQ.
    
    :param resource_ledger: Resource ledger from RuntimeManager
    :param slots_required: Compute slots needed
    :param memory_required: Memory needed (MB)
    :return: List of device names with sufficient capacity
    """
    available = []
    
    for device_name, ledger in resource_ledger.items():
        if (ledger['available_slots'] >= slots_required and 
            ledger['available_memory_mb'] >= memory_required):
            available.append(device_name)
    
    return available