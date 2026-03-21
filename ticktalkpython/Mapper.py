# Copyright 2021 The Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

'''
Mapping relates the graph of SQs to the physical system: SQs are assigned to
ensembles and told where their outputs should be sent. Mapping is done after
compilation and before graph interpretation/execution.

Mapping is done based for a known network of ensembles. There may be constraints
on mapping from the graph itself, and it is the Mapper's responsibility to
satisfy these constraints. These constraints may restrict SQs to only run on
specific Ensembles (or types of Ensembles) or may specify some multi-SQ
constraints like an upper bound on latency.

Within the runtime environment, the mapping should be done using either a static
system description or a runtime-generated description provided by the runtime
manager (RTM). The RTM should handle mapping as part of the graph instantiation
process before it kicks off interpretation by injecting initial input tokens.
'''

from typing import Union, List
import random

from . import Port
from . import Graph
from . import DebugLogger
from . import Query
from . import Constants

logger = DebugLogger.get_logger('Mapper')

def static_mapping(graph, ensemble_infos):
    '''
    Given that the graph has SQs with annotated constraints (from TTQuery
    within the program), the mapping returned will ensure that mapped SQs have
    a corresponding compatible ensemble

    :param graph: The graph to map entirely onto a singular ensemble
    :type graph: TTGraph

    :param ensemble: The ensembles to map the entire graph onto
    :type ensemble: [TTEnsembleInfo]

    :return: A dictionary using SQ names as keys and ensemble names as values,
        to represent which SQ the ensemble is mapped onto. This is used to
        instantiate all the SQs on their corresponding ensemble. An SQ is
        uniquely named and uniquely mapped to one ensemble.
    :rtype: dict
    '''
    mapping = {}

    for sq in graph.sqs:
        constraints = sq.constraints or []
        candidates = [
            ens.name for ens in ensemble_infos
            if not constraints or Query.TTQuery(constraints, Query.QueryOp.AND).test(ens)
        ]

        if not candidates:
            chosen = Constants.RUNTIME_MANAGER_ENSEMBLE_NAME
            logger.warning(f"SQ {sq.sq_name} has no matching ensemble → {chosen}")
        else:
            chosen = candidates[0]

        mapping[sq.sq_name] = chosen

    return mapping


def coordinated_static_mapping(graphs, ensemble_infos, app_configs, device_allocations=None):
    '''
    Coordinated static mapping for multiple applications.
    
    Maps all apps using available devices. With multitenancy,
    all apps share devices via time-slicing or concurrent execution.
    
    :param graphs: List of TTGraph objects to map
    :type graphs: List[TTGraph]
    
    :param ensemble_infos: Available ensembles
    :type ensemble_infos: List[TTEnsembleInfo]
    
    :param app_configs: Application configurations
                       Format: {app_id: {'graph': TTGraph}}
    :type app_configs: Dict[str, Dict]
    
    :param device_allocations: Current device allocation state (for incremental mode)
                              Format: {device_id: {'apps': {app_id: [sq_names]}}}
    :type device_allocations: Optional[Dict]
    
    :return: Per-app mappings {app_id: {sq_name: device_name}}
    :rtype: Dict[str, Dict[str, str]]
    '''
    if device_allocations is None:
        device_allocations = {}
    
    all_mappings = {}
    
    for app_id in app_configs:
        app_config = app_configs[app_id]
        graph = next((g for g in graphs if g.graph_name == app_id or 
                     app_configs.get(app_id, {}).get('graph') == g), None)
        
        if graph is None:
            logger.warning(f"No graph found for app {app_id}, skipping")
            continue
        
        # All devices available — multitenancy handles sharing
        app_mapping = static_mapping(graph, ensemble_infos)
        all_mappings[app_id] = app_mapping
        
        # Track allocations (multi-app per device)
        for sq_name, device_name in app_mapping.items():
            if device_name not in device_allocations:
                device_allocations[device_name] = {'apps': {}}
            apps = device_allocations[device_name]['apps']
            if app_id not in apps:
                apps[app_id] = []
            apps[app_id].append(sq_name)
        
        logger.info(f"Mapped app {app_id} with {len(app_mapping)} SQs")
    
    return all_mappings


# creates list of arc destinations
def generate_mapping(graph: Graph.TTGraph, mapping) -> dict:
    '''
    Generate arc destinations: SQ → list of [TTMappedPort] for each output port
    '''
    ipp_to_sq = graph.get_ipp_to_sq_dict()
    arc_dests = {}

    for sq in graph.sqs:
        dest_list = []
        for opp in sq.opps:
            data_name = opp.data_name
            destinations = ipp_to_sq.get(data_name, set())
            mapped_ports = [
                Port.TTMappedPort(mapping[dest_sq.sq_name], dest_sq.sq_name, pn)
                for dest_sq, pn in destinations
            ]
            dest_list.append(mapped_ports)
        arc_dests[sq] = dest_list

    return arc_dests

class TTMapper():
    '''
    The TTMapper handles mapping based on a system description (a set of
    ensembles) and a graph. The exact format of the system description is
    subject to change, and will likely become more complex as mapping algorithms
    become more sophisticated

    :param graph: The compiled graph representing a TTPython program, which is
        ready to be mapped to the set of ensembles
    :type graph: TTGraph

    :param ensembles: A set of ensembles composing the system; this is the system
        description
    :type ensembles: list(TTEnsembles)
    '''

    def __init__(self, graph: Graph.TTGraph, ensembles=None):
        self.graph = graph
        self.ensembles = [] if ensembles is None else ensembles

    @staticmethod
    def trivial_mapping(graph, ensemble):
        '''
        Trivial mapping: all SQs on one ensemble.
        Compatible with modern TTGraph (no symbol_table, no output_arc).
        '''
        mapped_graph = {}
        ensemble_name = ensemble.name

        # Get mapping from input port name → downstream (SQ, port_number)
        ipp_to_sq = graph.get_ipp_to_sq_dict()

        # === Step 1: Set up input arc destinations ===
        # Input arcs = ports with no upstream (source_var_names)
        for var_name in graph.source_var_names():
            if var_name not in ipp_to_sq:
                continue
            for dest_sq, port_number in ipp_to_sq[var_name]:
                # Find the arc object (if needed) — but we don't need it
                # Just build TTMappedPort
                mapped_port = Port.TTMappedPort(
                    ensemble_name, dest_sq.sq_name, port_number
                )
                # Store in dest_sq's input port? Or arc? We don't have arc → skip
                # But runtime may expect dest_mapping on arc → we’ll handle in generate_mapping
                logger.debug(f"Input {var_name} → {mapped_port}")

        # === Step 2: Set up output arc destinations (via opps) ===
        for sq in graph.sqs:
            for opp in sq.opps:
                data_name = opp.data_name
                if data_name not in ipp_to_sq:
                    continue
                for dest_sq, port_number in ipp_to_sq[data_name]:
                    mapped_port = Port.TTMappedPort(
                        ensemble_name, dest_sq.sq_name, port_number
                    )
                    # If opp has dest_mapping list, append
                    if hasattr(opp, 'dest_mapping') and isinstance(opp.dest_mapping, list):
                        if mapped_port not in opp.dest_mapping:
                            opp.dest_mapping.append(mapped_port)
                            logger.debug(f"Output {data_name} → {mapped_port}")

            mapped_graph[sq.sq_name] = ensemble_name

        return mapped_graph

    @staticmethod
    def random_mapping(graph: Graph.TTGraph, ensembles: Union[dict, list]):
        """
        Randomly assign SQs to ensembles while preserving data-flow:
        - If all upstream SQs are on the same ensemble → place downstream there.
        - Otherwise → pick a random ensemble.
        """
        ipp_to_sq = graph.get_ipp_to_sq_dict()
        mapping = {}
        visited = set()

        # Build upstream → downstream index
        upstream_of = {sq.sq_name: set() for sq in graph.sqs}
        for var_name in graph.source_var_names():
            for dest_sq, _ in ipp_to_sq.get(var_name, []):
                upstream_of[dest_sq.sq_name].add(None)  # source input
        for sq in graph.sqs:
            for opp in sq.opps:
                data_name = opp.data_name
                for dest_sq, _ in ipp_to_sq.get(data_name, []):
                    upstream_of[dest_sq.sq_name].add(sq.sq_name)

        def pick_random_ensemble():
            idx = random.randrange(len(ensembles))
            if isinstance(ensembles, dict):
                return list(ensembles.values())[idx].name
            return ensembles[idx].name

        def assign(sq_name):
            if sq_name in visited:
                return
            visited.add(sq_name)

            ups = [u for u in upstream_of[sq_name] if u is not None]
            upstream_ens = {mapping[u] for u in ups if u in mapping}

            if len(upstream_ens) == 1:
                mapping[sq_name] = next(iter(upstream_ens))
            else:
                mapping[sq_name] = pick_random_ensemble()

        # BFS from source SQs
        from collections import deque
        queue = deque([
            sq.sq_name for sq in graph.sqs
            if not any(u for u in upstream_of[sq.sq_name] if u)
        ])
        while queue:
            cur = queue.popleft()
            assign(cur)
            cur_sq = next(s for s in graph.sqs if s.sq_name == cur)
            for opp in cur_sq.opps:
                for dst_sq, _ in ipp_to_sq.get(opp.data_name, []):
                    if dst_sq.sq_name not in visited:
                        queue.append(dst_sq.sq_name)

        # Fallback
        for sq in graph.sqs:
            if sq.sq_name not in mapping:
                mapping[sq.sq_name] = pick_random_ensemble()

        return mapping