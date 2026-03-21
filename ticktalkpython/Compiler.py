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
The TTPython Graph Compiler takes a TTPython source file, reads it, creates an
abstract syntax tree, walks the tree, and translates it into a TTPython graph.
Here's the compilation process, in a nutshell:

* Read the file

* Find all ``@SQify``-ed functions (and any others with valid TTPython
  decorators), including those that might be found via ``import``.
  :ref:`Instructions.py<instructions>` contains several examples.

* Build a table of them, indexed by their name, and attach to each the function
  body

* Find the ``@GRAPHify``-ed function

* Extract and record the arguments as graph inputs

* Walk the abstract syntax tree of the body, translating function calls into
  ``SQ`` instances and interconnect these with ``Port``s to represent the flow
  of values from ``SQ`` outputs to ``SQ`` inputs. There will be one ``Port``
  instance per ``SQ`` output with a value name, and a output ``Port`` will
  send its output value to all input ports sharing the value name. A ``Port``
  can be considered as an any-to-any directed connector from SQ outputs to SQ
  inputs.

* Once complete, write out a representation of the graph (``SQ`` instances and
  ``Port`` instances)
'''

import ast
import json
import pickle
import os

from .CompilerAssistVisitors import import_sqified_functions_from_module
from .CompilerRules import TTGraphCompilationVisitor
from .CompilerTypecheck import TTTypechecker
from .Error import TTSyntaxError
from .Graph import TTGraph
from . import DebugLogger
from .FiringRule import *
from . import CompilerAssistVisitors
from . import CompilerUtils

logger = DebugLogger.get_logger('Compiler')

from collections import defaultdict

# A useful tool:  https://python-ast-explorer.com


def label_dfs(graph, curr_node, curr_level, visited: set):
    '''
    Visit and label nodes in a bfs fashion from input to output direction.
    will (re)label the max path to a node. visited prevents infinite recursion
    caused by loops

    :return: Returns the max level in the graph

    :rtype: int
    '''

    # skip this node if visited more than input arcs (to prevent infinite loops)
    if curr_node in visited:
        # this node does not count in the level
        return curr_level - 1
    visited.add(curr_node)
    graph.nodes[curr_node]['level'] = max(
        curr_level, graph.nodes[curr_node].get('level', curr_level))
    max_level = curr_level

    for downstream_node in graph.successors(curr_node):
        visited.add(curr_node)
        max_level = max(
            max_level,
            label_dfs(graph, downstream_node, curr_level + 1, visited))

    # pop your node when you aren't on the path
    visited.remove(curr_node)
    return max_level


def add_topological_labels(graph, node_name_list):
    '''
    Adds topological labels to graph
    '''
    max_level = 0
    for node_name in node_name_list:
        max_level = max(max_level, label_dfs(graph, node_name, 0, set()))


def draw_graph(ttgraph: TTGraph, output_file_name):
    import networkx as nx
    '''
    Create and display a TTGraph
    '''
    graph_input_nodes = []
    graph_output_nodes = []
    graph = nx.MultiDiGraph()
    graph.add_nodes_from(ttgraph.sqs)  # internal nodes
    graph_edge_label_dict = {}

    ipp_to_sq = ttgraph.get_ipp_to_sq_dict()

    # Every arc in the symbol table should either have
    #   a source and at least one destination:  normal SQ
    #   a source but no destination:            graph output
    #   a destination but no source:            graph input

    source_vars = ttgraph.source_var_names()

    logger.debug(f'sources are {ttgraph.source_var_names()}')
    for source_var in source_vars:
        graph_input_nodes.append(source_var)
        graph.add_node(source_var, level=0)

        for dest_sq, port_num in ipp_to_sq[source_var]:
            graph.add_edge(source_var, dest_sq, label=source_var)
            graph_edge_label_dict[(source_var, dest_sq)] = source_var

    for sq in ttgraph.sqs:
        for opp in sq.get_opps():
            name = opp.data_name
            logger.debug(f"looking for {name} in ipps")

            if name in ipp_to_sq:
                logger.debug(f"{name} has dests {ipp_to_sq[name]}")

                # all inter-SQ links for the opp
                for dest_sq, port_num in ipp_to_sq[name]:
                    graph.add_edge(sq, dest_sq, label=name)
                    graph_edge_label_dict[(sq, dest_sq)] = name

            # sink opp
            else:
                logger.debug(f"{name} is a sink var")
                graph_output_nodes.append(name)
                graph.add_node(name, level=-1)
                graph.add_edge(sq, name, label=name)
                graph_edge_label_dict[(sq, name)] = name

    max_level = add_topological_labels(graph, graph_input_nodes)

    # color all nodes green first
    nx.set_node_attributes(graph, "green", name="fillcolor")
    # color nodes that will periodically execute
    nx.set_node_attributes(
        graph,
        {node: "lightblue"
         for node in ttgraph.sqs if node.is_streaming},
        name="fillcolor")
    # color streaming source nodes
    nx.set_node_attributes(graph, {
        node: 'orange'
        for node in ttgraph.sqs
        if node.firing_rule_type is TTFiringRuleType.TimedRetrigger
    },
                           name="fillcolor")
    nx.set_node_attributes(graph, {
        node: 'yellow'
        for node in ttgraph.sqs
        if node.firing_rule_type is TTFiringRuleType.SequentialRetrigger
    },
                           name="fillcolor")
    # all input output arcs painted in red
    nx.set_node_attributes(
        graph,
        {node: "red"
         for node in graph_input_nodes + graph_output_nodes},
        name="fillcolor")
    nx.set_node_attributes(graph, "filled", name="style")

    # switch to pygraphviz
    py_graph = nx.drawing.nx_agraph.to_agraph(graph)

    # set level of SQs (equivalent to rank) to be the same
    ranks = defaultdict(list)
    for node in py_graph.iternodes():
        rank = node.attr['level']
        ranks[rank].append(node)

    for node_list in ranks.values():
        py_graph.add_subgraph(node_list, rank='same')

    py_graph.layout('dot')
    py_graph.draw(output_file_name)
    logger.info(f"Saved graph image to {output_file_name}")

    # to allow base requirements
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    img = mpimg.imread(output_file_name)
    plt.imshow(img, aspect='equal')
    plt.axis('off')
    plt.show()


def print_text_graph(graph: TTGraph):
    for sq in graph.sqs:
        print(f"{sq}: IPPs={sq.get_ipps()}; OPPs={sq.get_opps()}")


def TTCompile(ttpython_path, library_path=None, deployment_path=None):
    '''
    Read a TTPython file and convert it to a ``TTGraph`` . Save the graph in one
    or several output formats.

    :param ttpython_path: the TTPython source file.

    :type ttpython_path: string

    :param deployment_path: Path to app-specific deployment YAML file.
        Specifies which device instances exist in the target cluster.
        Required for accurate per-device execution time estimates.

    :type deployment_path: string, optional

    :return: Returns the compiled graph

    :rtype: TTGraph
    '''

    if len(ttpython_path) == 0:
        raise Exception("The filename cannot be blank")

    # Initialize device profiles from deployment YAML before compilation.
    # This resolves device instances (e.g. cav0 -> raspberry_pi_4 specs) so that
    # task characterization produces estimates keyed by actual instance IDs.
    if deployment_path:
        import os
        device_types_path = os.path.join(os.path.dirname(os.path.abspath(ttpython_path)), 'device_types.yaml')
        if not os.path.exists(device_types_path):
            # Fallback: try relative to current working directory
            device_types_path = 'device_types.yaml'
        from .DeviceProfile import initialize_profiles
        initialize_profiles(device_types_path, deployment_path)
        
        # Also initialize network topology from the same deployment YAML.
        # network_types.yaml is the universal catalog (like device_types.yaml).
        network_types_path = os.path.join(os.path.dirname(os.path.abspath(ttpython_path)), 'network_types.yaml')
        if not os.path.exists(network_types_path):
            network_types_path = 'network_types.yaml'
        from .NetworkTopology import initialize_topology
        initialize_topology(network_types_path, deployment_path)

    with open(ttpython_path, "r") as source:
        module = ast.parse(source.read())

    with open(ttpython_path, "r") as source:
        source_list = source.readlines()

    # build an initial context
    context_creator = CompilerAssistVisitors.TTGraphContextCreation(
        source_list, library_path, CompilerUtils.Context(), ttpython_path)
    context_creator.visit(module)

    # Import the core BinOp library
    logger.debug("Importing the Instructions library")
    import_sqified_functions_from_module('.Instructions', library_path,
                                         context_creator.visited_modules,
                                         context_creator.context)

    typechecker = TTTypechecker(source_list, library_path,
                                context_creator.context, ttpython_path)
    typechecker.typecheck(module)

    try:
        # TTGraph holds the state of the translation
        compile_visitor = TTGraphCompilationVisitor(context_creator.context,
                                                    source=source_list,
                                                    pathname=ttpython_path)
        graph = compile_visitor.compile_graph(module)

        # Embed task characterization in SQ objects
        _embed_task_characterization(graph)

        # Embed deployment specification in graph for runtime use.
        # This eliminates the need to load deployment YAML at runtime.
        if deployment_path:
            graph.deployment_spec = _build_deployment_spec(deployment_path)
            graph.network_spec = _build_network_spec()

        # Embed deadline requirements and propagate to upstream SQs
        _embed_deadline_requirements(graph)

        for sq in graph.sqs:
            logger.debug(f"SQ {sq} will be assigned to {sq.constraints}")

        logger.info("Compilation successful")
        return graph

    except TTSyntaxError as error:
        logger.error(repr(error))
        raise error


def _embed_task_characterization(graph):
    """
    Enhanced task characterization with computational pattern analysis.
    Embeds sophisticated criticality analysis based on graph structure.
    """
    # Step 1: Build comprehensive dependency graph for structural analysis
    dependency_graph = _build_dependency_graph(graph)
    
    # Step 2: Analyze graph structure for each SQ
    criticality_analysis = _analyze_graph_structure_for_criticality(graph, dependency_graph)
    
    # Step 3: Embed enhanced characterization in SQ objects
    for sq in graph.sqs:
        sq_analysis = criticality_analysis.get(sq.sq_name, {})
        
        # Compute execution estimates first — this sets sq.complexity_metrics
        # which the criticality fallback uses
        sq.execution_time_estimates = _get_execution_estimates(sq)
        sq.energy_cost_estimates = _get_energy_estimates(sq)
        
        # Use structural analysis result or fallback to complexity-based heuristic
        sq.criticality = sq_analysis.get('final_criticality', _fallback_heuristic_criticality(sq))
        
        logger.debug(f"SQ {sq.sq_name}: criticality={sq.criticality}")
    
    # Step 4: Enhanced critical path analysis
    graph.critical_path_sqs = _find_enhanced_critical_path(graph, dependency_graph, criticality_analysis)
    
    # Step 5: Store dependency graph for potential runtime use
    graph.dependency_structure = dependency_graph
    
    # NEW Step 6: Analyze and embed data sizes for communication
    _embed_communication_data_sizes(graph)
    
    logger.info(f"Task characterization complete: {len(criticality_analysis)} SQs analyzed")
    logger.debug(f"Criticality distribution: {_get_criticality_distribution(criticality_analysis)}")

def _embed_deadline_requirements(graph):
    """
    Analyze deadline SQs and propagate deadline awareness to upstream paths.
    
    This enables:
    1. Static identification of deadline-critical paths
    2. Deadline-aware placement optimization
    3. Runtime budget tracking
    """
    # Find all DEADLINE SQs
    deadline_sqs = [sq for sq in graph.sqs if getattr(sq, 'use_deadline', False)]
    
    if not deadline_sqs:
        logger.debug("No deadline SQs found in graph")
        return
    
    logger.info(f"Found {len(deadline_sqs)} deadline SQ(s), analyzing paths...")
    
    # Build dependency graph for path tracing
    dependency_graph = _build_dependency_graph(graph)
    
    # Store deadline metadata at graph level
    graph.deadline_requirements = {
        'deadline_sqs': [],
        'deadline_paths': {},
        'total_deadline_budget_us': 0,
        'has_hard_deadlines': False,
        'has_soft_deadlines': False,
    }
    
    for deadline_sq in deadline_sqs:
        sq_name = deadline_sq.sq_name
        budget = getattr(deadline_sq, 'deadline_budget_us', None)
        d_type = getattr(deadline_sq, 'deadline_type', 'soft')
        
        # Record deadline SQ
        graph.deadline_requirements['deadline_sqs'].append({
            'sq_name': sq_name,
            'budget_us': budget,
            'type': d_type,
            'has_planb': getattr(deadline_sq, 'has_planb', False),
        })
        
        # Track deadline types
        if d_type == 'hard':
            graph.deadline_requirements['has_hard_deadlines'] = True
        else:
            graph.deadline_requirements['has_soft_deadlines'] = True
        
        # Trace upstream path and mark SQs
        upstream_path = _trace_upstream_path(sq_name, dependency_graph)
        graph.deadline_requirements['deadline_paths'][sq_name] = upstream_path
        
        # Mark upstream SQs as being on a deadline path
        for upstream_sq_name in upstream_path:
            upstream_sq = _find_sq_by_name(graph, upstream_sq_name)
            if upstream_sq:
                upstream_sq.on_deadline_path = True
                upstream_sq.deadline_sink = sq_name
                
                # Propagate budget hint (divide evenly as approximation)
                if budget and len(upstream_path) > 0:
                    upstream_sq.deadline_budget_hint_us = budget // len(upstream_path)
        
        if budget:
            graph.deadline_requirements['total_deadline_budget_us'] += budget
        
        logger.debug(f"Deadline path for {sq_name}: {upstream_path} (budget: {budget}us)")
    
    logger.info(f"Deadline analysis complete: {len(deadline_sqs)} deadlines, "
                f"hard={graph.deadline_requirements['has_hard_deadlines']}, "
                f"soft={graph.deadline_requirements['has_soft_deadlines']}")


def _trace_upstream_path(sq_name, dependency_graph, visited=None):
    """
    Trace all upstream SQs from a given SQ (used for deadline path identification).
    
    :param sq_name: Starting SQ name
    :param dependency_graph: Dependency graph from _build_dependency_graph
    :param visited: Set of visited nodes (for cycle detection)
    :return: List of upstream SQ names in topological order (sources first)
    """
    if visited is None:
        visited = set()
    
    if sq_name in visited:
        return []
    
    visited.add(sq_name)
    
    upstream_path = []
    
    sq_data = dependency_graph.get(sq_name, {})
    dependencies = sq_data.get('dependencies', [])
    
    # Recurse to dependencies first (topological order)
    for dep_name in dependencies:
        upstream_path.extend(_trace_upstream_path(dep_name, dependency_graph, visited))
    
    # Add current SQ
    upstream_path.append(sq_name)
    
    return upstream_path


def _find_sq_by_name(graph, sq_name):
    """Find an SQ object by name in the graph."""
    for sq in graph.sqs:
        if sq.sq_name == sq_name:
            return sq
    return None

def _embed_communication_data_sizes(graph):
    """Analyze and embed data sizes for each SQ output."""
    
    # Initialize graph-level communication size storage
    if not hasattr(graph, 'communication_data_sizes'):
        graph.communication_data_sizes = {}
    
    # Analyze each SQ's outputs
    for sq in graph.sqs:
        if hasattr(sq, 'opps'):
            for opp in sq.opps:
                # Estimate data size using the analysis functions
                estimated_size = _estimate_data_size_from_sq_analysis(sq, opp)
                
                # Store in output port object
                opp.estimated_data_size = estimated_size
                
                # Store in graph-level lookup for easy access
                graph.communication_data_sizes[opp.data_name] = estimated_size
                
                logger.debug(f"Embedded data size: {sq.sq_name}.{opp.data_name} = {estimated_size} bytes")
    
    logger.info(f"Communication data sizes embedded for {len(graph.communication_data_sizes)} data flows")

def _analyze_graph_structure_for_criticality(graph, dependency_graph):
    """
    Sophisticated graph analysis to determine task criticality based on structural importance.
    This is the key innovation - criticality from architectural position, not just naming.
    """
    if not graph.sqs:
        return {}
    
    criticality_analysis = {}
    
    # Calculate structural metrics for each SQ
    for sq in graph.sqs:
        sq_name = sq.sq_name
        
        # Calculate dependency metrics
        input_dependencies = len(dependency_graph.get(sq_name, {}).get('dependents', []))
        output_dependencies = len(dependency_graph.get(sq_name, {}).get('dependencies', []))
        
        # Calculate bottleneck score (critical junction analysis)
        bottleneck_score = _calculate_bottleneck_score(sq_name, dependency_graph)
        
        # Calculate pipeline position (input sources get high priority)
        pipeline_position = _calculate_pipeline_position(sq_name, dependency_graph)
        
        # Combine structural metrics
        structural_score = (
            input_dependencies * 0.4 +      # Heavy weight for being depended upon
            bottleneck_score * 0.4 +        # Heavy weight for being a bottleneck  
            pipeline_position * 0.2         # Lighter weight for pipeline position
        )
        
        # Determine architectural criticality based on structural importance
        if structural_score >= 7.0:  # High structural importance
            architectural_criticality = 'essential'
        elif structural_score >= 3.0:  # Medium structural importance
            architectural_criticality = 'important'
        else:  # Low structural importance
            architectural_criticality = 'normal'
        
        # Apply keyword-based heuristics as secondary factor
        heuristic_criticality = _fallback_heuristic_criticality(sq)
        
        # Architectural analysis takes precedence over heuristics
        if architectural_criticality == 'essential':
            final_criticality = 'essential'
        elif architectural_criticality == 'important' and heuristic_criticality != 'essential':
            final_criticality = 'important'  
        else:
            # Use heuristic when structural analysis is inconclusive
            final_criticality = heuristic_criticality
        
        criticality_analysis[sq_name] = {
            'final_criticality': final_criticality,
            'structural_score': structural_score,
            'architectural_criticality': architectural_criticality,
            'heuristic_criticality': heuristic_criticality,
            'input_dependencies': input_dependencies,
            'output_dependencies': output_dependencies,
            'bottleneck_score': bottleneck_score,
            'pipeline_position': pipeline_position
        }
        
        logger.debug(f"Criticality analysis for {sq_name}: {final_criticality} (structural: {structural_score:.1f})")
    
    return criticality_analysis

def _build_dependency_graph(graph):
    """Build explicit dependency relationships between SQs."""
    dependency_graph = {}
    
    # Initialize dependency structure for all SQs
    for sq in graph.sqs:
        dependency_graph[sq.sq_name] = {
            'dependencies': [],  # SQs this one depends on (inputs)
            'dependents': []     # SQs that depend on this one (outputs)
        }
    
    # Analyze data flow using TTGraph's existing structure
    ipp_to_sq = graph.get_ipp_to_sq_dict()
    
    # Build dependency relationships from IPP->SQ mappings
    for sq in graph.sqs:
        sq_name = sq.sq_name
        
        # Check output ports (OPPs) and find which SQs consume them
        if hasattr(sq, 'opps'):
            for opp in sq.opps:
                output_name = opp.data_name
                
                # Find SQs that consume this output
                if output_name in ipp_to_sq:
                    for dest_sq, port_num in ipp_to_sq[output_name]:
                        dest_name = dest_sq.sq_name
                        
                        # Create dependency: dest_sq depends on sq
                        if dest_name != sq_name:  # Avoid self-dependency
                            dependency_graph[dest_name]['dependencies'].append(sq_name)
                            dependency_graph[sq_name]['dependents'].append(dest_name)
        
        # Isolated SQs (no port connections) retain empty dependency lists.
        # Their structural score will be low, correctly yielding 'normal' criticality.
    
    # Remove duplicates
    for sq_name in dependency_graph:
        dependency_graph[sq_name]['dependencies'] = list(set(dependency_graph[sq_name]['dependencies']))
        dependency_graph[sq_name]['dependents'] = list(set(dependency_graph[sq_name]['dependents']))
    
    return dependency_graph

def _calculate_bottleneck_score(sq_name, dependency_graph):
    """Calculate how much of a bottleneck this SQ represents."""
    sq_data = dependency_graph.get(sq_name, {})
    dependencies = sq_data.get('dependencies', [])
    dependents = sq_data.get('dependents', [])
    
    # Bottleneck indicators:
    # 1. Convergence point (multiple inputs)
    # 2. Fan-out point (multiple outputs)  
    # 3. Critical path bridge (connects clusters)
    
    convergence_score = len(dependencies) * 2    # Multiple inputs = convergence
    fanout_score = len(dependents) * 1.5         # Multiple outputs = fan-out
    
    # Critical path detection - connects upstream to downstream
    critical_path_score = 0
    if len(dependencies) > 0 and len(dependents) > 0:
        # SQ bridges input and output - potential critical path
        critical_path_score = 2
        
        # Extra score if it's the ONLY bridge
        if len(dependencies) == 1 and len(dependents) == 1:
            critical_path_score += 1
    
    return convergence_score + fanout_score + critical_path_score

def _calculate_pipeline_position(sq_name, dependency_graph):
    """Calculate position in processing pipeline (input sources = high importance).""" 
    sq_data = dependency_graph.get(sq_name, {})
    dependencies = sq_data.get('dependencies', [])
    dependents = sq_data.get('dependents', [])
    
    # Input sources (no dependencies) get highest importance
    if not dependencies:
        return 5.0  # High importance for data sources
    
    # Pure outputs (no dependents) get lower importance  
    if not dependents:
        return 1.0  # Lower importance for final outputs
    
    # Calculate depth from sources using graph traversal
    max_depth = _calculate_depth_from_sources(sq_name, dependency_graph)
    
    # Earlier in pipeline = higher importance
    return max(5.0 - max_depth * 0.5, 1.0)

def _calculate_depth_from_sources(sq_name, dependency_graph, visited=None, current_depth=0):
    """Calculate maximum depth from input sources."""
    if visited is None:
        visited = set()
    
    if sq_name in visited or current_depth > 10:  # Prevent infinite recursion
        return current_depth
    
    visited.add(sq_name)
    
    dependencies = dependency_graph.get(sq_name, {}).get('dependencies', [])
    
    if not dependencies:  # Input source
        return 0
    
    # Find maximum depth through all dependency paths
    max_upstream_depth = 0
    for dep in dependencies:
        upstream_depth = _calculate_depth_from_sources(dep, dependency_graph, visited.copy(), current_depth + 1)
        max_upstream_depth = max(max_upstream_depth, upstream_depth)
    
    return max_upstream_depth + 1


def _find_enhanced_critical_path(graph, dependency_graph, criticality_analysis):
    """Enhanced critical path identification using structural analysis."""
    critical_sqs = set()
    
    # Original logic: include sources and multi-input SQs
    for sq in graph.sqs:
        sq_name = sq.sq_name
        sq_data = dependency_graph.get(sq_name, {})
        
        # Input sources are critical
        if not sq_data.get('dependencies', []):
            critical_sqs.add(sq_name)
        
        # Multi-input convergence points are critical
        if len(sq_data.get('dependencies', [])) > 1:
            critical_sqs.add(sq_name)
    
    # Enhanced logic: include high structural importance SQs
    for sq_name, analysis in criticality_analysis.items():
        structural_score = analysis.get('structural_score', 0.0)
        
        # High structural importance = critical
        if structural_score >= 5.0:
            critical_sqs.add(sq_name)
        
        # High bottleneck score = critical  
        if analysis.get('bottleneck_score', 0.0) >= 4.0:
            critical_sqs.add(sq_name)
    
    return critical_sqs

def _fallback_heuristic_criticality(sq):
    """
    Complexity-based criticality fallback when structural analysis is inconclusive.
    
    Uses the composite complexity score from AST analysis to determine
    whether a task performs significant computation. This replaces the
    previous name-matching approach which was unreliable across domains
    (see Section 4.3, "Limitations of Heuristic Analysis").
    
    Only invoked when structural score < 7.0 (not definitively essential).
    Structural analysis remains authoritative — this can elevate but
    never lower the final criticality.
    
    Thresholds:
        composite >= 15.0 → essential (heavy computation, many branches/calls)
        composite >= 8.0  → important (moderate computation)
        composite < 8.0   → normal (simple/thin wrapper)
    """
    # complexity_metrics is set by _estimate_base_execution_time_from_task
    # which runs earlier in _embed_task_characterization.
    # If not yet available, compute it now.
    if not hasattr(sq, 'complexity_metrics'):
        function_node = getattr(sq, 'function_ast_node', None)
        metrics = _calculate_task_complexity(function_node)
        sq.complexity_metrics = metrics
    
    score = sq.complexity_metrics['composite_score']
    
    if score >= 15.0:
        return "essential"
    elif score >= 8.0:
        return "important"
    else:
        return "normal"
    

def _get_criticality_distribution(criticality_analysis):
    """Get distribution of criticality levels for logging."""
    distribution = {'essential': 0, 'important': 0, 'normal': 0}
    
    for analysis in criticality_analysis.values():
        criticality = analysis.get('final_criticality', 'normal')
        distribution[criticality] += 1
    
    return distribution

def _build_deployment_spec(deployment_path):
    """
    Build resolved deployment specification for embedding in graph.
    
    Reads the deployment YAML and resolves each device instance against the
    already-initialized DeviceProfileManager. The resulting dict contains
    everything RuntimeManager needs to set up device mappings, eliminating
    the need to load a separate deployment YAML at runtime.
    
    :param deployment_path: Path to deployment YAML file
    :return: Dict of {device_id: resolved_spec}
    """
    import yaml, os
    from .DeviceProfile import get_profile_manager
    
    if not deployment_path or not os.path.exists(deployment_path):
        return {}
    
    pm = get_profile_manager()
    
    with open(deployment_path, 'r') as f:
        config = yaml.safe_load(f)
    
    deployment_spec = {}
    for device_config in config.get('devices', []):
        device_id = device_config.get('id')
        if not device_id:
            continue
        
        device_type = device_config.get('type', 'unknown')
        profile = pm.get_profile(device_id)
        
        memory_mb = int(profile.memory_size / (1024 * 1024))
        
        deployment_spec[device_id] = {
            'type': device_type,
            'cpu_speed': profile.cpu_speed,
            'memory_size': profile.memory_size,
            'memory_mb': memory_mb,
            'cpu_cores': 1,
            'compute_slots': 1,
            'has_gpu': device_config.get('components', {}).get('gpu', False),
            'power_active': profile.power_active,
            'power_idle': profile.power_idle,
            'components': device_config.get('components', {}),
            'location': device_config.get('location', 'unknown'),
        }
    
    logger.info(f"Built deployment spec: {len(deployment_spec)} devices for graph embedding")
    return deployment_spec

def _build_network_spec():
    """
    Build resolved network topology for embedding in graph.
    
    Reads from the already-initialized NetworkTopology singleton.
    The resulting dict contains latency and bandwidth for each link,
    eliminating the need to load deployment YAML at runtime.
    
    :return: Dict of {(device_a, device_b): {'latency': float, 'bandwidth': float}}
    """
    from .NetworkTopology import get_network_topology
    
    topology = get_network_topology()
    
    network_spec = {}
    for (dev_a, dev_b), latency in topology.latency.items():
        bandwidth = topology.bandwidth.get((dev_a, dev_b), topology.default_bandwidth)
        network_spec[(dev_a, dev_b)] = {
            'latency': latency,
            'bandwidth': bandwidth
        }
    
    logger.info(f"Built network spec: {len(network_spec) // 2} bidirectional links for graph embedding")
    return network_spec

def _get_execution_estimates(sq):
    """
    Generate execution time estimates using resolved device instance profiles.
    
    Iterates over DeviceProfileManager.profiles (resolved instances like 'cav0', 'rsu')
    NOT device_types (the raw catalog). This produces estimates keyed by actual device
    instance IDs that SmartMapper can look up directly during placement.
    
    Requires DeviceProfileManager to be initialized with both device_types.yaml
    and a deployment YAML before this is called (done by TTCompile).
    """
    from .DeviceProfile import get_profile_manager
    pm = get_profile_manager()
    
    base_time = _estimate_base_execution_time_from_task(sq)
    
    estimates = {}
    
    # Calculate for each resolved device instance
    for device_id, profile in pm.profiles.items():
        estimates[device_id] = base_time / profile.cpu_speed
    
    # Always include 'default' for fallback
    estimates['default'] = base_time
    
    logger.debug(f"Generated execution estimates for {sq.sq_name}: "
                f"{len(pm.profiles)} devices + default")
    return estimates

class _TaskComplexityVisitor(ast.NodeVisitor):
    """
    Compute a composite complexity score from Python AST analysis.
    
    Combines three orthogonal indicators:
    
    1. Cyclomatic complexity (McCabe, 1976): Number of independent paths
       through control flow. Measures branching/conditional density.
       
    2. Code volume: Total AST node count as proxy for the amount of
       computation expressed in the function body.
       
    3. Structural depth: Maximum loop nesting depth. Nested loops
       multiply computational work (O(n) vs O(n^2) vs O(n^3)).
    
    These combine to produce more realistic differentiation than any
    single metric alone. A function with one deeply nested loop over
    large data scores higher than a function with many shallow branches.
    """
    
    def __init__(self):
        self.decision_count = 0
        self.node_count = 0
        self.call_count = 0
        self.max_loop_depth = 0
        self._current_loop_depth = 0
    
    def generic_visit(self, node):
        self.node_count += 1
        super().generic_visit(node)
    
    # Branching statements
    def visit_If(self, node):
        self.decision_count += 1
        self.node_count += 1
        self.generic_visit(node)
    
    def visit_For(self, node):
        self.decision_count += 1
        self.node_count += 1
        self._current_loop_depth += 1
        self.max_loop_depth = max(self.max_loop_depth, self._current_loop_depth)
        self.generic_visit(node)
        self._current_loop_depth -= 1
    
    def visit_While(self, node):
        self.decision_count += 1
        self.node_count += 1
        self._current_loop_depth += 1
        self.max_loop_depth = max(self.max_loop_depth, self._current_loop_depth)
        self.generic_visit(node)
        self._current_loop_depth -= 1
    
    def visit_ExceptHandler(self, node):
        self.decision_count += 1
        self.node_count += 1
        self.generic_visit(node)
    
    def visit_Assert(self, node):
        self.decision_count += 1
        self.node_count += 1
        self.generic_visit(node)
    
    def visit_BoolOp(self, node):
        self.decision_count += len(node.values) - 1
        self.node_count += 1
        self.generic_visit(node)
    
    def visit_IfExp(self, node):
        self.decision_count += 1
        self.node_count += 1
        self.generic_visit(node)
    
    def visit_comprehension(self, node):
        self.decision_count += len(node.ifs)
        self.node_count += 1
        self._current_loop_depth += 1
        self.max_loop_depth = max(self.max_loop_depth, self._current_loop_depth)
        self.generic_visit(node)
        self._current_loop_depth -= 1
    
    def visit_Call(self, node):
        self.call_count += 1
        self.node_count += 1
        self.generic_visit(node)


def _calculate_task_complexity(function_ast_node):
    """
    Calculate composite complexity score for a function AST.
    
    Score = cyclomatic_complexity 
            + (node_count / 20)
            + (call_count * 0.5) 
            + (loop_depth_penalty)
    
    Where loop_depth_penalty = 2^(max_depth) - 1 to reflect
    multiplicative cost of nested loops.
    
    :param function_ast_node: ast.FunctionDef node
    :return: Dict with individual metrics and composite score
    """
    if function_ast_node is None:
        return {
            'cyclomatic': 1,
            'node_count': 0,
            'call_count': 0,
            'max_loop_depth': 0,
            'composite_score': 1.0
        }
    
    visitor = _TaskComplexityVisitor()
    visitor.visit(function_ast_node)
    
    cyclomatic = 1 + visitor.decision_count
    
    # Node count normalized — every ~20 nodes adds 1 point of complexity
    volume_score = visitor.node_count / 20.0
    
    # Each function call represents delegated work
    call_score = visitor.call_count * 0.5
    
    # Nested loops multiply work exponentially
    loop_penalty = (2 ** visitor.max_loop_depth) - 1
    
    composite = cyclomatic + volume_score + call_score + loop_penalty
    
    return {
        'cyclomatic': cyclomatic,
        'node_count': visitor.node_count,
        'call_count': visitor.call_count,
        'max_loop_depth': visitor.max_loop_depth,
        'composite_score': composite
    }

def _estimate_base_execution_time_from_task(sq):
    """
    Estimate base execution time on a reference device (cpu_speed=1.0)
    using composite AST complexity analysis.
    
    Combines McCabe's cyclomatic complexity (McCabe, 1976) with code
    volume, function call count, and loop nesting depth to produce a
    composite score that correlates with computational cost.
    
    The score maps to base execution time via a linear scaling with
    floor and ceiling:
    
        base_time = clamp(score * SCALE_FACTOR, MIN_TIME, MAX_TIME)
    
    where SCALE_FACTOR calibrates to typical edge computing workloads
    in the 5-100ms range on reference hardware.
    
    Device-specific estimates are then computed by _get_execution_estimates
    which divides base_time by each device's cpu_speed factor.
    
    Limitation: External library calls (e.g., cv2.detectObjects) have
    low AST complexity but high actual cost. The profiling/learning
    mechanism is the long-term solution for these cases.
    
    :param sq: The SQ to analyze
    :return: Base execution time in seconds
    """
    function_node = getattr(sq, 'function_ast_node', None)
    metrics = _calculate_task_complexity(function_node)
    
    # Store metrics on the SQ for inspection and debugging
    sq.complexity_metrics = metrics
    
    score = metrics['composite_score']
    
    # Linear scaling: 1ms per complexity point, with floor and ceiling.
    # Calibrated for edge computing workloads where tasks typically
    # execute in the 5-100ms range on reference hardware.
    SCALE_FACTOR = 0.003   # 3ms per complexity point
    MIN_TIME = 0.005       # 5ms floor (even trivial tasks have overhead)
    MAX_TIME = 0.150       # 150ms ceiling
    
    base_time = max(MIN_TIME, min(MAX_TIME, score * SCALE_FACTOR))
    
    logger.debug(f"SQ {sq.sq_name} ({sq.function_name}): "
                f"complexity={score:.1f} (M={metrics['cyclomatic']}, "
                f"nodes={metrics['node_count']}, calls={metrics['call_count']}, "
                f"depth={metrics['max_loop_depth']}), base_time={base_time*1000:.1f}ms")
    
    return base_time

def _get_energy_estimates(sq):
    """
    Generate energy estimates using resolved device instance profiles.
    
    Energy = Power x Time. Uses the same resolved profiles as _get_execution_estimates.
    """
    from .DeviceProfile import get_profile_manager
    pm = get_profile_manager()
    
    time_estimates = _get_execution_estimates(sq)
    
    energy_estimates = {}
    for device_id, exec_time in time_estimates.items():
        if device_id in pm.profiles:
            power_active = pm.profiles[device_id].power_active
        else:
            power_active = 5.0  # Default 5W for 'default' key
        energy_estimates[device_id] = power_active * exec_time
    
    logger.debug(f"Generated energy estimates for {sq.sq_name}: {len(energy_estimates)} devices")
    return energy_estimates

def _get_learned_data_size(sq_name, output_name):
    """Get previously learned data size for this SQ output."""
    learned_sizes_file = 'learned_data_sizes.pkl'
    
    try:
        import pickle
        import os
        
        if os.path.exists(learned_sizes_file):
            with open(learned_sizes_file, 'rb') as f:
                learned_data = pickle.load(f)
            
            sq_data = learned_data.get(sq_name, {})
            return sq_data.get(output_name)
            
    except Exception as e:
        logger.debug(f"Could not load learned data sizes: {e}")
    
    return None

def _store_learned_data_size(sq_name, output_name, actual_size):
    """Store actual data size for future compilations."""
    learned_sizes_file = 'learned_data_sizes.pkl'
    
    try:
        import pickle
        import os
        
        # Load existing data
        learned_data = {}
        if os.path.exists(learned_sizes_file):
            with open(learned_sizes_file, 'rb') as f:
                learned_data = pickle.load(f)
        
        # Update with new data
        if sq_name not in learned_data:
            learned_data[sq_name] = {}
        learned_data[sq_name][output_name] = actual_size
        
        # Save back to single pickle file
        with open(learned_sizes_file, 'wb') as f:
            pickle.dump(learned_data, f)
        
        logger.info(f"Stored learned data size: {sq_name}.{output_name} = {actual_size} bytes")
        
    except Exception as e:
        logger.warning(f"Could not store learned data size: {e}")

class _ReturnSizeVisitor(ast.NodeVisitor):
    """
    Analyzes a function AST to estimate output data size from structural
    properties of the return statement.

    Examines:
    - Number of elements in the return list/tuple
    - Whether any returned variables were built via accumulation
      (.append() or .extend() calls on named variables)

    These are structural indicators of output size that do not depend
    on function or variable naming conventions.
    """

    def __init__(self):
        self.return_element_count = 1
        self.accumulated_names = set()  # Names that had .append()/.extend()
        self._return_elts = []

    def visit_Call(self, node):
        # Detect name.append(...) or name.extend(...) anywhere in the function.
        # Whether inside a loop or sequential, the target variable is
        # accumulating data and will be larger than a scalar.
        if (isinstance(node.func, ast.Attribute)
                and node.func.attr in ('append', 'extend')
                and isinstance(node.func.value, ast.Name)):
            self.accumulated_names.add(node.func.value.id)
        self.generic_visit(node)

    def visit_Return(self, node):
        if node.value is None:
            self.return_element_count = 0
        elif isinstance(node.value, (ast.List, ast.Tuple)):
            self.return_element_count = len(node.value.elts)
            self._return_elts = node.value.elts
        elif isinstance(node.value, ast.Constant):
            self.return_element_count = 1
        else:
            self.return_element_count = 1
        self.generic_visit(node)

    def count_accumulated_in_return(self):
        """How many return elements reference accumulated variables."""
        count = 0
        for elt in self._return_elts:
            if isinstance(elt, ast.Name) and elt.id in self.accumulated_names:
                count += 1
        return count


def _estimate_output_data_size(sq):
    """
    Estimate total output data size from structural AST analysis and
    port topology.

    Combines three structural indicators:

    1. Return element count: Number of elements in the return list.
       Each element contributes a base size to the total.

    2. Accumulation detection: Variables built via .append() or .extend()
       produce variable-length collections, significantly larger than
       scalar values. Each accumulated element in the return gets a
       collection multiplier.

    3. Input fan-in: Tasks with more input ports aggregate data from
       multiple sources. Output size scales with input count.

    The learning mechanism (_get_learned_data_size) takes priority
    over this estimate when runtime profiling data is available.

    :param sq: The SQ to analyze
    :return: Estimated total output size in bytes
    """
    function_node = getattr(sq, 'function_ast_node', None)

    if function_node is None:
        return 1024  # 1KB default when no AST available

    visitor = _ReturnSizeVisitor()
    visitor.visit(function_node)

    # Base size per return element (scalar value + serialization overhead)
    ELEMENT_BASE_SIZE = 256  # bytes

    # Multiplier for elements that are accumulated collections
    COLLECTION_MULTIPLIER = 8  # accumulated lists are ~8x a scalar

    # Count regular vs accumulated return elements
    accumulated_count = visitor.count_accumulated_in_return()
    scalar_count = max(visitor.return_element_count - accumulated_count, 0)

    # Size from return structure
    return_size = (scalar_count * ELEMENT_BASE_SIZE +
                   accumulated_count * ELEMENT_BASE_SIZE * COLLECTION_MULTIPLIER)

    # Fan-in multiplier: tasks with more inputs aggregate more data
    input_count = len(sq.ipps) if hasattr(sq, 'ipps') else 0
    fan_in_multiplier = 1.0 + (input_count * 0.3)

    estimated_size = int(return_size * fan_in_multiplier)

    # Floor and ceiling
    estimated_size = max(64, min(estimated_size, 512 * 1024))  # 64B to 512KB

    logger.debug(f"SQ {sq.sq_name} output size estimate: "
                f"return_elements={visitor.return_element_count}, "
                f"accumulated={accumulated_count}, inputs={input_count}, "
                f"total={estimated_size} bytes")

    return estimated_size  

def _estimate_data_size_from_sq_analysis(sq, opp):
    """
    Estimate data size for a single output port.

    Priority order:
    1. Learned sizes from runtime measurement (learned_data_sizes.pkl)
    2. Structural AST analysis of return statement and port topology

    The learning mechanism is populated by Engine._persist_learned_data_size
    after measuring actual data sizes over 5 invocations at runtime.
    On subsequent compilations, those measurements replace the structural
    estimates automatically.
    """
    # Check for learned sizes first (populated by runtime measurement)
    learned_size = _get_learned_data_size(sq.sq_name, opp.data_name)
    if learned_size:
        logger.debug(f"Using learned data size for {sq.sq_name}.{opp.data_name}: {learned_size} bytes")
        return learned_size

    # Structural analysis: estimate total function output, divide by port count
    total_size = _estimate_output_data_size(sq)
    num_ports = len(sq.opps) if hasattr(sq, 'opps') else 1
    estimated_size = total_size // max(num_ports, 1)

    logger.debug(f"Estimated data size for {sq.sq_name}.{opp.data_name}: "
                f"{estimated_size} bytes (total={total_size}, ports={num_ports})")

    return estimated_size


def dump_json(graph, json_path):
    '''
    Dumps a TTGraph out as JSON
    '''
    logger.info(f"Writing {json_path}")
    message = "JSON output may fail if keyword arguments contain "
    message += "objects that lack a JSON serialization."
    logger.warning(message)
    with open(json_path, "w") as json_out:
        json.dump(graph.json(), json_out, indent=4)


def dump_pickle(graph, pickle_path):
    '''
    Dumps a TTGraph out as Pickle
    '''
    logger.info(f"Writing {pickle_path}")
    with safe_open(pickle_path, 'wb') as pickle_out:
        pickle.dump(graph, pickle_out)


def safe_open(path, args):
    '''
    Open "path" for writing, creating any parent directories as needed.
    '''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, args)
