# Copyright 2025 The Authors
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
Combiner: SSPG-Based Multi-Application Graph Composition for TTPython

Combines N independently-compiled TTGraph objects into a single unified graph
using Synchronized Series-Parallel Graph (SSPG) unordered parallel composition.

Theoretical Foundation: Alur, Stanford, Watson (POPL 2023)
"A Robust Theory of Series Parallel Graphs"

Key Principle: Combine early, optimize globally, decompose for contention, execute unified.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
from copy import deepcopy
from enum import Enum


logger = logging.getLogger('Combiner')


class SSPGEdgeType(Enum):
    """SSPG edge labels: Γ = {seq, sl, sr, su, jl, jr, ju, sync}"""
    SEQ = 'seq'
    SL = 'sl'
    SR = 'sr'
    SU = 'su'
    JL = 'jl'
    JR = 'jr'
    JU = 'ju'
    SYNC = 'sync'
    INTERNAL = 'int'


@dataclass
class SyntheticPort:
    """Minimal port for synthetic SQs."""
    data_name: str
    is_streaming: bool = False


class SyntheticSQ:
    """
    Synthetic scheduling quantum for SSPG composition.
    Zero-WCET dummy nodes per Melani et al. (IEEE Trans. Computers 2015).
    """
    
    def __init__(self, sq_name: str, function_name: str, 
                 synthetic_type: str, connected_apps: List[str]):
        self.sq_name = sq_name
        self.function_name = function_name
        self.synthetic_type = synthetic_type
        self.connected_apps = connected_apps
        
        self.execution_time_estimates = {'default': 0.0}
        self.energy_cost_estimates = {'default': 0.0}
        self.memory_required = 0
        self.resource_requirements = {'compute_slots': 0, 'memory_mb': 0, 'exclusive': False}
        
        self._ipps: List[SyntheticPort] = []
        self._opps: List[SyntheticPort] = []
        self.constraints = []
        self.is_streaming = False
        self.firing_rule_type = None
        self.criticality = 'normal'
    
    def add_input_port(self, data_name: str):
        self._ipps.append(SyntheticPort(data_name=data_name))
    
    def add_output_port(self, data_name: str):
        self._opps.append(SyntheticPort(data_name=data_name))
    
    def get_ipps(self) -> List[SyntheticPort]:
        return self._ipps
    
    def get_opps(self) -> List[SyntheticPort]:
        return self._opps
    
    # Duck-type compatibility with TTSQ (Mapper.generate_mapping accesses .opps/.ipps directly)
    @property
    def opps(self):
        return self._opps
    
    @property
    def ipps(self):
        return self._ipps
    
    def __repr__(self):
        return f"SyntheticSQ({self.sq_name})"
    
    def __str__(self):
        return self.sq_name
    
    def __hash__(self):
        return hash(self.sq_name)
    
    def __eq__(self, other):
        if isinstance(other, SyntheticSQ):
            return self.sq_name == other.sq_name
        return False


class CombinedGraph:
    """
    Combined TTGraph via SSPG unordered parallel composition.
    
    Maintains TTGraph interface compatibility while exposing app boundaries
    for multitenancy decomposition.
    """
    
    def __init__(self, name: str, app_ids: List[str]):
        # TTGraph-compatible attributes
        self.graph_name = name
        self.sqs: List[Any] = []
        self.clock_dictionary: Dict = {}
        self.trigger_port_name = 'unified_trigger'
        self.source_vars: Dict[str, Any] = {}
        self.flattened_constraints: List = []
        self.deadline_requirements = None

        
        # Composition-specific attributes
        self.app_ids = app_ids
        self.super_trigger: Optional[SyntheticSQ] = None
        self.barrier_join: Optional[SyntheticSQ] = None
        
        # Track subgraph structure (CRITICAL for multitenancy)
        self.subgraph_info: Dict[str, Dict] = {}
        
        # SSPG edge metadata
        self.sspg_edges: List[Dict] = []
        
        # App configurations (criticality, etc.)
        self.app_configs: Dict[str, Dict] = {}
        self.shared_boilerplate: List[Any] = []
        self.deployment_spec: Dict = {}
    
    def get_ipp_to_sq_dict(self):
        """Generate input port to SQ mapping, resolving synthetic SQs.
        
        SUPER_TRIGGER is a compile-time SSPG abstraction that cannot be
        instantiated on devices (no sq_sync/sq_execute). This override
        resolves the fan-out at query time: 'unified_trigger' maps directly
        to each sub-application's real source SQs, bypassing the synthetic
        node entirely. BARRIER_JOIN is similarly excluded — its IPP names
        (ju__*) don't match any real SQ's OPPs, so it's already isolated.
        """
        ipp_to_sq = defaultdict(set)
        for sq in self.sqs:
            if isinstance(sq, SyntheticSQ):
                continue
            for port_num, ipp in enumerate(sq.get_ipps()):
                ipp_to_sq[ipp.data_name].add((sq, port_num))
        
        # Resolve SUPER_TRIGGER fan-out: route unified_trigger directly
        # to real source SQs that have a 'trigger' input port
        if self.super_trigger:
            for app_id, info in self.subgraph_info.items():
                for src_sq in info['sources']:
                    for port_num, ipp in enumerate(src_sq.get_ipps()):
                        if ipp.data_name in ('trigger', 'unified_trigger'):
                            ipp_to_sq['unified_trigger'].add((src_sq, port_num))
        
        return ipp_to_sq
    
    def get_dag(self):
        """Build and cache the dataflow DAG for the combined graph."""
        if hasattr(self, '_cached_dag') and self._cached_dag is not None:
            return self._cached_dag
        
        import networkx as nx
        
        G = nx.DiGraph()
        for sq in self.sqs:
            G.add_node(sq.sq_name, sq=sq)
        
        ipp_to_sq = self.get_ipp_to_sq_dict()
        for sq in self.sqs:
            for opp in sq.opps:
                data_name = opp.data_name
                if data_name in ipp_to_sq:
                    for dest_sq, port_num in ipp_to_sq[data_name]:
                        if dest_sq.sq_name != sq.sq_name:
                            G.add_edge(sq.sq_name, dest_sq.sq_name)

        for edge in self.sspg_edges:
            G.add_edge(edge['source'], edge['target'])

        self._cached_dag = G
        return G
    
    def source_var_names(self):
        """Return source variables (TTGraph interface)."""
        return self.source_vars
    
    def set_flattened_constraints(self, constraints):
        """Set constraints (TTGraph interface)."""
        self.flattened_constraints = constraints
    
    # ==================== MULTITENANCY BRIDGE METHODS ====================
    
    def get_app_ids(self) -> List[str]:
        """Return list of application identifiers in this combined graph."""
        return self.app_ids
    
    def get_sqs_for_app(self, app_id: str) -> List[Any]:
        """Return all SQs belonging to a specific application (excluding synthetic)."""
        prefix = f"{app_id}__"
        return [sq for sq in self.sqs 
                if hasattr(sq, 'sq_name') and sq.sq_name.startswith(prefix)]
    
    def get_app_for_sq(self, sq_name: str) -> Optional[str]:
        """Extract app_id from a prefixed SQ name."""
        if '__' in sq_name:
            return sq_name.split('__', 1)[0]
        return None
    
    def get_original_sq_name(self, prefixed_name: str) -> str:
        """Extract original SQ name from prefixed name."""
        if '__' in prefixed_name:
            return prefixed_name.split('__', 1)[1]
        return prefixed_name
    
    def get_app_config(self, app_id: str) -> Dict:
        """
        Return app-level config for multitenancy calculations.
        
        :param app_id: Application identifier
        :return: Dict with graph reference, sq_count, etc.
        """
        if app_id not in self.app_configs:
            info = self.subgraph_info.get(app_id, {})
            return {
                'app_id': app_id,
                'sq_count': info.get('sq_count', 0),
                'graph': self,
            }
        return self.app_configs[app_id]
    
    def decompose_mapping(self, flat_mapping: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """
        Convert flat mapping {prefixed_sq: device} to per-app format.
        
        This bridges CombinedGraph to the multitenancy infrastructure which
        expects {app_id: {sq_name: device}}.
        
        :param flat_mapping: Mapping from prefixed SQ names to devices
        :return: Per-app mapping dictionary
        """
        app_mappings = {app_id: {} for app_id in self.app_ids}
        
        for sq_name, device in flat_mapping.items():
            # Skip synthetic SQs
            if sq_name in ['SUPER_TRIGGER', 'BARRIER_JOIN']:
                continue
            
            app_id = self.get_app_for_sq(sq_name)
            if app_id and app_id in app_mappings:
                original_name = self.get_original_sq_name(sq_name)
                app_mappings[app_id][original_name] = device
        
        return app_mappings
    
    def build_app_configs_dict(self) -> Dict[str, Dict]:
        """
        Build app_configs dictionary for multitenancy calculations.
        
        :return: {app_id: {'graph': graph, ...}}
        """
        configs = {}
        for app_id in self.app_ids:
            configs[app_id] = {
                'app_id': app_id,
                'graph': self._build_virtual_graph_for_app(app_id),
            }
        return configs
    
    def _build_virtual_graph_for_app(self, app_id: str):
        """
        Build a virtual graph-like object for an app's SQs.
        
        CRITICAL: Returns SQs with UNPREFIXED names to match decompose_mapping()
        output format. Uses deepcopy to avoid mutating CombinedGraph SQs.
        """
        class VirtualGraph:
            """Lightweight graph-like object for a single app's SQs.
            Implements get_dag() and get_ipp_to_sq_dict() so SmartMapper's
            contention detection can traverse the dataflow DAG."""
            def __init__(self, sqs):
                self.sqs = sqs
                self._cached_dag = None
            
            def get_ipp_to_sq_dict(self):
                from collections import defaultdict
                ipp_to_sq = defaultdict(set)
                for sq in self.sqs:
                    for port_num, ipp in enumerate(sq.get_ipps()):
                        ipp_to_sq[ipp.data_name].add((sq, port_num))
                return ipp_to_sq
            
            def get_dag(self):
                if self._cached_dag is not None:
                    return self._cached_dag
                import networkx as nx
                G = nx.DiGraph()
                for sq in self.sqs:
                    G.add_node(sq.sq_name, sq=sq)
                ipp_to_sq = self.get_ipp_to_sq_dict()
                for sq in self.sqs:
                    for opp in sq.opps:
                        data_name = opp.data_name
                        if data_name in ipp_to_sq:
                            for dest_sq, port_num in ipp_to_sq[data_name]:
                                if dest_sq.sq_name != sq.sq_name:
                                    G.add_edge(sq.sq_name, dest_sq.sq_name)
                self._cached_dag = G
                return G
        
        prefix = f"{app_id}__"
        app_sqs = []
        for sq in self.get_sqs_for_app(app_id):
            sq_copy = deepcopy(sq)
            if sq_copy.sq_name.startswith(prefix):
                sq_copy.sq_name = sq_copy.sq_name[len(prefix):]
            app_sqs.append(sq_copy)
        
        return VirtualGraph(app_sqs)
    
    def is_combined_graph(self) -> bool:
        """Return True to identify this as a CombinedGraph."""
        return True


# ============================================================================
# Graph Analysis
# ============================================================================

def _analyze_graph(graph, app_id: str) -> Dict[str, Any]:
    """Analyze a TTGraph to identify sources, sinks, and structure."""
    analysis = {
        'app_id': app_id,
        'graph_name': graph.graph_name,
        'sources': [],
        'sinks': [],
        'all_sqs': list(graph.sqs),
    }
    
    ipp_to_sq = graph.get_ipp_to_sq_dict()
    
    all_consumed = set()
    for sq in graph.sqs:
        for ipp in sq.get_ipps():
            all_consumed.add(ipp.data_name)
    
    # Handle source_vars as either dict or list
    if graph.source_vars:
        if isinstance(graph.source_vars, dict):
            source_vars = set(graph.source_vars.keys())
        elif isinstance(graph.source_vars, list):
            source_vars = set(graph.source_vars)
        else:
            source_vars = set()
    else:
        source_vars = set()
        
    trigger_port = getattr(graph, 'trigger_port_name', 'trigger')
    source_vars.add(trigger_port)
    
    for sq in graph.sqs:
        for ipp in sq.get_ipps():
            if ipp.data_name in source_vars:
                if sq not in analysis['sources']:
                    analysis['sources'].append(sq)
    
    for sq in graph.sqs:
        for opp in sq.get_opps():
            if opp.data_name not in all_consumed:
                if sq not in analysis['sinks']:
                    analysis['sinks'].append(sq)
    
    # Fallbacks
    if not analysis['sources'] and analysis['all_sqs']:
        analysis['sources'].append(analysis['all_sqs'][0])
    if not analysis['sinks'] and analysis['all_sqs']:
        analysis['sinks'].append(analysis['all_sqs'][-1])
    
    return analysis

def _merge_boilerplate_chains(combined, graphs, app_ids):
    """
    Merge clock initialization boilerplate across STREAMify apps.
    
    When all apps have the identical boilerplate chain:
        READ_TTCLOCK → MULT → ADD → VALUES_TO_TTTIME
    this replaces per-app duplicates with a single shared chain.
    
    Per-app CONSTs (different parameter values) and COPY_TTTIME
    (different fanout targets) remain separate.
    
    Only applies when ALL apps have the full boilerplate chain.
    Skips silently otherwise (e.g. when mixing STREAMify + SQify apps).
    """
    MERGEABLE = {'READ_TTCLOCK', 'MULT', 'ADD', 'VALUES_TO_TTTIME'}
    
    # ── Step 1: detect boilerplate in each ORIGINAL graph ──
    app_bp = {}
    for graph, app_id in zip(graphs, app_ids):
        found = {}
        for sq in graph.sqs:
            fn = getattr(sq, 'function_name', None)
            if fn in MERGEABLE:
                found[fn] = sq
        if MERGEABLE.issubset(found.keys()):
            app_bp[app_id] = found
    
    if len(app_bp) != len(app_ids):
        logger.info("Boilerplate merge skipped: not all apps have clock init chain")
        return
    
    logger.info(f"Merging boilerplate clock chains across {len(app_ids)} apps")
    
    # ── Step 2: analyse chain wiring per app (original data_names) ──
    chain = {}
    for app_id, bp in app_bp.items():
        g = graphs[app_ids.index(app_id)]
        mult_ipps = [p.data_name for p in bp['MULT'].get_ipps()]
        
        # CONSTs whose output feeds MULT
        feeding_consts = []
        for sq in g.sqs:
            for opp in sq.get_opps():
                if opp.data_name in mult_ipps:
                    feeding_consts.append({
                        'sq_name': sq.sq_name,
                        'opp_dn': opp.data_name,
                        'port_idx': mult_ipps.index(opp.data_name),
                    })
        
        # Key internal data_names
        mult_out = bp['MULT'].get_opps()[0].data_name
        rtc_out  = bp['READ_TTCLOCK'].get_opps()[0].data_name
        add_out  = bp['ADD'].get_opps()[0].data_name
        vtt_out  = bp['VALUES_TO_TTTIME'].get_opps()[0].data_name
        
        # Find COPY_TTTIME that consumes VTT output
        copy_tt = None
        for sq in g.sqs:
            if getattr(sq, 'function_name', None) == 'COPY_TTTIME':
                for ipp in sq.get_ipps():
                    if ipp.data_name == vtt_out:
                        copy_tt = sq.sq_name
                        break
        
        chain[app_id] = {
            'feeding_consts': feeding_consts,
            'mult_ipps': mult_ipps,
            'mult_out': mult_out,
            'rtc_out': rtc_out,
            'add_out': add_out,
            'vtt_out': vtt_out,
            'copy_tt': copy_tt,
            'bp': bp,
        }
    
    # Shared edge names
    SN = {
        'mult_in0':      'shared__bp_mult_in0',
        'mult_in1':      'shared__bp_mult_in1',
        'mult_out':      'shared__bp_mult_out',
        'start_time':    'shared__bp_start_time',
        'stop_time':     'shared__bp_stop_time',
        'sampling_time': 'shared__bp_sampling_time',
    }
    
    # ── Step 3: create shared SQs (deepcopy first app as template) ──
    tmpl = chain[app_ids[0]]
    
    # READ_TTCLOCK  (ipp stays 'trigger', opp → shared)
    shared_rtc = deepcopy(tmpl['bp']['READ_TTCLOCK'])
    shared_rtc.sq_name = 'shared__READ_TTCLOCK'
    for opp in shared_rtc.get_opps():
        if opp.data_name == tmpl['rtc_out']:
            opp.data_name = SN['start_time']
    
    # MULT  (ipps → shared, opp → shared)
    shared_mult = deepcopy(tmpl['bp']['MULT'])
    shared_mult.sq_name = 'shared__MULT'
    for i, ipp in enumerate(shared_mult.get_ipps()):
        if i < 2:
            ipp.data_name = SN[f'mult_in{i}']
    for opp in shared_mult.get_opps():
        opp.data_name = SN['mult_out']
    
    # ADD  (ipps: match by original data_name → shared)
    shared_add = deepcopy(tmpl['bp']['ADD'])
    shared_add.sq_name = 'shared__ADD'
    for ipp in shared_add.get_ipps():
        if ipp.data_name == tmpl['mult_out']:
            ipp.data_name = SN['mult_out']
        elif ipp.data_name == tmpl['rtc_out']:
            ipp.data_name = SN['start_time']
    for opp in shared_add.get_opps():
        opp.data_name = SN['stop_time']
    
    # VALUES_TO_TTTIME  (ipps: start_time + stop_time → shared)
    shared_vtt = deepcopy(tmpl['bp']['VALUES_TO_TTTIME'])
    shared_vtt.sq_name = 'shared__VALUES_TO_TTTIME'
    for ipp in shared_vtt.get_ipps():
        if ipp.data_name == tmpl['add_out']:
            ipp.data_name = SN['stop_time']
        elif ipp.data_name == tmpl['rtc_out']:
            ipp.data_name = SN['start_time']
    for opp in shared_vtt.get_opps():
        opp.data_name = SN['sampling_time']
    
    shared_sqs = [shared_rtc, shared_mult, shared_add, shared_vtt]
    
    # ── Step 4: names of per-app boilerplate SQs to remove ──
    to_remove = set()
    for app_id, c in chain.items():
        for fn in MERGEABLE:
            to_remove.add(f"{app_id}__{c['bp'][fn].sq_name}")
    
    # ── Step 5: rewire per-app SQs in combined graph ──
    for app_id, c in chain.items():
        # 5a  CONSTs that fed MULT → point to shared MULT ports
        for ci in c['feeding_consts']:
            target_name = f"{app_id}__{ci['sq_name']}"
            target_opp  = f"{app_id}__{ci['opp_dn']}"
            shared_port = SN[f"mult_in{ci['port_idx']}"]
            for sq in combined.sqs:
                if sq.sq_name == target_name:
                    for opp in sq.get_opps():
                        if opp.data_name == target_opp:
                            opp.data_name = shared_port
                    break
        
        # 5b  COPY_TTTIME ipp → receive from shared VTT
        if c['copy_tt']:
            ct_name = f"{app_id}__{c['copy_tt']}"
            old_dn  = f"{app_id}__{c['vtt_out']}"
            for sq in combined.sqs:
                if sq.sq_name == ct_name:
                    for ipp in sq.get_ipps():
                        if ipp.data_name == old_dn:
                            ipp.data_name = SN['sampling_time']
                    break
    
    # ── Step 6: swap SQs ──
    combined.sqs = [sq for sq in combined.sqs if sq.sq_name not in to_remove]
    combined.sqs.extend(shared_sqs)
    combined.shared_boilerplate = shared_sqs
    
    # ── Step 7: fix SSPG edges ──
    combined.sspg_edges = [
        e for e in combined.sspg_edges if e['target'] not in to_remove]
    
    # SUPER_TRIGGER → shared READ_TTCLOCK
    edge_sym = 'su__shared__READ_TTCLOCK'
    combined.super_trigger.add_output_port(edge_sym)
    combined.sspg_edges.append({
        'type': SSPGEdgeType.SU,
        'source': combined.super_trigger.sq_name,
        'target': shared_rtc.sq_name,
        'symbol': edge_sym,
        'app_id': None,
    })
    
    # Clean stale SUPER_TRIGGER output ports
    stale = set()
    for app_id, c in chain.items():
        rtc_pname = f"{app_id}__{c['bp']['READ_TTCLOCK'].sq_name}"
        for port in combined.super_trigger.get_opps():
            if rtc_pname in port.data_name:
                stale.add(port.data_name)
    combined.super_trigger._opps = [
        p for p in combined.super_trigger._opps if p.data_name not in stale]
    
    # ── Step 8: update subgraph_info ──
    for app_id in app_ids:
        info = combined.subgraph_info[app_id]
        rtc_pname = f"{app_id}__{chain[app_id]['bp']['READ_TTCLOCK'].sq_name}"
        info['sources'] = [s for s in info['sources'] if s.sq_name != rtc_pname]
        info['sq_count'] -= len(MERGEABLE)
    
    saved = len(to_remove) - len(shared_sqs)
    logger.info(
        f"Boilerplate merged: {len(to_remove)} per-app SQs → "
        f"{len(shared_sqs)} shared ({saved} SQs saved)")

# ============================================================================
# Main Combination Logic
# ============================================================================

def combine(*graphs, app_ids: Optional[List[str]] = None,
            combined_name: Optional[str] = None) -> CombinedGraph:
    """
    Combine multiple TTGraph objects using SSPG unordered parallel composition.
    
    :param graphs: Variable number of TTGraph objects from TTCompile
    :param app_ids: Optional application identifiers (auto-generated if None)
    :param combined_name: Optional name for combined graph
    :return: CombinedGraph containing all applications
    """
    graphs = list(graphs)
    n = len(graphs)
    
    if n < 2:
        raise ValueError(f"At least 2 graphs required for composition, got {n}")
    
    if app_ids is None:
        app_ids = [f"app{i}" for i in range(n)]
    elif len(app_ids) != n:
        raise ValueError(f"Number of app_ids ({len(app_ids)}) must match graphs ({n})")
       
    if combined_name is None:
        combined_name = f"combined_{'_'.join(app_ids)}"
    
    logger.info(f"Combining {n} graphs via SSPG unordered parallel composition")
    
    combined = CombinedGraph(combined_name, app_ids)
    
    # Step 1: Analyze all source graphs
    analyses = {}
    for graph, app_id in zip(graphs, app_ids):
        analyses[app_id] = _analyze_graph(graph, app_id)
    
    # Step 2: Create SUPER_TRIGGER
    combined.super_trigger = SyntheticSQ(
        sq_name='SUPER_TRIGGER',
        function_name='SSPG_UNORDERED_SPLIT',
        synthetic_type='trigger',
        connected_apps=app_ids.copy()
    )
    combined.super_trigger.add_input_port('unified_trigger')
    combined.sqs.append(combined.super_trigger)
    combined.source_vars['unified_trigger'] = None
    
    # Step 3: Prefix and collect all SQs
    for app_id, analysis in analyses.items():
        prefixed_sources = []
        prefixed_sinks = []
        
        for sq in analysis['all_sqs']:
            prefixed_sq = deepcopy(sq)
            original_name = sq.sq_name
            prefixed_name = f"{app_id}__{original_name}"
            prefixed_sq.sq_name = prefixed_name
            
            for ipp in prefixed_sq.get_ipps():
                if hasattr(ipp, 'data_name'):
                    if ipp.data_name not in ['trigger', 'unified_trigger']:
                        ipp.data_name = f"{app_id}__{ipp.data_name}"
            
            for opp in prefixed_sq.get_opps():
                if hasattr(opp, 'data_name'):
                    opp.data_name = f"{app_id}__{opp.data_name}"
            
            combined.sqs.append(prefixed_sq)
            
            if sq in analysis['sources']:
                prefixed_sources.append(prefixed_sq)
            if sq in analysis['sinks']:
                prefixed_sinks.append(prefixed_sq)
        
        # Store subgraph info (CRITICAL for multitenancy)
        combined.subgraph_info[app_id] = {
            'sources': prefixed_sources,
            'sinks': prefixed_sinks,
            'sq_count': len(analysis['all_sqs']),
        }
        
        # Add split edges
        for src_sq in prefixed_sources:
            original_name = src_sq.sq_name[len(app_id) + 2:]
            edge_name = f"su__{app_id}__{original_name}"
            combined.super_trigger.add_output_port(edge_name)
            combined.sspg_edges.append({
                'type': SSPGEdgeType.SU,
                'source': combined.super_trigger.sq_name,
                'target': src_sq.sq_name,
                'symbol': edge_name,
                'app_id': app_id,
            })
    
    # Step 4: Create BARRIER_JOIN
    combined.barrier_join = SyntheticSQ(
        sq_name='BARRIER_JOIN',
        function_name='SSPG_WAIT_ALL',
        synthetic_type='join',
        connected_apps=app_ids.copy()
    )
    
    for app_id, info in combined.subgraph_info.items():
        for sink_sq in info['sinks']:
            original_name = sink_sq.sq_name[len(app_id) + 2:]
            edge_name = f"ju__{app_id}__{original_name}"
            combined.barrier_join.add_input_port(edge_name)
            combined.sspg_edges.append({
                'type': SSPGEdgeType.JU,
                'source': sink_sq.sq_name,
                'target': combined.barrier_join.sq_name,
                'symbol': edge_name,
                'app_id': app_id,
            })
    
    combined.barrier_join.add_output_port('barrier_complete')
    combined.sqs.append(combined.barrier_join)

    
    # Step 7: Merge clocks
    for graph, app_id in zip(graphs, app_ids):
        for clock_name, clock_spec in graph.clock_dictionary.items():
            prefixed_clock = f"{app_id}__{clock_name}"
            combined.clock_dictionary[prefixed_clock] = clock_spec
    
    # Step 8: Merge constraints
    for graph, app_id in zip(graphs, app_ids):
        for constraint in graph.flattened_constraints:
            prefixed_constraint = deepcopy(constraint)
            if hasattr(prefixed_constraint, 'name'):
                prefixed_constraint.name = f"{app_id}__{prefixed_constraint.name}"
            combined.flattened_constraints.append(prefixed_constraint)
    
    # Build app_configs for easy access
    for app_id in app_ids:
        combined.app_configs[app_id] = combined.get_app_config(app_id)
    
    logger.info(f"Combined graph '{combined_name}': {len(combined.sqs)} SQs")
    
    # Step 9: Merge clock init boilerplate (STREAMify apps only)
    _merge_boilerplate_chains(combined, graphs, app_ids)
    
    return combined


def print_summary(combined: CombinedGraph):
    """Print summary of combined graph."""
    print("\n" + "=" * 70)
    print("SSPG Combined Graph Summary")
    print("=" * 70)
    print(f"Name: {combined.graph_name}")
    print(f"Applications: {', '.join(combined.app_ids)}")
    print(f"Total SQs: {len(combined.sqs)}")
    print()
    
    print("Per-Application Breakdown:")
    for app_id, info in combined.subgraph_info.items():
        print(f"  {app_id}:")
        print(f"    SQs: {info['sq_count']}")
        print(f"    Hyperperiod: {info.get('hyperperiod', 'N/A')}ms")
        print(f"    Sources: {[sq.sq_name for sq in info['sources']]}")
        print(f"    Sinks: {[sq.sq_name for sq in info['sinks']]}")
    print("=" * 70)