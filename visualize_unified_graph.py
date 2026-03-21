#!/usr/bin/env python3
"""
Visualize the UnifiedPlacementGraph that QPF sees.

Usage:
    python visualize_unified_graph.py <pickle> <deployment_yaml> [-o output.png]

Filters out infrastructure SQs (CONST, READ_TTCLOCK, etc.) to focus on
application-level placement decisions.
"""

import sys
import os
import pickle
import yaml
import graphviz

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Infrastructure SQ prefixes to filter out (checked after stripping app prefix)
INFRA_PREFIXES = ('CONST', 'READ_TTCLOCK', 'MULT', 'ADD', 'VALUES_TO_TTTIME', 'COPY_TTTIME')
SYNTHETIC_SQS = ('SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT')


def load_graph(pickle_path):
    with open(pickle_path, 'rb') as f:
        return pickle.load(f)


def build_ensemble_infos(deployment_path, device_types_path):
    from ticktalkpython.Query import TTEnsembleInfo

    with open(deployment_path, 'r') as f:
        deployment = yaml.safe_load(f)
    with open(device_types_path, 'r') as f:
        device_types = yaml.safe_load(f)['device_types']

    ensemble_infos = []
    for device in deployment['devices']:
        device_id = device['id']
        device_type = device['type']
        type_spec = device_types.get(device_type, {})
        components = {
            'type': device_type,
            'cpu_cores': 4,
            'compute_slots': 3,
            'memory_mb': int(type_spec.get('memory_size', 4294967296) / (1024*1024)),
            'has_gpu': 'jetson' in device_type or 'nvidia' in device_type,
            **device.get('components', {})
        }
        ensemble_infos.append(TTEnsembleInfo(device_id, None, components))
    return ensemble_infos


def is_infra_sq(sq_name):
    """Return True if this is an infrastructure or synthetic SQ to filter out."""
    if sq_name in SYNTHETIC_SQS:
        return True
    # Strip app prefix (e.g., 'etl__CONST-0' -> 'CONST-0')
    unprefixed = sq_name.split('__', 1)[-1] if '__' in sq_name else sq_name
    return any(unprefixed.startswith(prefix) for prefix in INFRA_PREFIXES)


def draw_unified_graph(unified_graph, output_path, graph_name, app_ids=None):
    dot = graphviz.Digraph('UnifiedPlacementGraph', format='png')
    dot.attr(rankdir='TB', fontsize='11',
             labelloc='t', fontname='Helvetica-Bold', dpi='150')

    # Filter nodes: skip infra SQs
    app_nodes = [n for n in unified_graph.nodes if not is_infra_sq(n.sq.sq_name)]
    app_sq_names = set(n.sq.sq_name for n in app_nodes)

    # Filter edges: only between app SQs
    app_edges = [(s, d, sym) for s, d, sym in unified_graph.edges
                 if not is_infra_sq(s.sq.sq_name) and not is_infra_sq(d.sq.sq_name)]

    devices = sorted(set(n.device for n in app_nodes))

    # Group nodes by SQ
    sq_to_nodes = {}
    for node in app_nodes:
        key = node.sq.sq_name
        if key not in sq_to_nodes:
            sq_to_nodes[key] = []
        sq_to_nodes[key].append(node)

    # Per-app color palettes (each app gets a distinct hue family)
    app_color_families = [
        ['#66BB6A', '#43A047', '#2E7D32'],   # greens
        ['#FFA726', '#FB8C00', '#E65100'],   # oranges
        ['#42A5F5', '#1E88E5', '#1565C0'],   # blues
        ['#AB47BC', '#8E24AA', '#6A1B9A'],   # purples
        ['#EF5350', '#E53935', '#C62828'],   # reds
    ]
    app_fill_map = {}
    if app_ids and len(app_ids) > 1:
        for i, aid in enumerate(app_ids):
            app_fill_map[aid] = app_color_families[i % len(app_color_families)][0]

    def get_node_fill(pn):
        if app_fill_map:
            return app_fill_map.get(pn.app_id, '#78909C')
        return '#66BB6A'

    def node_id(pn):
        return f"{pn.sq.sq_name}@{pn.device}"

    def display_name(sq_name):
        """Strip app prefix for readability."""
        if '__' in sq_name:
            return sq_name.split('__', 1)[1]
        return sq_name

    # Device header row
    with dot.subgraph(name='cluster_devices') as c:
        c.attr(label='Available Devices', style='dashed', color='gray60', fontcolor='gray40')
        for dev in devices:
            c.node(f'dev_{dev}', dev,
                   shape='box3d', style='filled', fillcolor='#B3E5FC',
                   fontname='Helvetica-Bold', fontsize='12', width='1.5')

    # Group SQs by their constraint signature (eligible device set)
    constraint_groups = {}
    for sq_name, nodes in sq_to_nodes.items():
        eligible = tuple(sorted(n.device for n in nodes))
        if eligible not in constraint_groups:
            constraint_groups[eligible] = []
        constraint_groups[eligible].append((sq_name, nodes))

    # Label constraint groups
    group_labels = {}
    for eligible, sq_list in constraint_groups.items():
        if len(eligible) == len(devices):
            group_labels[eligible] = 'components=["compute"] \u2014 all devices eligible'
        elif len(eligible) == 1:
            group_labels[eligible] = f'components=["storage","mqtt_broker"] \u2014 {eligible[0]} only'
        else:
            group_labels[eligible] = f'components=["storage"] \u2014 {", ".join(eligible)}'

    fill_palette = ['#66BB6A', '#FFA726', '#42A5F5', '#EF5350', '#AB47BC',
                    '#26A69A', '#8D6E63', '#78909C']

    group_idx = 0
    for eligible, sq_list in constraint_groups.items():
        label = group_labels.get(eligible, f'Eligible: {", ".join(eligible)}')
        bg = '#F5F5F5' if group_idx % 2 == 0 else '#FAFAFA'
        group_idx += 1

        with dot.subgraph(name=f'cluster_constraint_{group_idx}') as c:
            c.attr(label=label, style='filled',
                   fillcolor=bg, color='gray70',
                   fontname='Helvetica', fontsize='10')

            for sq_name, nodes in sq_list:
                for pn in nodes:
                    nid = node_id(pn)
                    exec_ms = pn.execution_time * 1000 if (pn.execution_time and pn.execution_time < 1) else pn.execution_time
                    time_str = f"\n({exec_ms:.1f}ms)" if exec_ms else ""
                    label_text = f"{display_name(sq_name)}\n\u2192 {pn.device}{time_str}"
                    fill = get_node_fill(pn)

                    c.node(nid, label_text,
                           shape='ellipse', style='filled',
                           fillcolor=fill, fontcolor='white',
                           fontname='Helvetica', fontsize='9')

    # Edges
    edge_count = 0
    for src_node, dst_node, symbol in app_edges:
        src_id = node_id(src_node)
        dst_id = node_id(dst_node)

        if src_node.device == dst_node.device:
            dot.edge(src_id, dst_id,
                     color='#2E7D32', style='solid', penwidth='1.5',
                     arrowsize='0.7')
        else:
            dot.edge(src_id, dst_id,
                     color='#C62828', style='dashed', penwidth='0.8',
                     arrowsize='0.5')
        edge_count += 1

    # Legend
    with dot.subgraph(name='cluster_legend') as c:
        c.attr(label='Legend', style='rounded', color='gray70', fontsize='10')
        c.node('leg_same_a', '', shape='point', width='0.01')
        c.node('leg_same_b', 'Same device (0ms comm)', shape='plaintext', fontsize='9')
        c.edge('leg_same_a', 'leg_same_b', color='#2E7D32', style='solid', penwidth='1.5', arrowsize='0.7')
        c.node('leg_cross_a', '', shape='point', width='0.01')
        c.node('leg_cross_b', 'Cross device (network cost)', shape='plaintext', fontsize='9')
        c.edge('leg_cross_a', 'leg_cross_b', color='#C62828', style='dashed', penwidth='0.8', arrowsize='0.5')
        # App color legend for multi-app
        if app_fill_map:
            for aid, color in app_fill_map.items():
                c.node(f'leg_app_{aid}', f'  {aid}  ', shape='box', style='filled',
                       fillcolor=color, fontcolor='white', fontsize='9',
                       width='0.6', height='0.25')

    # Stats
    total_mappings = 1
    for sq_name, nodes in sq_to_nodes.items():
        total_mappings *= len(nodes)

    app_label = f" ({', '.join(app_ids)})" if app_ids and len(app_ids) > 1 else ""
    stats = (f"App SQs: {len(sq_to_nodes)}{app_label} | "
             f"Placement nodes: {len(app_nodes)} | "
             f"Edges: {edge_count} | "
             f"Placement space: {total_mappings:,} mappings")
    dot.attr(label=f'UnifiedPlacementGraph \u2014 {graph_name}\n{stats}')

    dot.render(output_path.replace('.png', ''), cleanup=True)
    print(f"\nVisualization saved: {output_path}")
    print(f"  {len(sq_to_nodes)} application SQs (infra SQs filtered out)")
    print(f"  {len(app_nodes)} placement nodes")
    print(f"  {edge_count} dependency edges")
    print(f"  {len(devices)} devices: {devices}")
    print(f"  Placement space: {total_mappings:,} possible mappings")


def main():
    if len(sys.argv) < 3:
        print("Usage: python visualize_unified_graph.py <pickle> <deployment_yaml> [-o output.png]")
        sys.exit(1)

    pickle_path = sys.argv[1]
    deployment_path = sys.argv[2]
    output_path = 'unified_graph.png'

    if '-o' in sys.argv:
        idx = sys.argv.index('-o')
        if idx + 1 < len(sys.argv):
            output_path = sys.argv[idx + 1]

    device_types_path = os.path.join(os.path.dirname(deployment_path) or '.', 'device_types.yaml')
    if not os.path.exists(device_types_path):
        device_types_path = 'device_types.yaml'

    print(f"Loading graph: {pickle_path}")
    graph = load_graph(pickle_path)
    graph_name = graph.graph_name

    print(f"Building ensemble infos from: {deployment_path}")
    ensemble_infos = build_ensemble_infos(deployment_path, device_types_path)

    print(f"Building UnifiedPlacementGraph...")
    from ticktalkpython.UnifiedGraph import UnifiedPlacementGraph

    # Detect CombinedGraph vs single-app graph
    is_combined = getattr(graph, 'is_combined_graph', lambda: False)()
    
    if is_combined:
        print(f"  Detected CombinedGraph with apps: {graph.app_ids}")
        unified_apps = {}
        for app_id in graph.app_ids:
            virtual_graph = graph._build_virtual_graph_for_app(app_id)
            unified_apps[app_id] = {'graph': virtual_graph}
        app_ids = graph.app_ids
    else:
        app_id = getattr(graph, 'graph_name', 'app')
        unified_apps = {app_id: {'graph': graph}}
        app_ids = [app_id]

    ensemble_dict = {ens.name: ens for ens in ensemble_infos}
    unified_graph = UnifiedPlacementGraph(unified_apps, ensemble_dict)

    total_nodes = len(unified_graph.nodes)
    infra_nodes = len([n for n in unified_graph.nodes if is_infra_sq(n.sq.sq_name)])
    print(f"  Total nodes: {total_nodes} ({infra_nodes} infra filtered out, {total_nodes - infra_nodes} shown)")

    draw_unified_graph(unified_graph, output_path, graph_name, app_ids=app_ids)


if __name__ == '__main__':
    main()
