# Copyright 2024 The Authors
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
TTPython Compiler CLI Entry Point

Supports single-file and multi-file compilation.
Multi-file compilation triggers SSPG graph combination.
"""

import argparse
import os
from typing import List


def main():
    parser = argparse.ArgumentParser(
        description='Compile TTPython program(s) to DFG. Supports single or multiple files.')

    parser.add_argument('files', metavar='F', type=str, nargs='+',
                        help='file(s) to compile (2+ files triggers SSPG combination)')
    parser.add_argument(
        '-o', '--out', nargs='?', default='./output',
        help='output directory for compiled program(s)')
    parser.add_argument(
        '--ast', action='store_true',
        help='print the compiled program AST')
    parser.add_argument(
        '-g', '--graph', action='store_true',
        help='show the compiled program dataflow graph')
    parser.add_argument(
        '--print_graph', action='store_true',
        help='print a textual representation of the graph')
    parser.add_argument(
        '-d', '--debug', action='store_true',
        help='show debug information')
    parser.add_argument(
        '--app-ids', type=str, nargs='*', default=None,
        help='application identifiers for multi-file compilation (must match file count)')
    parser.add_argument(
        '--deployment', type=str, default=None,
        help='deployment YAML file specifying the target device cluster. '
             'One file for all apps (shared cluster for multitenancy).')

    args = parser.parse_args()

    import ticktalkpython.DebugLogger as log
    log.set_base_logger_info()
    if args.debug:
        log.set_base_logger_debug()

    out_path = args.out
    if not out_path.endswith('/'):
        out_path = out_path + '/'
    os.makedirs(out_path, exist_ok=True)

    # Validate all files are Python
    for file_path in args.files:
        if not file_path.endswith('.py'):
            print(f"Error: {file_path} is not a Python file")
            return 1

    # Single file: standard compilation
    if len(args.files) == 1:
        return _compile_single(args.files[0], out_path, args)

    # Multiple files: compile each, then combine via SSPG
    return _compile_multiple(args.files, out_path, args)


def _compile_single(file_path: str, out_path: str, args) -> int:
    """Compile a single TTPython file."""
    from ticktalkpython.Compiler import TTCompile, dump_pickle, draw_graph, print_text_graph

    file_name = os.path.basename(file_path)[:-3]
    pickle_path = f"{out_path}{file_name}.pickle"
    graph_file_path = f"{out_path}{file_name}.png"

    deployment_path = args.deployment
    if deployment_path:
        print(f"Compiling '{file_name}' with deployment '{os.path.basename(deployment_path)}'")
    else:
        print(f"Compiling '{file_name}'")
    graph = TTCompile(file_path, os.path.dirname(file_path), deployment_path=deployment_path)

    dump_pickle(graph, pickle_path)
    print(f"Compiled output: {pickle_path}")

    if args.print_graph:
        print_text_graph(graph)
    if args.graph:
        draw_graph(graph, graph_file_path)
        print(f"Graph image: {graph_file_path}")

    return 0


def _compile_multiple(file_paths: List[str], out_path: str, args) -> int:
    """
    Compile multiple TTPython files and combine via SSPG.
    
    Pipeline: compile each -> combine -> save combined graph
    """
    from ticktalkpython.Compiler import TTCompile, dump_pickle, draw_graph
    from ticktalkpython.Combiner import combine, print_summary

    graphs = []
    file_names = []

    print(f"\n{'='*60}")
    print(f"Multi-App Compilation: {len(file_paths)} files")
    print(f"{'='*60}\n")

    # Step 1: Compile each file individually against the shared cluster
    deployment_path = args.deployment
    for file_path in file_paths:
        file_name = os.path.basename(file_path)[:-3]
        file_names.append(file_name)

        if deployment_path:
            print(f"[1/3] Compiling '{file_name}' with deployment '{os.path.basename(deployment_path)}'...")
        else:
            print(f"[1/3] Compiling '{file_name}'...")
        graph = TTCompile(file_path, os.path.dirname(file_path), deployment_path=deployment_path)
        graphs.append(graph)

        # Save individual graph
        individual_pickle = f"{out_path}{file_name}.pickle"
        dump_pickle(graph, individual_pickle)
        print(f"      Individual graph saved: {individual_pickle}")
        
        # Draw individual graph if requested
        if args.graph:
            individual_png = f"{out_path}{file_name}.png"
            draw_graph(graph, individual_png)
            print(f"      Individual graph image: {individual_png}")

    # Step 2: Generate app_ids
    if args.app_ids:
        if len(args.app_ids) != len(graphs):
            print(f"Error: --app-ids count ({len(args.app_ids)}) must match file count ({len(graphs)})")
            return 1
        app_ids = args.app_ids
    else:
        # Auto-generate from file names
        app_ids = [name.replace('eval_', '').replace('_', '') for name in file_names]

    # Step 3: Combine via SSPG
    print(f"\n[2/3] Combining {len(graphs)} graphs via SSPG unordered parallel composition...")
    print(f"      App IDs: {app_ids}")

    combined = combine(*graphs, app_ids=app_ids)

    if graphs[0].deployment_spec:
        combined.deployment_spec = graphs[0].deployment_spec

    # Step 4: Save combined graph
    combined_pickle = f"{out_path}combined_graph.pickle"
    dump_pickle(combined, combined_pickle)
    print(f"\n[3/3] Combined graph saved: {combined_pickle}")

    # Print summary
    print_summary(combined)

    # Optional visualization
    if args.graph:
        combined_png = f"{out_path}combined_graph.png"
        try:
            _visualize_combined(combined, combined_png)
            print(f"Combined graph image: {combined_png}")
        except Exception as e:
            print(f"Visualization skipped: {e}")

    print(f"\n{'='*60}")
    print(f"Multi-App Compilation Complete")
    print(f"{'='*60}")
    print(f"Combined graph: {combined_pickle}")
    print(f"Ready for: SmartMapper -> RuntimeManager deployment")
    print(f"{'='*60}\n")

    return 0


def _visualize_combined(combined, output_path: str):
    """
    Generate visualization of combined graph.
    Shows SSPG structure with internal dataflow preserved.
    """
    try:
        import graphviz
        
        dot = graphviz.Digraph(comment=combined.graph_name)
        dot.attr(rankdir='TB', splines='ortho', nodesep='0.5', ranksep='0.8')
        dot.attr('node', shape='ellipse', style='filled')
        
        # Generate colors dynamically for any number of apps
        base_colors = ['#90EE90', '#FFD700', '#87CEEB', '#DDA0DD', '#F0E68C', 
                       '#98FB98', '#FFA07A', '#B0C4DE', '#FFB6C1', '#20B2AA']
        app_colors = {}
        for i, app_id in enumerate(combined.app_ids):
            app_colors[app_id] = base_colors[i % len(base_colors)]
        
        # Create subgraphs: first app, shared clock, remaining apps
        # Graphviz lays out clusters left-to-right in creation order,
        # so inserting the shared clock chain after the first app
        # centres it between the application subgraphs.
        has_shared = hasattr(combined, 'shared_boilerplate') and combined.shared_boilerplate
        
        for idx, app_id in enumerate(combined.app_ids):
            with dot.subgraph(name=f'cluster_{app_id}') as sub:
                sub.attr(label=app_id, style='rounded', bgcolor='#F5F5F5')
                
                app_sqs = combined.get_sqs_for_app(app_id)
                for sq in app_sqs:
                    sub.node(sq.sq_name, 
                            label=sq.sq_name.replace(f'{app_id}__', ''),
                            fillcolor=app_colors.get(app_id, '#CCCCCC'))
            
            # Insert shared clock chain after the first app cluster
            if idx == 0 and has_shared:
                with dot.subgraph(name='cluster_shared') as sub:
                    sub.attr(label='Shared Clock Chain', style='rounded,dashed',
                             bgcolor='#E8E8E8', color='#666666')
                    for sq in combined.shared_boilerplate:
                        display = sq.sq_name.replace('shared__', '')
                        sub.node(sq.sq_name, label=display,
                                fillcolor='#FFFFFF', style='filled')
        
        # Force horizontal layout: app1 — shared clock — app2
        # Invisible edges constrain Graphviz to place shared cluster between apps
        if has_shared and len(combined.app_ids) >= 2:
            first_app_sqs = combined.get_sqs_for_app(combined.app_ids[0])
            second_app_sqs = combined.get_sqs_for_app(combined.app_ids[1])
            shared_anchor = combined.shared_boilerplate[0].sq_name
            if first_app_sqs:
                dot.edge(first_app_sqs[0].sq_name, shared_anchor,
                         style='invis', constraint='true')
            if second_app_sqs:
                dot.edge(shared_anchor, second_app_sqs[0].sq_name,
                         style='invis', constraint='true')
        
        # Add synthetic nodes (outside clusters)
        dot.node('SUPER_TRIGGER', 'SUPER_TRIGGER', 
                shape='diamond', fillcolor='#FF6B6B', fontcolor='white')
        dot.node('BARRIER_JOIN', 'BARRIER_JOIN',
                shape='diamond', fillcolor='#FF6B6B', fontcolor='white')

        
        # Add internal dataflow edges (within each app)
        ipp_to_sq = combined.get_ipp_to_sq_dict()
        for sq in combined.sqs:
            if sq.sq_name in ['SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT']:
                continue
            for opp in sq.get_opps():
                symbol = opp.data_name
                if symbol in ipp_to_sq:
                    for dest_sq, _ in ipp_to_sq[symbol]:
                        if dest_sq.sq_name not in ['SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT']:
                            dot.edge(sq.sq_name, dest_sq.sq_name, 
                                    label='', color='#333333')
        
        # Add SSPG composition edges
        for app_id, info in combined.subgraph_info.items():
            # Split edges: SUPER_TRIGGER -> sources
            for src_sq in info['sources']:
                dot.edge('SUPER_TRIGGER', src_sq.sq_name, 
                        color='#FF0000', style='dashed', penwidth='2')
            
            # Join edges: sinks -> BARRIER_JOIN
            for sink_sq in info['sinks']:
                dot.edge(sink_sq.sq_name, 'BARRIER_JOIN',
                        color='#008000', style='dashed', penwidth='2')
        
        # Shared boilerplate source edge
        if hasattr(combined, 'shared_boilerplate') and combined.shared_boilerplate:
            for sq in combined.shared_boilerplate:
                if getattr(sq, 'function_name', '') == 'READ_TTCLOCK':
                    dot.edge('SUPER_TRIGGER', sq.sq_name,
                            color='#FF0000', style='dashed', penwidth='2')
        
        

        # Render
        output_base = output_path.rsplit('.', 1)[0]
        dot.render(output_base, format='png', cleanup=True)
        
    except ImportError:
        # Fallback to networkx/matplotlib
        _visualize_combined_fallback(combined, output_path)

def _visualize_combined_fallback(combined, output_path: str):
    """Fallback visualization using networkx if graphviz not available."""
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
        
        G = nx.DiGraph()
        
        # Generate colors dynamically
        base_colors = ['#90EE90', '#FFD700', '#87CEEB', '#DDA0DD', '#F0E68C']
        app_colors = {}
        for i, app_id in enumerate(combined.app_ids):
            app_colors[app_id] = base_colors[i % len(base_colors)]
        
        # Add all nodes with colors
        node_colors = []
        for sq in combined.sqs:
            G.add_node(sq.sq_name)
            if sq.sq_name in ['SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT']:
                node_colors.append('#FF6B6B')
            else:
                app_id = combined.get_app_for_sq(sq.sq_name)
                node_colors.append(app_colors.get(app_id, '#CCCCCC'))
        
        # Add internal dataflow edges
        ipp_to_sq = combined.get_ipp_to_sq_dict()
        for sq in combined.sqs:
            if sq.sq_name in ['SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT']:
                continue
            for opp in sq.get_opps():
                symbol = opp.data_name
                if symbol in ipp_to_sq:
                    for dest_sq, _ in ipp_to_sq[symbol]:
                        if dest_sq.sq_name not in ['SUPER_TRIGGER', 'BARRIER_JOIN', 'SUPER_RESULT']:
                            G.add_edge(sq.sq_name, dest_sq.sq_name)
        
        # Add SSPG edges
        for app_id, info in combined.subgraph_info.items():
            for src_sq in info['sources']:
                G.add_edge('SUPER_TRIGGER', src_sq.sq_name)
            for sink_sq in info['sinks']:
                G.add_edge(sink_sq.sq_name, 'BARRIER_JOIN')
        
        G.add_edge('BARRIER_JOIN', 'SUPER_RESULT')
        
        plt.figure(figsize=(16, 12))
        try:
            pos = nx.nx_agraph.graphviz_layout(G, prog='dot')
        except:
            pos = nx.spring_layout(G, k=2, iterations=50)
        
        nx.draw(G, pos, with_labels=True, node_color=node_colors,
                node_size=1500, font_size=7, arrows=True)
        plt.title(f"Combined Graph: {combined.graph_name}")
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
    except Exception as e:
        raise RuntimeError(f"Visualization failed: {e}")


if __name__ == "__main__":
    exit(main())