# Copyright 2022 The Authors
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

import argparse
import os

from ticktalkpython import DebugLogger
from ticktalkpython import Ensemble
from ticktalkpython.IPC import *
from output_functions import get_applied_output_func


def run_application_client(name, self_ip, self_port, rtm_ip, rtm_port, timeout,
                           log_file_name, device_type=None):

    rtm_address = rtm_ip + ":" + str(rtm_port)

    output_func = None
    if log_file_name is not None:
        output_func = get_applied_output_func('log_msg_to_file',
                                              [log_file_name])

    ens1 = Ensemble.TTEnsemble(log_file_name, name, output_func)
    if device_type:
        ens1.set_device_info(device_type=device_type)
    ens1.setup_queues(is_sim=False)
    ens1.setup_physical_processes(network_ip=self_ip,
                                  rx_network_port=self_port,
                                  tx_network_port=self_port + 1)
    ens1.connect_to_TickTalk_network(rtm_address)

    if timeout <= 0:
        ens1.enter_steady_state()
    else:
        ens1.enter_steady_state(timeout=timeout)


def main():
    parser = argparse.ArgumentParser(
        description='instantiate an ensemble for a TTPython program')

    parser.add_argument('name', help="the name of the ensemble")
    parser.add_argument(
        '--ip',
        default='127.0.0.1',
        help='the ip of the ensemble (default: localhost:127.0.0.1)')
    parser.add_argument('--rtm_ip',
                        default='127.0.0.1',
                        help=("the runtime manager's ip to connect to"
                              " (default: localhost:127.0.0.1)"))
    parser.add_argument(
        'port',
        type=int,
        help='the port of the ensemble. reserves both the port number p and p+1'
    )
    parser.add_argument('--rtm_port',
                        type=int,
                        required=True,
                        help='the port of the runtime manager')
    parser.add_argument(
        '--timeout',
        default=60,
        type=int,
        help='ensemble timeout (default: 60 (sec), 0 for infty)')
    parser.add_argument(
        '-o',
        '--output',
        type=str,
        nargs='?',
        default=None,
        const='./output.log',
        help='file (default: ./output.log) to write unspecified output ports '
        'of the program. If the output is not specified, this will forward '
        'outputs to the runtime manager.')
    parser.add_argument('-d',
                        '--debug',
                        action='store_true',
                        help='flag whether to show debug information')
    parser.add_argument(
        '--device-type',
        type=str,
        default=None,
        help='device type from device_types.yaml (e.g., raspberry_pi_4, jetson_nano). '
             'Reports to RuntimeManager for deployment spec verification.')
    parser.add_argument(
        '--run-label',
        type=str,
        default=None,
        help='Label for this run (e.g. eval_etl_qpf_makespan), matching the '
             'value passed to runrtm.py --run-label. Exported as '
             'TTPYTHON_RUN_LABEL so sink SQs running on this device log it '
             'for runtime_validation.py to match against analytical results.')

    args = parser.parse_args()
    name = args.name
    ip = args.ip
    port = args.port
    rtm_ip = args.rtm_ip
    rtm_port = args.rtm_port
    timeout = args.timeout
    is_debug = args.debug
    log_file_name = args.output
    device_type = args.device_type

    if args.run_label:
        os.environ['TTPYTHON_RUN_LABEL'] = args.run_label

    DebugLogger.set_base_logger_info()
    if is_debug:
        DebugLogger.set_base_logger_debug()

    input('\n\n\nHit enter to start\n\n')
    run_application_client(name, ip, port, rtm_ip, rtm_port, timeout,
                           log_file_name, device_type=device_type)

if __name__ == '__main__':
    main()
