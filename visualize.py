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

import re
from matplotlib import pyplot as plt
import argparse
import datetime

from ticktalkpython.Constants import PRINTED_TIME_OFFSET, TIME_FRACTIONAL



def convert_print_time_to_int(str):
    return datetime.datetime.timestamp(datetime.datetime.fromisoformat(str))


def extract_output_info_from_line(line):
    value = None
    source_sq = None
    source_device = None
    t = None
    if line[1:8] == 'TTToken':
        start_T_index = line.find('T:')
        value_str = line[9:start_T_index - 1]
        value = float(value_str)

        open_paren_indices = [i.start() for i in re.finditer('\\(', line)]
        close_paren_indices = [i.start() for i in re.finditer('\\)', line)]
        timestamps_str = line[open_paren_indices[-1] +
                              1:close_paren_indices[-1]]

        # add bias to times to capture millisecond precision
        start_time = int(
            convert_print_time_to_int(timestamps_str.split(',')[0]))
        stop_time = int(
            convert_print_time_to_int(timestamps_str.split(',')[1]))
        t = (start_time + stop_time) / 2

        start_SQ_index = line.find('from SQ "')
        end_SQ_index = line.find('" on ENS: "')
        source_sq = line[start_SQ_index + len('from SQ "'):end_SQ_index]

        start_device_index = end_SQ_index
        # not very unique..
        end_device_index = line.find('".')
        source_device = line[start_device_index +
                               len('" on ENS: "'):end_device_index]

    return value, source_sq, source_device, t


def process_outputs(name, output_file, output_SQ_names, list_info, normalize):
    phy = 'phy'
    sim = 'simulation'
    header_types = [
        f'start execution of {exec_type} ' for exec_type in (phy, sim)
    ]
    values = [[] for i in range(len(output_SQ_names))]
    times = [[] for i in range(len(output_SQ_names))]
    source_devices = [None for i in range(len(values))]
    all_names = set()
    all_sq = set()

    first_time = -1

    with open(output_file, 'r') as f:
        lines = f.readlines()
        header_lines = []
        if list_info:
            print()
            print("Names:")
        for i, line in enumerate(lines):
            for header_type in header_types:
                if header_type in line:
                    try:
                        cur_name = line[line.index('(') + 1:line.index(')')]
                        if list_info and cur_name not in all_names:
                            all_names.add(cur_name)
                            print(cur_name)
                        if name in (cur_name, ""):
                            header_lines.append(i)
                    except:
                        pass
        try:
            most_recent_set = lines[header_lines[-1]:]
        except:
            print(f"No execution with name {name} found")
            return
        if list_info:
            print()
            print("SQ:")
        for line in most_recent_set:
            val, sq, ens, timestamp = extract_output_info_from_line(line)
            if val != None:
                if list_info and sq not in all_sq:
                    all_sq.add(sq)
                    print(sq)
                try:
                    sq_index = output_SQ_names.index(sq)

                    values[sq_index].append(val)

                    if normalize and first_time != -1:
                        normalized_time = timestamp - first_time
                        times[sq_index].append(normalized_time)
                    elif normalize:
                        first_time = timestamp
                        times[sq_index].append(timestamp - first_time)
                    else:
                        times[sq_index].append(timestamp)

                    # assume this cannot be more than one
                    source_devices[sq_index] = ens
                except:
                    pass

    for i, sq_name in enumerate(output_SQ_names):
        if not source_devices[i] is None:
            plt.plot(times[i], values[i], 'b.')
            plt.title("'" + sq_name + "' output from device '" +
                      source_devices[i] + "'")
            # we're just going to assume numeric
            plt.ylabel('Token Value')
            plt.xlabel('Time')
            plt.show()
        else:
            print(f"'{sq_name}' does not have a corresponding source_device")


def main():
    parser = argparse.ArgumentParser(
        description='visualize the output of an executed TTPython program')

    parser.add_argument('file',
                        metavar='F',
                        type=str,
                        help='the output file to visualize')
    parser.add_argument('sq_names',
                        metavar='N',
                        type=str,
                        nargs='+',
                        help='sq names to extract')
    parser.add_argument(
        '--name',
        default='',
        help='the name of the execution to show (default: last one that ran)')
    parser.add_argument('--list',
                        action='store_true',
                        default=False,
                        help='lists available sq names in given output file')
    parser.add_argument(
        '--normalize',
        action='store_true',
        help='offsets first token time as 0 (assumes specified time is in usec)'
    )

    args = parser.parse_args()
    file = args.file
    sq_names = args.sq_names
    name = args.name
    lst = args.list
    is_normalized = args.normalize

    filename = file.split('/')[-1]

    print(f"Viewing {filename}")
    process_outputs(name, file, sq_names, lst, is_normalized)


if __name__ == "__main__":
    main()
