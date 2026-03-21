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
Inter-Process Communication (IPC) is an essential abstraction on ``TTEnsembles``
for simplifying runtime iteractions in accordance with the three-part
formulation of SQs. The ensemble has 3 main processes at runtime: an input token
processor  ``TTInputTokenProcess`` (handles synchronization for SQs), an
executor ``TTExecuteProces`` (executes the code within an SQ), and network
interface/forwarder ``TTNetworkManager`` (which tags duplicates tokens
and sends them to the ensemble that downstream SQs are mapped to and forwards
anything that arrives over the network to the appropriate process).

Each of these has data and control layers, where the data layer is responsible
for interpreting the graph that represents the users program; the
control/management layer, handles higher level operations for the SQs, such as
instantiating or destroying them, or remapping them. Our IPC abstraction helps
to distinguish those types of messages. This is very similar to
``TTNetworkMessage``. Note that control messages should seek guaranteed
delivery over the network.

Primarily, this file contains enumerations. These enumerations are designators
that help self-identify messages with their recipient and function. It is up to
the actual processes to read these from the message and respond appropriately.
'''

from enum import Enum, auto
import time
import json

class SyncMsg(Enum):
    '''
    Message types used by the process that handles synchronization for SQs on
    input tokens, i.e., the ``TTInputTokenProcess``
    '''
    # Data layer
    InputToken = 0
    TimedInput = 1

    # Control layer
    InstantiateSQ = 100
    RemoveSQ = 101
    UpdateFiringRule = 102
    AddClocks = 110
    RemoveClocks = 111
    UpdateClocks = 112

    # System layer
    EndExecution = 200

    def is_data(self):
        return self.value < 100

    def is_control(self):
        return 100 <= self.value and self.value < 200


class ExecuteMsg(Enum):
    '''
    Message types used by the process that handles execution for SQs on tokens,
    i.e. the ``TTExecuteProcess``
    '''
    # Data layer
    NewExecutionContext = 0
    StatefulExecutionContext = 1

    # Control layer
    InstantiateSQ = 100
    RemoveSQ = 101
    UpdateCode = 102
    AddClocks = 110
    RemoveClocks = 111
    UpdateClocks = 112

    # System layer
    EndExecution = 200

    def is_data(self):
        return self.value < 100

    def is_control(self):
        return 100 <= self.value and self.value < 200


class NetMsg(Enum):
    '''
    Message types used by the process that handles the network layer and
    forwarding tokens based on SQ mapping, i.e., the ``TTNetworkManager``
    '''
    # Data layer
    SendTokenList = 0
    # allows conditional outputs
    SendToken = 1
    EmptyToken = 2

    # Control layer
    InstantiateSQ = 100
    RemoveSQ = 101
    UpdateMapping = 102
    AddRoutingTableEntry = 110
    RemoveRoutingTableEntry = 111
    UpdateRoutingTableEntry = 112
    PropagateRoutingTable = 113
    ForwardNetworkMessage = 120

    # System layer
    EndExecution = 200

    def is_data(self):
        return self.value < 100

    def is_control(self):
        return 100 <= self.value and self.value < 200


class RuntimeMsg(Enum):
    '''
    Message types used by the ``TTRuntimeManagerProcess``, which is generally
    used for setting up the system and graph, but plays little role in the
    interpretation of the graph (aside from creating initial input tokens)
    '''
    
    # ========== DATA LAYER (1-99) ==========
    LogOutputToken = 1

    # ========== GRAPH AND APPLICATION MANAGEMENT (100-109) ==========
    InstantiateAndMapGraph = 100
    InstantiateAndMapMultipleGraphs = 101
    ExecuteGraphOnInputs = 102
    JoinTickTalkSystem = 103
    DeployApp = 104
    TerminateApp = 105
    InstantiateAndMapCombinedGraph = auto()  # Deploy CombinedGraph with multitenancy

    # ========== DEVICE HEALTH AND MONITORING (110-119) ==========
    DeviceHeartbeat = 110
    HandleDeviceFailure = 111
    DeviceHealthQuery = 112
    DeviceHealthResponse = 113
    DeviceStatusUpdate = 114

    # ========== DEVICE INTEGRATION AND LIFECYCLE (120-129) ==========
    DeviceAnnounce = 120               # New device announces itself
    DeviceWelcome = 121                # RuntimeManager welcomes new device
    DeviceRejoining = 122              # Device announces it's rejoining
    DeviceStabilityConfirmed = 123     # Device confirmed stable after rejoining
    CapabilityQuery = 124              # Query device capabilities
    CapabilityResponse = 125           # Device responds with capabilities

    # ========== TASK MIGRATION AND OPTIMIZATION (130-139) ==========
    MigrationAssessmentRequest = 130   # Request migration impact assessment
    MigrationAssessmentResponse = 131  # Response with migration recommendations
    TaskMigrationRequest = 132         # Request task migration
    TaskMigrationConfirm = 133         # Confirm migration receipt
    TaskMigrationComplete = 134        # Migration completed successfully

    # ========== DEADLINE AND TIMING METRICS (140-149) ==========
    DeadlineResult = 140               # Report deadline check outcome
    DeadlineMetricsQuery = 141         # Request deadline statistics
    DeadlineMetricsResponse = 142      # Return deadline statistics
    DeadlinePathAnalysis = 143         # Request/report deadline path analysis

    # ========== SYSTEM CONTROL (200+) ==========
    EndExecution = 200

    def is_data(self):
        return self.value < 100

    def is_control(self):
        return 100 <= self.value and self.value < 200

    def is_system(self):
        return self.value >= 200
# ========== RUNTIME ADAPTATION MESSAGE FORMATS ==========
# These work with the existing RuntimeMsg enum

class HeartbeatMessage:
    """Precise heartbeat format using existing RuntimeMsg.DeviceHeartbeat."""
    
    def __init__(self, device_unique_id, device_name, system_metrics=None):
        self.msg_type = RuntimeMsg.DeviceHeartbeat  # Use your existing enum
        self.timestamp = time.time()
        self.device_unique_id = device_unique_id
        self.device_name = device_name
        self.sequence_id = int(self.timestamp * 1000) % 1000000
        
        self.system_metrics = system_metrics or {
            'cpu_usage': 0.0,
            'memory_usage': 0.0,
            'active_tasks': 0,
            'network_latency': 0.0
        }
        
        self.message_id = f"HB_{device_unique_id[-8:]}_{self.sequence_id}"
    
    def to_bytes(self):
        payload = {
            'msg_type': self.msg_type.value,  # Convert enum to int
            'timestamp': self.timestamp,
            'device_unique_id': self.device_unique_id,
            'device_name': self.device_name,
            'sequence_id': self.sequence_id,
            'system_metrics': self.system_metrics,
            'message_id': self.message_id
        }
        return json.dumps(payload).encode('utf-8')

class DeviceStatusUpdateMessage:
    """State change notification using existing RuntimeMsg.DeviceStatusUpdate."""
    
    def __init__(self, device_unique_id, device_name, old_state, new_state, reason, affected_tasks=None):
        self.msg_type = RuntimeMsg.DeviceStatusUpdate  # Use your existing enum
        self.timestamp = time.time()
        self.device_unique_id = device_unique_id
        self.device_name = device_name
        self.old_state = old_state
        self.new_state = new_state
        self.reason = reason
        self.affected_tasks = affected_tasks or []
        self.message_id = f"SC_{device_unique_id[-8:]}_{int(self.timestamp * 1000) % 1000000}"
    
    def to_bytes(self):
        payload = {
            'msg_type': self.msg_type.value,
            'timestamp': self.timestamp,
            'device_unique_id': self.device_unique_id,
            'device_name': self.device_name,
            'old_state': self.old_state,
            'new_state': self.new_state,
            'reason': self.reason,
            'affected_tasks': self.affected_tasks,
            'message_id': self.message_id
        }
        return json.dumps(payload).encode('utf-8')

class DeviceRejoiningMessage:
    """Rejoining notification using existing RuntimeMsg.DeviceRejoining."""
    
    def __init__(self, device_unique_id, device_name, downtime_duration):
        self.msg_type = RuntimeMsg.DeviceRejoining  # Use your existing enum
        self.timestamp = time.time()
        self.device_unique_id = device_unique_id
        self.device_name = device_name
        self.downtime_duration = downtime_duration  # seconds offline
        self.message_id = f"RJ_{device_unique_id[-8:]}_{int(self.timestamp * 1000) % 1000000}"
    
    def to_bytes(self):
        payload = {
            'msg_type': self.msg_type.value,
            'timestamp': self.timestamp,
            'device_unique_id': self.device_unique_id,
            'device_name': self.device_name,
            'downtime_duration': self.downtime_duration,
            'message_id': self.message_id
        }
        return json.dumps(payload).encode('utf-8')

class Recipient(Enum):
    '''
    A description of the process that is meant to receive this message
    '''
    # perhaps this is an overspecification since each process
    # has its own super-type for incoming messages
    ProcessInputTokens = 1
    ProcessExecute = 2
    ProcessNetwork = 3
    ProcessRuntimeManager = 10


class Message:
    '''
    A wrapper for a message that will be sent between processes on an ensemble.
    This encapsulates the payload, which may be anything (so long as it is
    serializable) with information that describes which process it is for and
    how it should be treated.

    :param msg_type: The message type to describe what the message contains and
        how it should be handled.

    :type msg_type: SyncMsg | ExecuteMsg |
        NetMsg | RuntimeMsg

    :param payload: The values carried by the message

    :type payload: Any

    :param process_recipient: An enumerated value describing which process
        should receive this message; typically used for sanity checks, but also
        to help determine where an IPC message should go when it arrives through
        the network interface

    :type process_recipient: Recipient
    '''
    def __init__(self, msg_type, payload, process_recipient):

        self.payload = payload
        self.msg_type = msg_type
        self.process_recipient = process_recipient

    def __repr__(self):
        return (f'<Message {hex(id(self))} - type={self.msg_type}; '
                f'recipient={self.process_recipient}; payload={self.payload}>')


class NetMsgToken:
    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self.payload = payload

    def __repr__(self):
        return (f'<NetMsgToken msg_type:{repr(self.msg_type)}; '
                f'payload={repr(self.payload)}>')


class SendTokenListMessage(Message):
    def __init__(self, payload, process_recipient, source_sq):
        assert isinstance(payload, list) and 0 < len(payload)
        super().__init__(NetMsg.SendTokenList, payload, process_recipient)
        self.source_sq = source_sq

        for msg_token in payload:
            assert isinstance(msg_token, NetMsgToken)

    def __repr__(self):
        return (
            f'<SendTokenListMessage - type={repr(self.msg_type)}; '
            f'recipient={repr(self.process_recipient)}; '
            f'payload={repr(self.payload)}; source={repr(self.source_sq)}>')


class FinishedException(Exception):
    pass
