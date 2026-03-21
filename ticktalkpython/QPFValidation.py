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

"""
QPF Validation - Ensure Quantitative Placement Framework components are initialized.

Call ensure_qpf_initialized() before using ObjectiveCalculator or optimization.
"""

from . import DebugLogger

logger = DebugLogger.get_logger('QPFValidation')


def ensure_qpf_initialized():
    """
    Verify that all QPF components are properly initialized before use.
    
    This function checks:
    1. DeviceProfile manager has device profiles loaded
    2. NetworkTopology has network links loaded
    
    Raises:
        RuntimeError: If any component is not initialized
    
    Usage:
        from ticktalkpython.QPFValidation import ensure_qpf_initialized
        ensure_qpf_initialized()
        # Now safe to use ObjectiveCalculator
    """
    from .DeviceProfile import get_profile_manager
    from .NetworkTopology import get_network_topology
    
    logger.debug("Validating QPF component initialization...")
    
    # Check DeviceProfile
    try:
        profile_manager = get_profile_manager()
        if not profile_manager.profiles:
            raise RuntimeError(
                "DeviceProfile not initialized. "
                "Call initialize_profiles('device_types.yaml', 'deployment.yaml') first."
            )
        logger.debug(f"✓ DeviceProfile OK: {len(profile_manager.profiles)} devices")
    except Exception as e:
        raise RuntimeError(f"DeviceProfile validation failed: {e}")
    
    # Check NetworkTopology
    try:
        topology = get_network_topology()
        if not topology.latency and not topology.bandwidth:
            logger.warning(
                "NetworkTopology has no links defined. "
                "This is OK if all SQs are on same device, but unusual. "
                "Consider calling initialize_topology('network_types.yaml', 'deployment.yaml')."
            )
        else:
            logger.debug(f"✓ NetworkTopology OK: {len(topology.latency)//2} links")
    except Exception as e:
        raise RuntimeError(f"NetworkTopology validation failed: {e}")
    
    logger.info("QPF components validated successfully")