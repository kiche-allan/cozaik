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
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from ticktalkpython.SQ import GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


@SQify
def camera_sampler(trigger):
    import sys, time
    sys.path.insert(0, '/content/ticktalkpython/libraries')
    import camera_recognition
    global sq_state
    if sq_state.get('camera', None) == None:
        # Setup our various camera settings
        camera_specifications = camera_recognition.Settings()
        camera_specifications.darknetPath = '/content/darknet/'
        camera_specifications.useCamera = False
        camera_specifications.inputFilename = (
            '/content/yolofiles/cav1/live_test_output.avi')
        camera_specifications.camTimeFile = (
            '/content/yolofiles/cav1/cam_output.txt')
        camera_specifications.cameraHeight = .2
        camera_specifications.cameraAdjustmentAngle = 0.0
        camera_specifications.fps = 60
        camera_specifications.width = 1280
        camera_specifications.height = 720
        camera_specifications.flip = 2
        sq_state['camera'] = camera_recognition.Camera(camera_specifications)

    # Package up 5 frames so that we can parse them
    output_package = []
    for idx in range(5):
        frame_read, camera_timestamp = sq_state['camera'].takeCameraFrame()
        output_package.append([frame_read, camera_timestamp])

    return [output_package, time.time()]


@SQify
def process_camera(cam_sample):
    import sys, time
    sys.path.insert(0, '/content/ticktalkpython/libraries')
    import camera_recognition
    global sq_state

    for each in cam_sample[0]:
        camera_frame = each[0]
        camera_timestamp = each[1]
        if sq_state.get('camera_recognition', None) == None:
            # Setup our various camera settings
            camera_specifications = camera_recognition.Settings()
            camera_specifications.darknetPath = '/content/darknet/'
            camera_specifications.useCamera = False
            camera_specifications.inputFilename = (
                '/content/yolofiles/cav1/live_test_output.avi')
            camera_specifications.camTimeFile = (
                '/content/yolofiles/cav1/cam_output.txt')
            camera_specifications.cameraHeight = .2
            camera_specifications.cameraAdjustmentAngle = 0.0
            camera_specifications.fps = 60
            camera_specifications.width = 1280
            camera_specifications.height = 720
            camera_specifications.flip = 2
            sq_state['camera_recognition'] = camera_recognition.ProcessCamera(
                camera_specifications)

        coordinates, processed_timestamp = sq_state[
            'camera_recognition'].processCameraFrame(camera_frame,
                                                     camera_timestamp)

    return [coordinates, processed_timestamp, cam_sample[1], time.time()]


@SQify
def write_to_file(processed_camera):
    # Output filename
    output = processed_camera[0]
    camera_timestamp = processed_camera[1]
    outfile = "/content/ticktalkpython/output/example_1_output.txt"
    with open(outfile, 'a') as file:
        file.write(str(output) + "\n")
        print('Processed cam_time:' + str(camera_timestamp) +
              ', sys_proc_time:' +
              str(processed_camera[3] - processed_camera[2]) +
              " cam_timestamp: " + str(camera_timestamp))
    return 1


@GRAPHify
def example_1_test(trigger):
    with TTClock.root() as root_clock:
        cam_sample = camera_sampler(trigger)

        processed_camera = process_camera(cam_sample)

        write = write_to_file(processed_camera)
