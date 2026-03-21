Tutorial - Generating Streams
=============================

.. _tutorial-streamify:

You may have noticed that the previous example ``camera_sampler`` was only
called once. In reality, we'd like to have an indefinite continuous stream
of data, not just call it once! In many cyberphysical systems and IoT
applications, periodic sensing is inherent. Fortunately, we have a TTPython
construct just for that.

Previously, we mentioned how to take a Python function ``camera_sampler``
and add it to our dataflow graph with the decorator ``@SQify``. However, ``@SQify``
is the most basic function and requires tokens to come in to trigger its
computation, i.e., it needs a caller. We can simulate this caller periodically
by using a different decorator, ``@STREAMify``, which creates a special type of SQ.
``@STREAMify`` will cause a function to produce outputs periodically, creating a stream of data.

.. code-block:: python

    @STREAMify
    def camera_sampler(trigger):
        ...

A STREAMified node in TTPython has a unique firing rule. Once a token has been received,
it will generate tokens until the stop time as specified by the programmer.
One can think of this as a token entering the node and the node generating tokens until
the time specified. The function will retrigger itself periodically with the ``TT``
parameters specified in the STREAMify section.

Here, the initial caller of ``camera_sampler`` needs to provide more
information for a STREAMified function to work. As it periodically executes by
itself, the single call to start this operation needs to specify by which
clock it will use to retrigger itself, the period of time between iterations,
and the phase of the clock to synchronize to. Futhermore, as the function will
periodically generate new data, this data needs to be timestamped correspondingly.
The caller of this function needs to provide this information through the keywords
``TTClock``, ``TTPeriod``, ``TTPhase`` and ``TTDataIntervalWidth``.

.. code-block:: python

	...
	cam_sample = camera_sampler(sample_window, TTClock=root_clock, TTPeriod=750000, TTPhase=0, TTDataIntervalWidth=250000)
	...

In TTPython, we have defaulted our root clock to use the system time
to synchronize with NIST's clock. The default root clock runs on the
microsecond precision, so if we were to specify a period of 0.750 seconds,
we would assign ``TTPeriod=750000``.

These clocks assume that there is an internal microsecond clock ticking,
where its counter resets after 750,000 ticks. The programmer can specify
when the SQ will trigger during the counting. A ``TTPhase=0`` specifies that
the ``camera_sampler`` will trigger when the counter reads 0.

When running periodic computation, the function also needs to define the
time context of the data it generates. We use the keyword
``TTDataIntervalWidth`` to do so. This keyword informs the runtime
how to generate a new time-interval for this sample in the stream. When
the runtime initiates an instance of the SQ, the runtime will take the
start and stop time for executing this SQ and take the average. The new
interval is this timestamp average, plus-minus the ``TTDataIntervalWidth``
divided by 2. The width of the resulting interval is then
``TTDataIntervalWidth``.

For our car position tracking example, we'll have STREAMified SQ use the root
clock. We have rewritten the camera application to this periodic nature, which
now takes a single frame with every iteration and outputs the coodinates in a
real-time fashion.

.. code-block:: python

    @STREAMify #streamify is meant for generating sampled data streams
	def camera_sampler(trigger):
		import camera_recognition
		global sq_state

		# Setup the class if we have not done so already
		if sq_state.get('camera', None) == None:
			# Setup our various camera settings
			camera_specifications = camera_recognition.Settings()
			camera_specifications.useCamera = False
			camera_specifications.inputFilename = '/content/yolofiles/cav1/live_test_output.avi'
			camera_specifications.camTimeFile = '/content/yolofiles/cav1/cam_output.txt'
			camera_specifications.cameraHeight = .2
			camera_specifications.cameraAdjustmentAngle = 0.0
			camera_specifications.fps = 60
			camera_specifications.width = 1280
			camera_specifications.height = 720
			camera_specifications.flip = 2
			sq_state['camera'] = camera_recognition.Camera(camera_specifications)

		# Take the camera frame from either a camera or a prerecorded video
		frame_read, camera_timestamp = sq_state['camera'].takeCameraFrame()

    return [frame_read, camera_timestamp]

    @SQify
	def process_camera(cam_sample):
		import camera_recognition
		global sq_state

		camera_frame = cam_sample[0]
		camera_timestamp = cam_sample[1]

		# Setup the class if we have not done so already
		if sq_state.get('camera_recognition', None) == None:
			# Setup our various camera settings
			camera_specifications = camera_recognition.Settings()
			camera_specifications.darknetPath = '/content/darknet/'
			camera_specifications.cameraHeight = .2
			camera_specifications.cameraAdjustmentAngle = 0.0
			camera_specifications.fps = 60
			camera_specifications.width = 1280
			camera_specifications.height = 720
			camera_specifications.flip = 2
			sq_state['camera_recognition'] = camera_recognition.ProcessCamera(camera_specifications)

		# Process the camera frame that we have recieved
		coordinates, processed_timestamp = sq_state['camera_recognition'].processCameraFrame(camera_frame, camera_timestamp)

		return [coordinates, processed_timestamp]

	@GRAPHify
	def example_1_test(trigger):
		A_1 = 1
		with TTClock.root() as root_clock:
			# This is for setting the start-tick of the STREAMify's periodic firing rule
			start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
			# Set the number of iterations that this will run for
			N = 50
			# Setup the stop-tick of the STREAMify's firing rule
			stop_time = start_time + (1000000 * N) # sample for N seconds

			# create a sampling interval by copying the start and stop tick from token values to the token time interval
			sampling_time = VALUES_TO_TTTIME(start_time, stop_time)

			# copy the sampling interval to the input values to the STREAMify node; these input values will be treated as sticky tokens, and define the duration over which STREAMify'd nodes must run
			sample_window = COPY_TTTIME(A_1, sampling_time)

			# Call the camera sampler periodically @ 750ms interval
			cam_sample = camera_sampler(sample_window, TTClock=root_clock, TTPeriod=750000, TTPhase=0, TTDataIntervalWidth=250000)

			# Process the camera frame returned by streamify
			processed_camera = process_camera(cam_sample)

			# Write our output to a file for later overvation
			write = write_to_file(processed_camera)

The input token's time interval intersection between all arguments of the
function dictates the period of time over which the stream will be generated.
You can see that we modify the input token's time interval with the two
functions ``VALUES_TO_TTIME`` and ``COPY_TTIME``. We've provided a slew of
functions to the programmer to interface with the TTPython architecture.
An in depth look at these internal functions can be found
:ref:`here <instructions>`. In the above example, by setting N=50, we are
creating a sampling interval of 50 seconds.

We reserve keywords that start with ``TT`` as special keywords that are
TTPython facing; that is, ``camera_sampler`` does not have access to these
keywords. These help the TTPython runtime to correctly set up the clock
synchronization of the ensemble that hosts that particular SQ.

Now that we have the necessary steps to run a periodic application, check
out and run Steps 4-8 in the Jupyter Notebook. We will not be covering the
camera and LIDAR sensor fusion section of the application in this
tutorial as the TTPython concepts needed to understand it are equivalent
in the camera intersection data pipeline.
