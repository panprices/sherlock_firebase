from google.cloud import pubsub_v1
import json
import threading

class Publisher():

	def __init__(self, project_id, topic):
		batch_settings = pubsub_v1.types.BatchSettings(max_messages=1)
		self.client = pubsub_v1.PublisherClient()
		self.topic_path = self.client.topic_path(project_id, topic)

	def split_tuple(self, input_data, wanted_num) :
		num_in_each_bucket = (len(input_data) + 1) / wanted_num
		buckets = []
		bucket = []
		for data_row in input_data :
			if len(bucket) > num_in_each_bucket :
				buckets.append(bucket)
				bucket = []
			bucket.append(data_row)
		buckets.append(bucket)
		# Free memory just in case
		del input_data
		return buckets

	def publish_msg_threaded(self, messages, num_threads=10) :
		threads = {}
		buckets = self.split_tuple(messages, num_threads)
		# Iterate over the data
		for i, bucket in enumerate(buckets) :
			# Define a thread and save a pointer in threads dict
			threads[i] = threading.Thread(
				target=self.publish_messages,
				args=([bucket])
			)
			# Start the thread
			threads[i].start()
		# Join all the threads to the main which will make this
		# exectuion blocking until all the threads have finished
		for i, bucket in enumerate(buckets) :
			threads[i].join()

	def publish_messages(self, messages):
		message_ids = []
		for message in messages:
			# Data must be a bytestring
			data = json.dumps(message, indent=2, sort_keys=True, default=str).encode('utf-8')

			print(data)

			future = self.client.publish(self.topic_path, data=data)
			message_id = future.result()
			message_ids.append(message_id)
		return message_ids