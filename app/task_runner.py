import json
from queue import Queue
from threading import Thread, Event
import os
import multiprocessing
from queue import Empty

class ThreadPool:
    """
    This class implements a thread pool.
    """

    def __init__(self):
        """
        Initialize the ThreadPool instance.
        """
        self.dictionary = {}
        self.id_counter = 0  # Counter task IDs
        self.num_threads = self.get_thread_count()  # Get the number of threads allowed

        # Initialize a task queue and threads 
        self.task_queue = Queue()
        self.threads = [TaskRunner(self.task_queue, self.dictionary) for _ in range(self.num_threads)]

    def get_thread_count(self):
        """
        Get the number of threads allowed.
        """
        return int(os.environ.get('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))

    def add_task(self, task):
        """
        Add task to the task queue.
        """
        self.id_counter += 1
        task_id = self.id_counter
        self.task_queue.put((task, task_id))
        return task_id

    def start(self):
        """
        Start every thread from the pool.
        """
        num_threads = len(self.threads)
        index = 0
        while index < num_threads:
            self.threads[index].start()
            index += 1

    def stop(self):
        """
        Get every thread to stop.
        """
        num_threads = len(self.threads)
        index = 0
        while index < num_threads:
            self.threads[index].stop()
            index += 1

class TaskRunner(Thread):
    """
    Class that implements the functionality of a thread.
    """
    def __init__(self, task_queue, dictionary):
        """
        init function for Task_Runner.
        """
        Thread.__init__(self)
        self.task_queue = task_queue
        self.graceful_shutdown = Event()
        self.dictionary = dictionary

    def run(self):
        """
        Takes tasks and executes them.
        """
        while not self.graceful_shutdown.is_set():
            try:
                # Wait for a task from the queue with a timeout
                task, task_id = self.task_queue.get(timeout=1)

                # Execute the task and save the result
                self.execute_task(task, task_id)

            except Empty:
                # Empty queue, continue 
                continue

            except Exception as e:
                # Handle specific exceptions or log them as needed
                print(f"Error occurred")

        # Graceful shutdown complete or thread stopped

    def execute_task(self, task, id):
        """
        Execute a task and save the result.
        """
        # Update status to "running" initially
        self.update_status(id, "running", None)

        # Execute the task and get the result
        value = task.execute()

        # Update status to "done" and save the result
        self.update_status(id, "done", value)

        # Process the data or perform necessary operations
        self.save_result(id, value)

    def update_status(self, job_id, status, result):
        """
        Update the status and result in the dictionary.
        """
        self.dictionary[job_id] = {"status": status, "result": result}

    def save_result(self, job_id, result):
        """
        Save result to JSON.
        """
        # Define the directory where the results will be saved
        results_dir = Path("results")

        # Create the directory if it doesn't exist
        results_dir.mkdir(parents=True, exist_ok=True)

        # Define the path to the JSON file
        file_path = results_dir / f"{job_id}.json"

        # Write the results to the JSON file
        with file_path.open(mode='w', encoding='utf-8') as file:
            json.dump(result, file)
    def stop(self):
        """
        The thread stops.
        """
        # Method to stop the thread gracefully
        self.graceful_shutdown.set()
