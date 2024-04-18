from app import webserver
from flask import request, jsonify
from app.task import *
from flask import current_app

@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    '''
    This implements a post endpoint.
    '''
    if request.method == 'POST':
        data = request.get_json()  # Get JSON data from the request

        # return received data in the response
        return jsonify({"message": "Received data successfully", "data": data}), 200
    else:
        # Method is Not Allowed 
        return jsonify({"error": "Method not allowed"}), 405
@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    '''
    Get the response of a job.
    '''
    job_id = int(job_id)
    task_info = current_app.tasks_runner.dictionary.get(job_id)

    if task_info:
        status = task_info["status"]
        if status == "running":
            return jsonify({'status': 'running'}), 200
        elif status == "done":
            return jsonify({"status": "done", "data": task_info["result"]}), 200
    # If the job id is not found or the task is not completed, return 404
    return jsonify({"status": "not_found"}), 404


@webserver.route('/api/jobs', methods=['GET'])
def get_job_statuses():
    '''
    Gets status for jobs.
    '''
    # Build a list of dictionaries for each job_id and its status
    jobs = []
    for job_id, job_info in current_app.tasks_runner.dictionary.items():
        jobs.append({"job_id": job_id, "status": job_info["status"]})

    return (lambda jobs: jsonify({"status": "success", "jobs": jobs}))

@webserver.route('/api/num_jobs', methods=['GET'])
def get_remaining_jobs_count():
    '''
    Gets all the jobs that are left.
    '''
    # Get the number of jobs to be completed from the task runner
    num_jobs = current_app.tasks_runner.task_queue.qsize()

    response_data = {"status": "success", "remaining_jobs": num_jobs}

    return jsonify(response_data)




@webserver.route('/api/states_mean', methods=['POST'])
def request_states_mean():
    '''
    This gets the average for all the states.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateStatesMeanTask(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    '''
    Gets the mean from a state.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateMeanTask(data['question'], data['state'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    '''
    This gets top 5 of the values.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateBest5Task(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)
    
    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202



@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    '''
    This gets worst 5 of the values.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateWorst5Task(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202


@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    '''
    This gets the value of the global mean.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateGlobalMeanTask(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202


@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    '''
    This gets the difference calculated by average.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateDiffFromMeanTask(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    response_data = {"status": "success", "job_id": job_id}
    
    return jsonify(response_data), 202




@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    '''
    This gets the difference of means.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateStateDiffFromMeanTask(data['question'], data['state'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202


@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    '''
    Gets the mean value from a category.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateMeanByCategoryTask(data['question'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202


@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    '''
    This gets a states mean from a category.
    '''
    # Get data
    data = request.json

    # Create Task object for the request
    task = CalculateStateMeanByCategoryTask(data['question'], data['state'], current_app.data_ingestor.data)

    # Add task to the task queue
    job_id = current_app.tasks_runner.add_task(task)

    # Return response to acknowledge receival of request
    return jsonify({"status": "success", "job_id": job_id}), 202


@webserver.route('/api/graceful_shutdown', methods=['GET'])

def graceful_shutdown_request():
    '''
    This is for the shutdown.
    '''
    # Before shutdown , call ThreadPool stop
    current_app.stop()

    # Return JSON response 
    return jsonify({"status": "success"}), 200


# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    '''
    This is for the accessable routes.
    '''
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route
    paragraphs = ""
    i = 0
    while i < len(routes):
        paragraphs += f"<p>{routes[i]}</p>"
        i += 1

    msg += paragraphs
    return msg




def get_defined_routes():
    '''
    This gets the defined routes.
    '''
    routes = []
    it = iter(current_app.url_map.iter_rules())
    while True:
        try:
            rule = next(it)
            methods = ', '.join(rule.methods)
            routes.append({"endpoint": str(rule), "methods": methods})
        except StopIteration:
            break
    return routes

