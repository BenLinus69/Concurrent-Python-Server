from app import webserver

class Task:
    '''
    Class of tasks.
    '''
    def __init__(self, data_frame):
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func
        '''
        raise NotImplementedError("Method 'execute' must be implemented in subclasses")

class CalculateMeanTask:
    '''
    Gets the mean from a state.
    '''
    def __init__(self, question, state, data_frame):
        self.question = question
        self.state = state
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateMeanTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        relevant_data = self.get_relevant_data()
        state_mean = relevant_data['Data_Value'].mean()

        return {self.state: state_mean}

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        if self.state is None:
            return {"status": "error", "message": "State not specified"}

        return None

    def get_relevant_data(self):
        '''
        Get data based on the question and state.
        '''
        return self.data_frame[(self.data_frame['Question'] == self.question) &
                               (self.data_frame['LocationDesc'] == self.state)]


class CalculateStatesMeanTask:
    '''
    This gets the average for all the states.
    '''
    def __init__(self, question, data_frame):
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateStatesMeanTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        state_means = self.calculate_state_means()
        result = {state: mean for state, mean in sorted(state_means.items(), key=lambda x: x[1])}
        return result


    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_state_means(self):
        '''
        Calculate mean values for all states based on the question.
        '''
        relevant_data = self.data_frame[self.data_frame['Question'] == self.question]
        state_means = {state: state_data['Data_Value'].mean() for state, state_data in relevant_data.groupby('LocationDesc')}
        return state_means



class CalculateBest5Task:
    '''
    This gets top 5 of the values.
    '''
    def __init__(self, question, data_frame):
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateBest5Task.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        state_means = self.calculate_state_means()
        sorted_results = self.sort_states(state_means)
        best_5 = self.get_best_5(sorted_results)

        return best_5

    def validate_input(self):
        '''
        Validate input
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_state_means(self):
        '''
        Calculate mean values for all states based on the question.
        '''
        relevant_data = self.data_frame[self.data_frame['Question'] == self.question]
        state_means = {state: state_data['Data_Value'].mean() for state, state_data in relevant_data.groupby('LocationDesc')}
        return state_means


    def sort_states(self, state_means):
        '''
        Sort states based on mean values.
        '''
        if self.question in webserver.data_ingestor.questions_best_is_min:
            sorted_results = sorted(state_means.items(), key=lambda x: x[1])
        else:
            sorted_results = sorted(state_means.items(), key=lambda x: x[1], reverse=True)
        return sorted_results

    def get_best_5(self, sorted_results):
        '''
        Get best 5 states from results.
        '''
        best_5 = dict(sorted_results[:5])
        return best_5

class CalculateWorst5Task:
    '''
    This gets worst 5 of the values.
    '''
    def __init__(self, question, data_frame):
        '''
        Initialize CalculateWorst5Task.
        '''
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateWorst5Task.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        state_means = self.calculate_state_means()
        sorted_results = self.sort_states(state_means)
        worst_5 = self.get_worst_5(sorted_results)

        return worst_5

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_state_means(self):
        '''
        Calculate mean values for all states based on the question.
        '''
        relevant_data = self.data_frame[self.data_frame['Question'] == self.question]
        state_means = {state: state_data['Data_Value'].mean() for state, state_data in relevant_data.groupby('LocationDesc')}
        return state_means


    def sort_states(self, state_means):
        '''
        Sort based on mean values.
        '''
        if self.question in webserver.data_ingestor.questions_best_is_max:
            sorted_results = sorted(state_means.items(), key=lambda x: x[1])
        else:
            sorted_results = sorted(state_means.items(), key=lambda x: x[1], reverse=True)
        return sorted_results

    def get_worst_5(self, sorted_results):
        '''
        Get the worst 5 states from results.
        '''
        worst_5 = dict(sorted_results[:5])
        return worst_5


class CalculateGlobalMeanTask:
    '''
    This gets the value of the global mean.
    '''
    def __init__(self, question, data_frame):
        '''
        Initialize CalculateGlobalMeanTask.
        '''
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateGlobalMeanTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        global_mean = self.calculate_global_mean()
        return {"global_mean": global_mean}

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_global_mean(self):
        '''
        Calculate the global mean value based on the question.
        '''
        relevant_data = self.data_frame[(self.data_frame['Question'] == self.question)]
        global_mean = relevant_data['Data_Value'].mean()
        return global_mean


class CalculateDiffFromMeanTask:
    '''
    This gets the difference calculated by mean.
    '''
    def __init__(self, question, data_frame):
        '''
        Initialize CalculateDiffFromMeanTask.
        '''
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateDiffFromMeanTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        state_means = self.calculate_state_means()
        global_mean = self.calculate_global_mean()
        diff_from_mean = self.calculate_diff_from_mean(state_means, global_mean)

        return diff_from_mean

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_state_means(self):
        '''
        Calculate mean values for each state based on the question.
        '''
        relevant_data = self.data_frame[self.data_frame['Question'] == self.question]
        state_means = {state: relevant_data[relevant_data['LocationDesc'] == state]['Data_Value'].mean() for state in relevant_data['LocationDesc'].unique()}
        return state_means



    def calculate_global_mean(self):
        '''
        Calculate the global mean value based on the question.
        '''
        relevant_data = self.data_frame[self.data_frame['Question'] == self.question]
        global_mean = relevant_data['Data_Value'].mean()
        return global_mean

    def calculate_diff_from_mean(self, state_means, global_mean):
        '''
        Calculate the difference between the global mean and state means.
        '''
        sorted_diff_from_mean = sorted({state: global_mean - mean for state, mean in state_means.items()}.items(), key=lambda x: x[1], reverse=True)
        return {state: mean for state, mean in sorted_diff_from_mean}

class CalculateStateDiffFromMeanTask:
    '''
    This gets the difference of means.
    '''
    def __init__(self, question, state, data_frame):
        '''
        Initialize CalculateStateDiffFromMeanTask.
        '''
        self.question = question
        self.state = state
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateStateDiffFromMeanTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        state_mean, global_mean = self.calculate_means()
        diff_from_mean = global_mean - state_mean

        return {self.state: diff_from_mean}

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_means(self):
        '''
        Calculate the mean values for the specified state and global data.
        '''
        relevant_data = self.data_frame[(self.data_frame['Question'] == self.question) &
                                        (self.data_frame['LocationDesc'] == self.state)]
        global_data = self.data_frame[self.data_frame['Question'] == self.question]

        if relevant_data.empty:
            return None, None 

        state_mean = relevant_data['Data_Value'].mean()
        global_mean = global_data['Data_Value'].mean()

        return state_mean, global_mean


class CalculateMeanByCategoryTask:
    '''
    Gets the mean value from a category.
    '''
    def __init__(self, question, data_frame):
        '''
        Initialize CalculateMeanByCategoryTask.
        '''
        self.question = question
        self.data_frame = data_frame

    def execute(self):
        '''
        Execute func for CalculateMeanByCategoryTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        mean_by_category = self.calculate_mean_by_category()
        formatted_results = self.format_results(mean_by_category)

        return formatted_results

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_mean_by_category(self):
        '''
        Calculate the mean value for each category from states.
        '''
        return self.data_frame[self.data_frame['Question'] == self.question].groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()

    def format_results(self, mean_by_category):
        '''
        Format the results.
        '''
        formatted_results = {f"('{state}', '{category}', '{segment}')": mean_value for (state, category, segment), mean_value in mean_by_category.items()}
        return formatted_results

class CalculateStateMeanByCategoryTask:
    '''
    This gets a states mean from a category.
    '''
    def __init__(self, question, state, data_frame):
        '''
        Initialize CalculateStateMeanByCategoryTask.
        '''
        self.question = question
        self.state = state
        self.data_frame = data_frame

    def execute(self):
        '''
        This executes CalculateStateMeanByCategoryTask.
        '''
        error_response = self.validate_input()
        if error_response:
            return error_response, 400

        mean_by_category = self.calculate_mean_by_category()
        formatted_results = self.format_results(mean_by_category)

        return {self.state: formatted_results}

    def validate_input(self):
        '''
        Validate the input parameters.
        '''
        if self.question not in webserver.data_ingestor.questions_best_is_min and \
           self.question not in webserver.data_ingestor.questions_best_is_max:
            return {"status": "error", "message": "Invalid question"}

        return None

    def calculate_mean_by_category(self):
        '''
        Calculate the mean value by category for state and question.
        '''
        relevant_data = self.data_frame[(self.data_frame['Question'] == self.question) & (self.data_frame['LocationDesc'] == self.state)]
        mean_by_category = relevant_data.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        return mean_by_category

    def format_results(self, mean_by_category):
        '''
        Format the results.
        '''
        formatted_results = {f"('{category}', '{segment}')": mean_value for (category, segment), mean_value in mean_by_category.items()}
        return formatted_results

