import pandas as pd
import random
from datetime import datetime, timedelta
from io import StringIO

class QuantumWorkloadGenerator:
    def __init__(self, available_dataset: pd.DataFrame):
        self.available_datasets = available_dataset
        

    def generate_single_spike_dataset(self, num_qtasks: int, burst_duration: timedelta, normal_interval: timedelta, burst_tasks: int) -> pd.DataFrame:
        """
        Generate a dataset with a sudden burst of tasks.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - burst_duration: Duration of the burst period.
        - normal_interval: Time interval between tasks during normal periods.
        - burst_tasks: Number of tasks arriving within the burst duration.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        # Randomly select num_qtasks tasks from the available datasets
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        for _ in range(burst_tasks):  # First 40 tasks arrive within the burst duration
            timestamp = current_time + timedelta(seconds=random.uniform(0, burst_duration.total_seconds()))
            timestamps.append(timestamp.timestamp())
            current_time = timestamp
            
        for _ in range(num_qtasks-burst_tasks):  # Next 10 tasks follow normal arrival rate
            current_time += normal_interval
            timestamps.append(current_time.timestamp())
        # Ensure we have exactly num_qtasks timestamps
        
        # Add the timestamps to the DataFrame
        sample_df['timestamp'] = timestamps
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv('single_spike_dataset.csv', index=False)
        return sample_df

    def generate_bursty_dataset(self, num_qtasks: int, burst_duration: timedelta, burst_tasks: int, low_activity_interval: timedelta) -> pd.DataFrame:
        """
        Generate a dataset with a sudden burst of tasks followed by low activity.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - burst_duration: Duration of the burst period.
        - burst_tasks: Number of tasks arriving within the burst duration.
        - low_activity_interval: Time interval between tasks during low activity periods.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        sample_df = None
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        # Generate burst tasks
        for _ in range(burst_tasks):
            if len(timestamps) >= num_qtasks:
                break
            timestamps.append(current_time.timestamp())
            current_time += timedelta(seconds=random.uniform(0, burst_duration.total_seconds() / burst_tasks))
        
        # Generate low activity tasks
        while len(timestamps) < num_qtasks:
            timestamps.append(current_time.timestamp())
            current_time += low_activity_interval

        sample_df['timestamp'] = timestamps[:num_qtasks]
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv('bursty_dataset.csv', index=False)
        return sample_df

    def simulate_regular_spikes(self, num_qtasks: int, spike_interval: timedelta, spike_tasks: int, normal_interval: timedelta) -> pd.DataFrame:
        """
        Simulate regular spikes in the arrival rate of tasks at fixed intervals.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - spike_interval: Time interval between spikes.
        - spike_tasks: Number of tasks arriving during each spike.
        - normal_interval: Time interval between tasks during normal periods.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        for i in range(num_qtasks):
            if i % spike_tasks == 0 and i != 0:
                current_time += spike_interval
                for _ in range(spike_tasks):
                    if len(timestamps) >= num_qtasks:
                        break
                    timestamps.append(current_time.timestamp())
                    current_time += timedelta(seconds=random.uniform(0, spike_interval.total_seconds() / spike_tasks))
            else:
                timestamps.append(current_time.timestamp())
                current_time += normal_interval

        sample_df['timestamp'] = timestamps[:num_qtasks]
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv("regular_spike_dataset.csv", index=False)
        return sample_df

    def simulate_ramp_up(self, num_qtasks: int, initial_interval: timedelta, final_interval: timedelta) -> pd.DataFrame:
        """
        Simulate ramp-up timestamps where the interval between tasks decreases over time.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - initial_interval: Initial time interval between tasks.
        - final_interval: Final time interval between tasks.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        interval_step = (initial_interval - final_interval) / (num_qtasks - 1)

        for i in range(num_qtasks):
            timestamps.append(current_time.timestamp())
            current_time += initial_interval - i * interval_step

        sample_df['timestamp'] = timestamps[:num_qtasks]
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv("ramp_up_dataset.csv", index=False)
        return sample_df

    def simulate_ramp_down(self, num_qtasks: int, initial_interval: timedelta, final_interval: timedelta) -> pd.DataFrame:
        """
        Simulate ramp-down timestamps where the interval between tasks increases over time.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - initial_interval: Initial time interval between tasks.
        - final_interval: Final time interval between tasks.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        interval_step = (final_interval - initial_interval) / (num_qtasks - 1)

        for i in range(num_qtasks):
            timestamps.append(current_time.timestamp())
            current_time += initial_interval + i * interval_step

        sample_df['timestamp'] = timestamps[:num_qtasks]
        #sample_df.insert(0, 'subset', 1)
        sample_df.to_csv("ramp_down_dataset.csv", index = False)
        return sample_df

    def generate_random_bursts(self, num_qtasks: int, burst_chance: float, burst_duration: timedelta, max_tasks_in_burst: int, normal_interval: timedelta) -> pd.DataFrame:
        """
        Generate timestamps with random bursts of high task arrivals.

        Parameters:
        - num_qtasks: Number of qtasks to generate.
        - burst_chance: Probability of a burst occurring at any given task arrival.
        - burst_duration: Duration of each burst.
        - max_tasks_in_burst: Maximum number of tasks in a burst.
        - normal_interval: Time interval between tasks during normal periods.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True)
        
        base_time = datetime.now()
        timestamps = []
        current_time = base_time

        while len(timestamps) < num_qtasks:
            if random.random() < burst_chance:
                burst_end_time = current_time + burst_duration
                for _ in range(random.randint(1, max_tasks_in_burst)):
                    if len(timestamps) >= num_qtasks:
                        break
                    timestamps.append(current_time.timestamp())
                    current_time += timedelta(seconds=random.uniform(0, burst_duration.total_seconds()))
            else:
                timestamps.append(current_time.timestamp())
                current_time += normal_interval

        sample_df['timestamp'] = timestamps[:num_qtasks]
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv("random_burst_dataset.csv", index= False)
        return sample_df

    def map_invocation_times(self, invocation_times: pd.DataFrame) -> pd.DataFrame:
        """
        Map the available quantum tasks dataset with the user's invocation time dataset to generate a quantum workload dataset.

        Parameters:
        - invocation_times: A DataFrame containing the invocation times.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        num_qtasks = len(invocation_times)
        sample_df = self.available_datasets.sample(n=num_qtasks, replace=True).reset_index(drop=True)
        
        # Ensure the invocation_times DataFrame has a 'timestamp' column
        if 'timestamp' not in invocation_times.columns:
            raise ValueError("The invocation times DataFrame must contain a 'timestamp' column.")
        
        sample_df['timestamp'] = invocation_times['timestamp']
        sample_df.insert(0, 'subset', 1)
        sample_df.to_csv("map_invocation_times_dataset.csv", index = False)
        return sample_df


    def generate_workload(self, scenario: int, **kwargs) -> pd.DataFrame:
        """
        Generate a workload dataset based on the chosen scenario.

        Parameters:
        - scenario: The scenario to simulate (1-6).
        - kwargs: Additional parameters for the chosen scenario.

        Returns:
        - A DataFrame with the generated quantum workload dataset.
        """
        if scenario == 1:
            return self.generate_bursty_dataset(**kwargs)
        elif scenario == 2:
            return self.simulate_regular_spikes(**kwargs)
        elif scenario == 3:
            return self.simulate_ramp_up(**kwargs)
        elif scenario == 4:
            return self.simulate_ramp_down(**kwargs)
        elif scenario == 5:
            return self.generate_random_bursts(**kwargs)
        elif scenario == 6:
            return self.map_invocation_times(**kwargs)
        else:
            raise ValueError("Invalid scenario number. Choose a scenario between 1 and 6.")
# Example usage
if __name__ == "__main__":

    available_datasets = pd.read_csv("qdataset_indep_2-50q.csv")

    qwg = QuantumWorkloadGenerator(available_datasets)
    
    # 1. Generate a dataset with a sudden burst of tasks.
    #burst_duration = timedelta(minutes=1)
    #low_activity_interval = timedelta(minutes=10)
    #burst_tasks = 40
    #num_qtasks = 50
    #generated_dataset = qwg.generate_bursty_dataset(num_qtasks, burst_duration, burst_tasks, low_activity_interval)
    

    # 2. Generate a dataset with a sudden burst of tasks followed by low activity.
    burst_duration = timedelta(minutes=1)
    low_activity_interval = timedelta(minutes=5)
    burst_tasks = 10
    num_qtasks = 25
    generated_dataset = qwg.generate_bursty_dataset(num_qtasks, burst_duration, burst_tasks, low_activity_interval)
    

    # 3. Simulate regular spikes in the arrival rate of tasks at fixed intervals.
    spike_interval = timedelta(minutes=15)
    normal_interval = timedelta(minutes=10)
    spike_tasks = 5
    generated_dataset = qwg.simulate_regular_spikes(num_qtasks, spike_interval, spike_tasks, normal_interval)
    

    # 4. Simulate ramp-up timestamps where the interval between tasks decreases over time.
    initial_interval = timedelta(minutes=10)
    final_interval = timedelta(seconds=30)
    generated_dataset = qwg.simulate_ramp_up(num_qtasks, initial_interval, final_interval)
    

    # 5. Simulate ramp-down timestamps where the interval between tasks increases over time.
    initial_interval = timedelta(seconds=30)
    final_interval = timedelta(minutes=10)
    generated_dataset = qwg.simulate_ramp_down(num_qtasks, initial_interval, final_interval)
   

    # 6. Generate timestamps with random bursts of high task arrivals.
    burst_chance = 0.2
    max_tasks_in_burst = 10
    normal_interval = timedelta(minutes=10)
    generated_dataset = qwg.generate_random_bursts(num_qtasks, burst_chance, burst_duration, max_tasks_in_burst, normal_interval)
    
