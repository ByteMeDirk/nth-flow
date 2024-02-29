"""
This class is the entrypoint that defines the Flow object for each
of the DAGs. It is the main class that orchestrates the tasks and
the dependencies between them for each DAG.
"""

import argparse
import logging
from dataclasses import dataclass
from glob import glob
from typing import List, Dict
from uuid import uuid4

import yaml

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s - nthFlow - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclass
class TaskConfig:
    """
    Data class to define the configuration for a single Task.
    Each task is linked to a single Flow via the flow_uid.
    """

    uid: str
    flow_uid: str
    name: str
    command: str
    args: dict
    dependencies: List[str]

    # Task Metadata
    start_time: str = None
    end_time: str = None
    error: str = None
    retries: int = 0
    status: str = "PENDING"

    def __post_init__(self):
        """
        Post init method to check if the status is valid.
        """
        if self.status not in [
            "PENDING",
            "RUNNING",
            "SUCCESS",
            "FAILED",
            "UP_FOR_RETRY",
        ]:
            raise ValueError(f"Invalid status: {self.status}")


# Data Class to define a Single FLow
@dataclass
class FlowConfig:
    """
    Data class to define the configuration for a single Flow.
    """

    uid: str
    name: str
    cron: str
    default_args: dict
    tasks: List[TaskConfig]

    # Flow Metadata
    start_time: str = None
    end_time: str = None
    error: str = None
    retries: int = 0
    status: str = "PENDING"

    def __post_init__(self):
        """
        Post init method to check if the status is valid.
        """
        if self.status not in [
            "PENDING",
            "RUNNING",
            "SUCCESS",
            "FAILED",
            "UP_FOR_RETRY",
        ]:
            raise ValueError(f"Invalid status: {self.status}")


class Flow:
    def __init__(self, config_directory: str):
        self._config_directory: str = config_directory
        self._config_files: List[str] = self._get_configs()
        self.flows: Dict[str, FlowConfig] = (
            {}
        )  # Dictionary to store FlowConfig instances
        self.flow_dag: Dict[str, Dict[str, int]] = (
            {}
        )  # Dictionary to store the flow DAG

    def _get_configs(self):
        """
        Iterates through the defined config directory and returns
        a list of all the config files.

        Returns:
            List[str]: A list of all the config files.
        """
        self._config_files = glob(self._config_directory)
        logging.info("Found %s config files", len(self._config_files))
        return self._config_files

    def _read_config(self, config_file: str):
        """
        Reads in the config file as a dict.

        Args:
            config_file (str): The path to the config file.

        Returns:
            dict: The config file as a dict.
        """
        logging.info("Reading config file: %s", config_file)
        with open(config_file, "r") as file:
            config = yaml.safe_load(file)
        return config

    def _set_flow(self):
        """
        Iterates over the _config_files and reads them in as dicts,
        then applies the flow config to the FlowConfig dataclass
        and it's child tasks to the TaskConfig dataclass.

        Each dataclass is then stored to be accessed by the scheduler
        for compiling and running.
        """
        # Read in the flow config
        logging.info("Setting flow...")
        for _config_file in self._config_files:
            config = self._read_config(config_file=_config_file)

            for _flow_name, _flow_config in config.items():
                _flow_uid = uuid4().hex

                # Iterate over flow config tasks
                flow_config_tasks: List[TaskConfig] = []
                for _flow_config_task in _flow_config["tasks"]:
                    task_config = TaskConfig(
                        uid=uuid4().hex,
                        flow_uid=_flow_uid,
                        name=_flow_config_task["name"],
                        command=_flow_config_task["command"],
                        args=_flow_config_task.get("args", {}),
                        dependencies=_flow_config_task.get("dependencies", []),
                    )
                    flow_config_tasks.append(task_config)

                # Create a FlowConfig instance and store it in the dictionary
                flow_config = FlowConfig(
                    uid=_flow_uid,
                    name=_flow_name,
                    cron=_flow_config["cron"],
                    default_args=_flow_config["default_args"],
                    tasks=flow_config_tasks,
                )
                self.flows[flow_config.uid] = flow_config

        logging.info("Flows: %s", self.flows)

    @staticmethod
    def _topological_sort(tasks: List[TaskConfig]) -> List[str]:
        """
        Sorts the tasks based on their dependencies.
        This is done by creating a dictionary of tasks and their dependencies
        and then iterating over the dictionary to remove tasks with no dependencies.

        Args:
            tasks (List[TaskConfig]): A list of TaskConfig instances.

        Returns:
            List[str]: A list of the sorted tasks.
        """
        # Create a dictionary to store the tasks with their dependencies
        task_dict = {task.name: set(task.dependencies) for task in tasks}

        # Create a list to store the sorted tasks
        sorted_tasks = []

        # While there are tasks left to process
        while task_dict:
            # Get all tasks with no dependencies
            no_deps = [task for task, deps in task_dict.items() if not deps]

            # If there are no tasks with no dependencies, there is a cycle
            if not no_deps:
                logging.error("Cycle detected for tasks: %s", task_dict)
                raise ValueError("Cycle detected for tasks: %s" % task_dict)

            # Add tasks with no dependencies to the sorted tasks
            sorted_tasks.extend(no_deps)

            # Remove the tasks with no dependencies from the dictionary
            task_dict = {
                task: (deps - set(no_deps))
                for task, deps in task_dict.items()
                if task not in no_deps
            }

        logging.info("Sorted tasks: %s", sorted_tasks)
        return sorted_tasks

    def _set_flow_dag(self) -> None:
        """
        Sets the flow DAG for each flow.
        The flow DAG is a dictionary of tasks and their dependencies.
        The tasks are sorted using a topological sort to ensure that
        the dependencies are met.
        """
        for flow_uid, flow_config in self.flows.items():
            task_order = self._topological_sort(flow_config.tasks)
            self.flow_dag[flow_uid] = {
                task_name: index for index, task_name in enumerate(task_order)
            }

        logging.info("Flow DAG: %s", self.flow_dag)

    def build(self):
        self._set_flow()
        self._set_flow_dag()


if __name__ == "__main__":
    # Define the command line arguments
    parser = argparse.ArgumentParser(description="Flow")
    parser.add_argument(
        "--config_directory",
        type=str,
        required=True,
        help="The directory containing the config files",
    )
    args = parser.parse_args()

    # Create a Flow object
    flow = Flow(args.config_directory)
    flow.build()
