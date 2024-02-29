import unittest
from unittest.mock import patch, mock_open

from nthflow.flow import Flow


class TestFlow(unittest.TestCase):
    def setUp(self):
        self.flow = Flow("tests/mock_configs/*.yml")

    @patch("glob.glob")
    def test_get_configs(self, mock_glob):
        mock_glob.return_value = [
            "tests/mock_configs/config1.yml",
            "tests/mock_configs/config2.yml",
        ]
        self.assertEqual(
            self.flow._get_configs(),
            ["tests/mock_configs/config1.yml", "tests/mock_configs/config2.yml"],
        )

    @patch("builtins.open", new_callable=mock_open, read_data="name: my_flow_1")
    def test_read_config(self, mock_file):
        self.assertEqual(
            self.flow._read_config("tests/mock_configs/config1.yml"),
            {"name": "my_flow_1"},
        )

    @patch.object(Flow, "_read_config")
    @patch.object(Flow, "_get_configs")
    def test_set_flow(self, mock_get_configs, mock_read_config):
        mock_get_configs.return_value = ["tests/mock_configs/config1.yml"]
        mock_read_config.return_value = {
            "my_flow_1": {
                "cron": "0 0 * * *",
                "default_args": {
                    "on_failure_callback": "send_team_email.py",
                    "on_success_callback": None,
                    "retries": 3,
                    "retry_delay": 5,
                    "start_date": "2021-01-01",
                },
                "tasks": [
                    {
                        "name": "task1",
                        "command": "python task1.py",
                        "args": {"arg1": "value1"},
                    },
                    {
                        "name": "task2",
                        "command": "python task2.py",
                        "args": {"arg2": "value2"},
                        "dependencies": ["task1"],
                    },
                ],
            }
        }
        self.flow._set_flow()

        for flow_uid, flow_config in self.flow.flows.items():
            self.assertEqual(flow_config.name, "my_flow_1")
            self.assertEqual(flow_config.cron, "0 0 * * *")
            self.assertEqual(
                flow_config.default_args,
                {
                    "on_failure_callback": "send_team_email.py",
                    "on_success_callback": None,
                    "retries": 3,
                    "retry_delay": 5,
                    "start_date": "2021-01-01",
                },
            )

    @patch.object(Flow, "_set_flow")
    @patch.object(Flow, "_set_flow_dag")
    def test_build(self, mock_set_flow_dag, mock_set_flow):
        self.flow.build()
        mock_set_flow.assert_called_once()
        mock_set_flow_dag.assert_called_once()


if __name__ == "__main__":
    unittest.main()
