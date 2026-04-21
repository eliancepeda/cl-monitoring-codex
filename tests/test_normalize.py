import unittest

from collector.normalize import build_observation_unit, normalize_launch_parameters


class NormalizeTests(unittest.TestCase):
    def test_normalize_launch_parameters_marks_rule_and_hypothesis_roles(self):
        normalized = normalize_launch_parameters("--sp catalog --proxy_country us --debug")
        by_key = {item["normalized_key"]: item for item in normalized["parameters"]}

        self.assertEqual(by_key["sp"]["role"], "identity candidate")
        self.assertEqual(by_key["sp"]["classification_status"], "hypothesis")
        self.assertEqual(by_key["proxy_country"]["role"], "execution modifier")
        self.assertEqual(by_key["proxy_country"]["classification_status"], "fact")
        self.assertEqual(by_key["debug"]["role"], "non-production flag")
        self.assertEqual(by_key["debug"]["classification_status"], "fact")

    def test_normalize_launch_parameters_keeps_negative_literal_values(self):
        normalized = normalize_launch_parameters("--timeout -1")

        self.assertEqual(normalized["parameters"], [
            {
                "raw_token": "--timeout",
                "normalized_key": "timeout",
                "value": "-1",
                "role": "unknown",
                "classification_status": "unknown",
            }
        ])
        self.assertEqual(normalized["positionals"], [])

    def test_normalize_launch_parameters_degrades_gracefully_on_unbalanced_quotes(self):
        normalized = normalize_launch_parameters('--sp catalog bad "quote')

        self.assertEqual(normalized["tokens"], ["--sp", "catalog", "bad", '"quote'])
        self.assertEqual(
            normalized["parameters"],
            [
                {
                    "raw_token": "--sp",
                    "normalized_key": "sp",
                    "value": "catalog",
                    "role": "identity candidate",
                    "classification_status": "hypothesis",
                }
            ],
        )
        self.assertEqual(normalized["positionals"], ["bad", '"quote'])

    def test_build_observation_unit_uses_spider_schedule_and_params(self):
        task = {
            "_id": "task-1",
            "schedule_id": "schedule-1",
            "args": "--sp catalog --fp shoes",
            "status": "finished",
        }
        spider = {"_id": "spider-1", "name": "demo-spider"}

        observation = build_observation_unit(task, spider)

        self.assertEqual(
            observation["observation_key"],
            "demo-spider|schedule-1|fp=shoes|sp=catalog",
        )
        self.assertEqual(observation["task_id"], "task-1")
        self.assertEqual(observation["spider_name"], "demo-spider")

    def test_build_observation_unit_prefers_param_field_when_present(self):
        task = {
            "_id": "task-1",
            "schedule_id": "schedule-1",
            "param": "-sp 1 -fp 99",
            "args": "--sp catalog --fp shoes",
            "status": "finished",
        }
        spider = {"_id": "spider-1", "name": "demo-spider"}

        observation = build_observation_unit(task, spider)

        self.assertEqual(
            observation["normalized_params"]["parameters"],
            [
                {
                    "raw_token": "-sp",
                    "normalized_key": "sp",
                    "value": "1",
                    "role": "identity candidate",
                    "classification_status": "hypothesis",
                },
                {
                    "raw_token": "-fp",
                    "normalized_key": "fp",
                    "value": "99",
                    "role": "identity candidate",
                    "classification_status": "hypothesis",
                },
            ],
        )
        self.assertEqual(
            observation["observation_key"],
            "demo-spider|schedule-1|fp=99|sp=1",
        )

    def test_build_observation_unit_treats_empty_param_as_absent(self):
        task = {
            "_id": "task-1",
            "schedule_id": "schedule-1",
            "param": "",
            "args": "--sp catalog --fp shoes",
            "command": "--sp ignored",
            "cmd": "--sp ignored-too",
            "status": "finished",
        }
        spider = {"_id": "spider-1", "name": "demo-spider"}

        observation = build_observation_unit(task, spider)

        self.assertEqual(
            observation["normalized_params"]["parameters"],
            [
                {
                    "raw_token": "--sp",
                    "normalized_key": "sp",
                    "value": "catalog",
                    "role": "identity candidate",
                    "classification_status": "hypothesis",
                },
                {
                    "raw_token": "--fp",
                    "normalized_key": "fp",
                    "value": "shoes",
                    "role": "identity candidate",
                    "classification_status": "hypothesis",
                },
            ],
        )
        self.assertEqual(
            observation["observation_key"],
            "demo-spider|schedule-1|fp=shoes|sp=catalog",
        )

    def test_build_observation_unit_treats_all_zero_schedule_id_as_unscheduled(self):
        task = {
            "_id": "task-1",
            "schedule_id": "000000000000000000000000",
            "args": "--sp catalog --fp shoes",
            "status": "finished",
        }
        spider = {"_id": "spider-1", "name": "demo-spider"}

        observation = build_observation_unit(task, spider)

        self.assertEqual(observation["schedule_id"], "unscheduled")
        self.assertEqual(
            observation["observation_key"],
            "demo-spider|unscheduled|fp=shoes|sp=catalog",
        )


if __name__ == "__main__":
    unittest.main()
