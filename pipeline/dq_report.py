import json
from datetime import datetime

class DQReport:
    def __init__(self):
        self.start_time = datetime.utcnow()
        self.report = {
            "run_timestamp": self.start_time.isoformat(),
            "stage": "2",
            "source_record_counts": {},
            "dq_issues": [],
            "gold_layer_record_counts": {},
            "execution_duration_seconds": 0
        }

    def set_source_counts(self, customers, accounts, transactions):
        self.report["source_record_counts"] = {
            "customers": customers,
            "accounts": accounts,
            "transactions": transactions
        }

    def add_issue(self, issue_code, count, action):
        self.report["dq_issues"].append({
            "issue_code": issue_code,
            "records_affected": count,
            "handling_action": action
        })

    def set_gold_count(self, fact_transactions):
        self.report["gold_layer_record_counts"] = {
            "fact_transactions": fact_transactions
        }

    def finalize(self):
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        self.report["execution_duration_seconds"] = int(duration)

    def save(self, path="/data/output/dq_report.json"):
        self.finalize()
        with open(path, "w") as f:
            json.dump(self.report, f, indent=2)
