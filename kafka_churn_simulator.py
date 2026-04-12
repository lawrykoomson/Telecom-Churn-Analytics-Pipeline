"""
Real-Time Churn Event Stream Simulator
========================================
Simulates Apache Kafka-style real-time streaming of
subscriber churn risk events for MTN Ghana.

Architecture:
    Producer       → generates live subscriber risk events
    RiskConsumer   → flags Critical and High tier subscribers
    MetricsConsumer → aggregates real-time churn KPIs
    AlertConsumer  → logs retention action alerts

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_churn.log"),
        logging.StreamHandler()
    ]
)

TOPIC_NAME         = "telecom.churn.risk.events"
PARTITION_COUNT    = 3
PRODUCER_RATE_HZ   = 10
SIMULATION_SECONDS = 60

REGIONS = ["Greater Accra","Ashanti","Western","Eastern","Northern","Volta","Brong-Ahafo"]
PLANS   = ["Daily Bundle","Weekly Bundle","Monthly Bundle","Pay As You Go","Enterprise"]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


class ChurnEventTopic:
    def __init__(self, name, partitions=3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.counter    = 0
        self.lock       = threading.Lock()

    def produce(self, message):
        with self.lock:
            pid = self.counter % len(self.partitions)
            self.partitions[pid].put(message)
            self.counter += 1

    def consume(self, partition_id, timeout=0.1):
        try:
            return self.partitions[partition_id].get(timeout=timeout)
        except queue.Empty:
            return None


class ChurnEventProducer(threading.Thread):
    def __init__(self, topic, rate_hz, duration_secs):
        super().__init__(name="ChurnProducer", daemon=True)
        self.topic    = topic
        self.rate_hz  = rate_hz
        self.duration = duration_secs
        self.produced = 0
        self.running  = True
        self.logger   = logging.getLogger("ChurnProducer")
        self._counter = 1

    def generate_event(self):
        days_inactive = random.choices(
            [random.randint(1,30), random.randint(30,90),
             random.randint(90,270), random.randint(270,730)],
            weights=[15, 28, 35, 22]
        )[0]
        complaints = random.choices([0,1,2,3,4,5], weights=[60,20,10,6,3,1])[0]
        drop_rate  = abs(random.normalvariate(2.5, 2.0))
        spend      = abs(random.normalvariate(45, 30))
        engagement = max(0, min(100, round(
            (max(0,10-days_inactive/9)/10*30) +
            (random.uniform(0,300)/300*30) +
            (random.uniform(0,100)/100*20) +
            (max(0,90-days_inactive)/90*20), 1
        )))
        risk_score = min(100, round(
            (min(days_inactive,90)/90*35) +
            (min(complaints,5)/5*25) +
            (min(drop_rate,10)/10*20) +
            ((1-engagement/100)*20), 1
        ))
        if risk_score >= 75:   tier = "CRITICAL"
        elif risk_score >= 50: tier = "HIGH"
        elif risk_score >= 25: tier = "MEDIUM"
        else:                  tier = "LOW"

        return {
            "event_id":        f"CHN-{str(self._counter).zfill(8)}",
            "subscriber_id":   f"SUB{str(random.randint(1,15000)).zfill(7)}",
            "timestamp":       datetime.now().isoformat(),
            "region":          random.choices(REGIONS, weights=[30,25,15,12,8,6,4])[0],
            "plan_type":       random.choices(PLANS,   weights=[30,25,20,15,10])[0],
            "days_inactive":   days_inactive,
            "num_complaints":  complaints,
            "monthly_spend":   round(spend, 2),
            "engagement_score": engagement,
            "churn_risk_score": risk_score,
            "churn_risk_tier": tier,
            "revenue_at_risk": round(spend * risk_score / 100, 2),
            "is_churned":      1 if random.random() < 0.18 else 0,
        }

    def run(self):
        self.logger.info(f"Producer started → '{self.topic.name}' at {self.rate_hz} events/sec")
        end_time   = time.time() + self.duration
        sleep_time = 1.0 / self.rate_hz
        while self.running and time.time() < end_time:
            self.topic.produce(self.generate_event())
            self.produced  += 1
            self._counter  += 1
            time.sleep(sleep_time)
        self.running = False
        self.logger.info(f"Producer finished — published {self.produced:,} events")


class RiskConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="RiskConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.alerts   = []
        self.logger   = logging.getLogger("RiskConsumer")

    def run(self):
        self.logger.info("Consumer started — monitoring partition 0 for Critical/High risk")
        while self.running:
            msg = self.topic.consume(0)
            if msg is None:
                continue
            if msg["churn_risk_tier"] in ("CRITICAL", "HIGH"):
                self.alerts.append(msg)
                if msg["churn_risk_tier"] == "CRITICAL":
                    self.logger.warning(
                        f"CRITICAL CHURN RISK | {msg['subscriber_id']} | "
                        f"Score: {msg['churn_risk_score']} | "
                        f"Revenue at risk: GHS {msg['revenue_at_risk']:.2f} | "
                        f"{msg['region']}"
                    )


class MetricsConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="MetricsConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.logger   = logging.getLogger("MetricsConsumer")
        self.metrics  = {
            "total": 0, "critical": 0, "high": 0,
            "medium": 0, "low": 0, "churned": 0,
            "total_revenue_at_risk": 0.0,
            "by_region": {}, "by_plan": {}
        }

    def run(self):
        self.logger.info("Consumer started — aggregating churn metrics from partition 1")
        while self.running:
            msg = self.topic.consume(1)
            if msg is None:
                continue
            m = self.metrics
            m["total"]  += 1
            m["churned"] += msg["is_churned"]
            m["total_revenue_at_risk"] += msg["revenue_at_risk"]
            tier = msg["churn_risk_tier"].lower()
            if tier in m:
                m[tier] += 1
            m["by_region"][msg["region"]] = \
                m["by_region"].get(msg["region"], 0) + msg["revenue_at_risk"]
            m["by_plan"][msg["plan_type"]] = \
                m["by_plan"].get(msg["plan_type"], 0) + 1

    def snapshot(self):
        m = self.metrics
        t = max(m["total"], 1)
        return {
            "total_events":         m["total"],
            "critical_count":       m["critical"],
            "high_count":           m["high"],
            "churn_rate_pct":       round(m["churned"] / t * 100, 1),
            "total_revenue_at_risk": round(m["total_revenue_at_risk"], 2),
            "top_region":           max(m["by_region"], key=m["by_region"].get, default="N/A"),
            "top_plan":             max(m["by_plan"],   key=m["by_plan"].get,   default="N/A"),
        }


class AlertConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="AlertConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.consumed = 0
        self.logger   = logging.getLogger("AlertConsumer")
        self.log_file = REPORTS_PATH / "churn_events_live.jsonl"

    def run(self):
        self.logger.info(f"Consumer started — logging all events to {self.log_file}")
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


def print_live_metrics(producer, metrics, risk, alert, interval=10):
    start = time.time()
    while producer.running:
        time.sleep(interval)
        elapsed  = int(time.time() - start)
        snap     = metrics.snapshot()
        print("\n" + "="*65)
        print(f"  CHURN STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Events Produced      : {producer.produced:,}")
        print(f"  Throughput           : {producer.produced/max(elapsed,1):.1f} events/sec")
        print(f"  Total Scored         : {snap['total_events']:,}")
        print(f"  Critical Risk        : {snap['critical_count']:,}")
        print(f"  High Risk            : {snap['high_count']:,}")
        print(f"  Churn Rate           : {snap['churn_rate_pct']}%")
        print(f"  Revenue at Risk      : GHS {snap['total_revenue_at_risk']:,.2f}")
        print(f"  Top Region           : {snap['top_region']}")
        print(f"  Top Plan             : {snap['top_plan']}")
        print(f"  Retention Alerts     : {len(risk.alerts):,}")
        print(f"  Events Logged        : {alert.consumed:,}")
        print("="*65)


def run_kafka_churn_simulator():
    print("\n" + "="*65)
    print("  MTN GHANA — CHURN RISK KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} events/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Expected       : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} events")
    print("="*65 + "\n")

    topic    = ChurnEventTopic(TOPIC_NAME, PARTITION_COUNT)
    producer = ChurnEventProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    risk     = RiskConsumer(topic)
    metrics  = MetricsConsumer(topic)
    alert    = AlertConsumer(topic)

    for t in [producer, risk, metrics, alert]:
        t.start()

    m_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics, risk, alert, 10),
        daemon=True
    )
    m_thread.start()
    producer.join()
    time.sleep(3)
    for t in [risk, metrics, alert]:
        t.running = False

    final = metrics.snapshot()
    print("\n" + "="*65)
    print("  CHURN KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Events Produced  : {producer.produced:,}")
    print(f"  Critical Risk Detected : {final['critical_count']:,}")
    print(f"  High Risk Detected     : {final['high_count']:,}")
    print(f"  Churn Rate Detected    : {final['churn_rate_pct']}%")
    print(f"  Total Revenue at Risk  : GHS {final['total_revenue_at_risk']:,.2f}")
    print(f"  Retention Alerts       : {len(risk.alerts):,}")
    print(f"  Top Risk Region        : {final['top_region']}")
    print(f"  Top Risk Plan          : {final['top_plan']}")
    print(f"  Events Logged          : {alert.consumed:,}")
    print("="*65 + "\n")

    if risk.alerts:
        import csv
        alerts_path = REPORTS_PATH / "churn_retention_alerts.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=risk.alerts[0].keys())
            writer.writeheader()
            writer.writerows(risk.alerts)
        print(f"  Retention alerts saved to: {alerts_path}")


if __name__ == "__main__":
    run_kafka_churn_simulator()