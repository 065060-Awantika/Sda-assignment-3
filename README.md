## Assignment 3: Dashboard for Analysis of Consumed Data
Tool: Grafana | Storage: PostgreSQL | Streaming: Apache Kafka

Flow: producer.py -> Kafka topics -> consumer.py -> PostgreSQL -> Grafana

How to run:
1. Start the Docker containers (Kafka, Postgres, Grafana)
2. python producer.py
3. python consumer.py
4. Open http://localhost:3000 and import the dashboard JSON from this repo

---

## 1. Objective

Assignment 1 identified the industry (E-Commerce / Online Retail), the four streaming
data sources (clickstream, cart, transaction, marketing touchpoints), and a flattened
GA4-style event schema. This assignment builds on that foundation by:

1. Generating a realistic sample dataset based strictly on the Assignment 1 schema.
2. Building a Python Kafka producer that streams that data to Kafka.
3. Demonstrating, with real terminal output, that messages are actually flowing
   through Kafka topics.

No new industry, schema, or data sources are introduced here — everything below is a
direct implementation of what Assignment 1 already specified.

## 2. Dataset Creation Methodology

The dataset is generated programmatically (`data/generate_data.py`) rather than
hand-written, so that it is large enough to demonstrate streaming and internally
consistent enough to support later analytics (Assignment 3).

Each generated **session** represents one customer journey, built around a single
`user_pseudo_id`, `ga_session_id`, product, device, and traffic source — so events
belonging to the same customer visit are traceable end-to-end, exactly as they would
be in real GA4 data. Three journey types are generated, matching the funnel stages
Assignment 1's analytics section (conversion rate, cart abandonment) depends on:

| Journey type | Sequence | Purpose |
|---|---|---|
| Converted | campaign → page_view → view_item → add_to_cart → begin_checkout → **purchase** | Feeds conversion rate |
| Cart abandonment | campaign → page_view → view_item → **add_to_cart** (no purchase) | Feeds cart-abandonment rate |
| Browse only | campaign → page_view → **view_item** (no cart) | Feeds engagement / bounce metrics |

75% of sessions arrive via a paid/marketing channel (and so emit a campaign
touchpoint event); the remainder are organic/direct traffic, which is realistic and
also gives Assignment 3 a genuine "campaign vs. organic" comparison to visualize.

Running the default configuration produces **189 events** across 45 sessions:

| Topic | Event count |
|---|---|
| pageview-events | 90 |
| cart-events | 50 |
| transaction-events | 17 |
| campaign-events | 32 |

## 3. Data Schema

Same flattened schema as Assignment 1, Section 3.3:

```
event_timestamp, event_name, user_pseudo_id, ga_session_id,
item_id, item_name, item_category, price, quantity,
traffic_source.source, traffic_source.medium, traffic_source.name,
device.category, device.operating_system,
geo.country, geo.city
```

Purchase events additionally carry `order_id` and `transaction_value`.

**Modeling note on the marketing touchpoint:** GA4's native event model does not have
a distinct "campaign event" — traffic-source attribution normally rides on every
event in a session. To honor Assignment 1's 4-topic architecture, this project
represents the marketing touchpoint as a `campaign_click` event fired at the *start*
of any session that arrived via a paid channel, carrying the `traffic_source` fields.
This is a simplification made for the assignment's demo purposes, not a change to the
underlying schema.

## 4. Sample Data

Example event of each type (from the generated dataset):

```json
{"event_timestamp": "2020-11-01T21:42:23Z", "event_name": "campaign_click", "user_pseudo_id": "u_4910.20201118", "ga_session_id": "SESS80022", "traffic_source": {"source": "google", "medium": "cpc", "name": "SUMMER_SALE"}, "device": {"category": "desktop", "operating_system": "macOS"}, "geo": {"country": "India", "city": "Mumbai"}}
```

```json
{"event_timestamp": "2020-11-01T21:43:46Z", "event_name": "view_item", "user_pseudo_id": "u_4910.20201118", "ga_session_id": "SESS80022", "traffic_source": {"source": "google", "medium": "cpc", "name": "SUMMER_SALE"}, "device": {"category": "desktop", "operating_system": "macOS"}, "geo": {"country": "India", "city": "Mumbai"}, "item_id": "GGOEGOAQ0221", "item_name": "Google Classic Hoodie", "item_category": "Apparel", "price": 1799.0, "quantity": 1}
```

Full dataset: `data/sample_ecommerce_events.jsonl` (189 events, chronological).
Same events pre-split by topic: `sample_events/*.jsonl`.

## 5. Kafka Topic Configuration

Four topics, matching Assignment 1's architecture, each with 1 partition / replication
factor 1 (sufficient for a local single-broker demo):

- `pageview-events`
- `cart-events`
- `transaction-events`
- `campaign-events`

Created via `create_topics.bat` (see Section 7).

## 6. Producer Implementation

`producer.py` uses `kafka-python`. For each event it:

1. Reads the next line from the sample JSONL file.
2. Looks up the correct topic from `event_name` (e.g. `purchase` → `transaction-events`).
3. Serializes the event as JSON.
4. Sends it to Kafka, keyed by `ga_session_id`.
5. Prints the topic, partition, offset, event type, and user — so the terminal output
   itself proves delivery.
6. Flushes and closes the producer cleanly on completion or interruption.
7. Exits with a clear message if the broker is unreachable, instead of hanging silently.

**Why `ga_session_id` as the message key:** Kafka guarantees ordering only within a
partition, and messages with the same key always land on the same partition. Keying by
session ID means every event belonging to one customer's browsing session is
delivered to consumers **in order**, which is exactly what session-level aggregation
(conversion rate, cart abandonment) in Assignment 3 will need. It's a simple key that
doesn't require any custom partitioning logic.

## 7. Data Flow — How to Run Everything (Windows/PowerShell)

**Prerequisites:** Kafka and ZooKeeper (or KRaft) already installed, per your course
setup. Python 3.8+.

### Step 0 — Install the Python dependency
```powershell
cd Assignment_2
pip install -r requirements.txt
```

### Step 1 — (Re)generate the sample data (optional — already included)
```powershell
python data\generate_data.py --sessions 45
```

### Terminal 1 — Start ZooKeeper (skip if using KRaft-only Kafka)
```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

### Terminal 2 — Start Kafka broker
```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### Step 2 — Create the topics (run once)
```powershell
cd Assignment_2
create_topics.bat
```

### Terminal 3 — Start a console consumer (do this BEFORE running the producer, so you can watch messages arrive live)
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic pageview-events --from-beginning --property print.key=true
```
Open additional consumer windows for the other topics if you want to watch all four
at once (`cart-events`, `transaction-events`, `campaign-events`).

### Terminal 4 — Run the producer
```powershell
cd Assignment_2
python producer.py
```
Or limit how many events to send:
```powershell
python producer.py --events 50
python producer.py --events 100 --delay 0.5
```

You should see messages print in Terminal 4 (producer confirmations) **and**
simultaneously appear in Terminal 3 (consumer output) — that side-by-side view is
the proof of streaming.

## 8. Testing / Message Flow Verification

To confirm the pipeline is genuinely working, check:

- [ ] `create_topics.bat` lists all 4 topics with no errors.
- [ ] Producer terminal shows `[SENT] #N -> topic=... partition=... offset=...` lines
      for each event, with no `[ERROR]` lines.
- [ ] Consumer terminal (running `--from-beginning`) shows the same JSON events
      appearing, with keys matching `ga_session_id` values from the producer log.
- [ ] Events appear in the consumer window **while** the producer is still running
      (not just after it finishes) — this is what proves real streaming rather than a
      batch file dump.
- [ ] Switching `--topic` in the consumer command shows different event types in each
      of the four topics (e.g. `purchase` only ever appears under `transaction-events`).

## 9. Screenshot for Submission

Take **one clean screenshot showing three terminals side by side**:

1. **Left/top: the producer terminal**, mid-run, showing several `[SENT]` lines with
   visible topic names and offsets.
2. **Right/bottom: a consumer terminal** for `pageview-events` (or another busy
   topic), showing the raw JSON messages that just arrived, with the topic name
   visible in the command itself.
3. Make sure at least one full JSON line is readable in the screenshot (not cut off),
   so the grader can see real fields (`event_name`, `user_pseudo_id`, `traffic_source`,
   etc.) rather than just a wall of text.

Tips for a clean layout: resize both PowerShell windows to roughly half-screen each,
run the consumer first so it's idle and ready, then start the producer and screenshot
a few seconds in once messages are visibly flowing in both windows.

*(No screenshot is included in this package — take it yourself while running the
commands above, as instructed.)*

## 10. Conclusion

This assignment implements the pipeline's first two stages from Assignment 1's
architecture diagram: a Python producer generating and publishing GA4-schema events,
and Apache Kafka distributing them across four topics matching the four identified
data sources. The dataset is realistic and session-consistent, supporting the
conversion-rate and cart-abandonment metrics Assignment 1 targets. The next stage
(Assignment 3) will add a consumer/processor that aggregates these streamed events
and writes results to the SQL sink already set up in the course, feeding the
dashboard layer.

---

## Appendix — Talking Points for Viva (Part 9)

**Why is the sample data based on Assignment 1?**
The brief requires continuity — A1 already committed to a specific industry, schema,
and topic design (grounded in the real, Google-published GA4 sample dataset), so A2's
job is to *implement* that design, not invent a new one.

**Why JSON?**
GA4 itself exports events in a JSON/nested format; JSON is also Kafka's most common
serialization for semi-structured event data and is trivial to produce and consume
without a schema registry — appropriate for a course assignment's scope.

**Why is Kafka appropriate here?**
The business problem (A1, Section 2.1) is specifically about *not* waiting for
end-of-day reports. Kafka's publish-subscribe model lets events be published the
moment they happen and consumed independently by however many downstream processors
need them, without the producer knowing or caring who's listening.

**Why separate topics per event category?**
It mirrors how the four data sources were identified as logically distinct in A1, and
it lets a consumer subscribe only to what it needs (e.g., a cart-abandonment monitor
only needs `cart-events`, not `transaction-events`) instead of filtering one firehose
topic.

**Why keep `ga_session_id` as the key?**
It keeps a session's events ordered on one partition, which matters once Assignment 3
aggregates per-session behaviour (did this session convert? did it abandon a cart?).

**How does the producer work, in one sentence?**
It reads each sample event, decides its topic from `event_name`, serializes it to
JSON, and sends it to Kafka keyed by session ID — printing confirmation of exactly
where each message landed.

**How do I prove data is streaming, not just stored?**
Run a consumer with `--from-beginning` *before* starting the producer, and show it
receiving messages live, terminal-to-terminal, as the producer sends them — not
reading from a pre-existing file.

**How does this connect to Assignment 3?**
A3 adds a consumer that reads from these same four topics, computes the KPIs A1
Section 5 defined (conversion rate, cart abandonment, campaign performance, etc.), and
writes the results into the SQL database already set up in the course, ready for the
dashboard layer.
