  WITH events AS (
SELECT account_event.event,
       account_event.account_id,
       event.dst,
       count(*) AS events_count
  FROM {shard}.event_comment AS event
 INNER JOIN {shard}.account_event AS account_event
    ON account_event.event_id = event.id
 WHERE account_event.account_id <> event.dst
   AND account_event.event = 'event_comment'
   AND account_event.account_id LIKE $1
   AND event.dst LIKE $1
 GROUP BY account_event.event,
       account_event.account_id,
       event.dst
 UNION ALL
SELECT account_event.event,
       account_event.account_id,
       event.dst,
       count(*)
  FROM {shard}.event_share AS event
 INNER JOIN {shard}.account_event AS account_event
    ON account_event.event_id = event.id
 WHERE account_event.account_id <> event.dst
   AND account_event.event = 'event_share'
   AND account_event.account_id LIKE $1
   AND event.dst LIKE $1
 GROUP BY account_event.event,
       account_event.account_id,
       event.dst
 UNION ALL
SELECT account_event.event,
       account_event.account_id,
       event.dst,
       count(*)
  FROM {shard}.event_mention AS event
 INNER JOIN {shard}.account_event AS account_event
    ON account_event.event_id = event.id 
 WHERE account_event.account_id <> event.dst
   AND account_event.event = 'event_metion'
   AND account_event.account_id LIKE $1
   AND event.dst LIKE $1
 GROUP BY account_event.event,
       account_event.account_id,
       event.dst
), events_counts AS (
SELECT events.account_id, events.dst, sum(events.events_count) AS events_count
  FROM events
 GROUP BY events.account_id, events.dst
), events_rows AS (
SELECT account_id, dst, events_count, 
       row_number() OVER (PARTITION BY account_id ORDER BY events_count) AS rownum
  FROM events_counts
)
SELECT account_id, dst
  FROM events_rows
 WHERE rownum <= 150
