WITH events AS (
 SELECT account_event.account_id,
        event.dst
   FROM {shard}.event_comment AS event
  INNER JOIN {shard}.account_event AS account_event
     ON account_event.event_id = event.id
  WHERE account_event.account_id <> event.dst
    AND account_event.event = 'event_comment'
    AND event.dst LIKE $1
  UNION
 SELECT account_event.account_id,
        event.dst
   FROM {shard}.event_share AS event
  INNER JOIN {shard}.account_event AS account_event
     ON account_event.event_id = event.id
  WHERE account_event.account_id <> event.dst
    AND account_event.event = 'event_share'
    AND event.dst LIKE $1
  UNION
 SELECT account_event.account_id,
        event.dst
   FROM {shard}.event_mention AS event
  INNER JOIN {shard}.account_event AS account_event
     ON account_event.event_id = event.id 
  WHERE account_event.account_id <> event.dst
    AND account_event.event = 'event_metion'
    AND event.dst LIKE $1
  )
 SELECT events.account_id,
        event_rev.src AS friend_id 
   FROM events AS events
  INNER JOIN {shard}.account_event AS account_event_rev
     ON account_event_rev.account_id = events.account_id
  INNER JOIN {shard}.event_comment_rev AS event_rev
     ON event_rev.id = account_event_rev.event_id
    AND event_rev.src = events.dst
  WHERE account_event_rev.account_id <> event_rev.src
    AND account_event_rev.event = 'event_comment_rev'
    AND event_rev.src LIKE $1
  UNION
 SELECT events.account_id,
        event_rev.src AS friend_id 
   FROM events AS events
  INNER JOIN {shard}.account_event AS account_event_rev
     ON account_event_rev.account_id = events.account_id
  INNER JOIN {shard}.event_share_rev AS event_rev
     ON event_rev.id = account_event_rev.event_id
    AND event_rev.src = events.dst
  WHERE account_event_rev.account_id <> event_rev.src
    AND account_event_rev.event = 'event_share_rev'
    AND event_rev.src LIKE $1
  UNION
 SELECT events.account_id,
        event_rev.src AS friend_id 
   FROM events AS events
  INNER JOIN {shard}.account_event AS account_event_rev
     ON account_event_rev.account_id = events.account_id
  INNER JOIN {shard}.event_mention_rev AS event_rev
     ON event_rev.id = account_event_rev.event_id
    AND event_rev.src = events.dst
  WHERE account_event_rev.account_id <> event_rev.src
    AND account_event_rev.event = 'event_mention_rev'
    AND event_rev.src LIKE $1
