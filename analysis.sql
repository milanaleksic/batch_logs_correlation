delete
from batch
-- remove nicely processed items
where id in (select id
             from batch
             group by id
             having count(*) = 1)
   -- remove items we can't match with logs
   or created < (select min(ts) from log);

select count(*) from log;

select count(*) from batch;

select cnt, count(*)
from (select id, count(*) cnt
      from batch
      group by id)
group by cnt;

-- match duplicate causes
drop view if exists matched;

create view matched (extenalId, id, status, statusReason, created, sec_diff, service, thread) as
select externalId,
       id,
       status,
       statusReason,
       created                                                                                              sec_diff,
       86400 * (julianday(created) - julianday(lag(created) over win))                                      sec_diff,
       (select service
        from log
        where log.id = id
          and abs(86400 * (julianday(ts) - julianday(created))) < 1)                                        service,
       (select thread from log where log.id = id and abs(86400 * (julianday(ts) - julianday(created))) < 1) thread
from batch
    WINDOW win AS (partition by id ORDER BY created);

select *
from matched;

select status, statusReason, count(*)
from matched
group by status, statusReason;

select thread, count(*)
from matched
where sec_diff is not null
group by thread;
