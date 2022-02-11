create keysapce fm;

CREATE TABLE fm.alarm_dump (
                             domain text,
                             vendor text,
                             creation_date date,
                             technology text,
                             unique_identifier text,
                             reporting_time timestamp,
                             dump_process_time timestamp,
                             sender_name text,
                             sender_ip text,
                             rca text,
                             rca_id text,
                             raw_trap text,
                             alarm_external_id text,
                             alarm_name text,
                             description text,
                             subentity text,
                             event_time timestamp,
                             entity_id text,
                             entity_status text,
                             severity text,
                             l1_process_time timestamp,
                             rowkey text,
                             PRIMARY KEY ((domain, vendor, creation_date), technology, unique_identifier)
  ) WITH CLUSTERING ORDER BY (technology DESC, unique_identifier ASC)
AND default_time_to_live = 1728000
AND transactions = {'enabled': 'true'};
