\set id random(1, __MAX_ID__)
SELECT legacy_col FROM bench.child WHERE id = :id;
UPDATE bench.child SET payload = md5((payload || :id)::text) WHERE id = :id;
