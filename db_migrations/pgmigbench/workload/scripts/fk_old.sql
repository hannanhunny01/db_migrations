\set id random(1, __MAX_ID__)
SELECT c.parent_id
FROM bench.child c
WHERE c.id = :id;
UPDATE bench.child
SET payload = md5((payload || :id)::text)
WHERE id = :id;
