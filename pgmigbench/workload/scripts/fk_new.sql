\set id random(1, __MAX_ID__)
SELECT p.payload
FROM bench.child c
LEFT JOIN bench.parent p ON p.id = c.parent_id
WHERE c.id = :id;
UPDATE bench.child
SET payload = md5((payload || :id)::text)
WHERE id = :id;
