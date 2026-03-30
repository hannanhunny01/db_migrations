\set id random(1, __MAX_ID__)
SELECT customer_tier FROM bench.child WHERE id = :id;
UPDATE bench.child
SET payload = md5((payload || :id)::text),
    customer_tier = COALESCE(customer_tier, 'basic')
WHERE id = :id;
