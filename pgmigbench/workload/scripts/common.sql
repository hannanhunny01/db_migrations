\set id random(1, __MAX_ID__)
SELECT payload FROM bench.child WHERE id = :id;
