BEGIN;

    ALTER TABLE results DROP CONSTRAINT results_pkey;
    ALTER TABLE results ADD PRIMARY KEY (place_id);

COMMIT;