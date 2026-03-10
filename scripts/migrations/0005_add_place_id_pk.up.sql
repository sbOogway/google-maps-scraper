BEGIN;

    ALTER TABLE gmaps_results DROP CONSTRAINT results_pkey;
    ALTER TABLE gmaps_results ADD PRIMARY KEY (place_id);

COMMIT;