BEGIN;
    ALTER TABLE gmaps_results DROP COLUMN latitude;
    ALTER TABLE gmaps_results DROP COLUMN longitude;
COMMIT;
