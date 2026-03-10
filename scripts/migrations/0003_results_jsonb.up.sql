BEGIN;
    ALTER TABLE gmaps_results DROP COLUMN title;
    ALTER TABLE gmaps_results DROP COLUMN category;
    ALTER TABLE gmaps_results DROP COLUMN address;
    ALTER TABLE gmaps_results DROP COLUMN openhours;
    ALTER TABLE gmaps_results DROP COLUMN website;
    ALTER TABLE gmaps_results DROP COLUMN phone;
    ALTER TABLE gmaps_results DROP COLUMN pluscode;
    ALTER TABLE gmaps_results DROP COLUMN review_count;
    ALTER TABLE gmaps_results DROP COLUMN rating;
    ALTER TABLE gmaps_results DROP COLUMN latitude; 
    ALTER TABLE gmaps_results DROP COLUMN longitude;

    ALTER TABLE gmaps_results 
        ADD COLUMN data JSONB NOT NULL;
    
    ALTER TABLE gmaps_results 
        ADD COLUMN place_id TEXT NOT NULL;

COMMIT;
