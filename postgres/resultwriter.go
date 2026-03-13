package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	// "log"
	"errors"
	"time"

	"github.com/gosom/scrapemate"

	"github.com/gosom/google-maps-scraper/gmaps"
)

func NewResultWriter(db *sql.DB) scrapemate.ResultWriter {
	return &resultWriter{db: db}
}

type resultWriter struct {
	db *sql.DB
}

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	const maxBatchSize = 50

	buff := make([]*gmaps.Entry, 0, 50)
	lastSave := time.Now().UTC()

	for result := range in {
		entry, ok := result.Data.(*gmaps.Entry)

		if !ok {
			return errors.New("invalid data type")
		}

		buff = append(buff, entry)

		if len(buff) >= maxBatchSize || time.Now().UTC().Sub(lastSave) >= time.Minute {
			err := r.batchSave(ctx, buff)
			if err != nil {
				return err
			}

			buff = buff[:0]
		}
	}

	if len(buff) > 0 {
		err := r.batchSave(ctx, buff)
		if err != nil {
			return err
		}
	}

	return nil
}


func dirtyMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func singleSave(ctx context.Context, db *sql.DB, entry *gmaps.Entry) error {
	// log.Printf("singleSave: start saving entry title=%q place_id=%q\n", entry.Title, entry.PlaceID)

	qb := `insert into businesses (name, gmaps_id) values 
	($1, $2) on conflict do nothing returning id`
	argsqb := []any{entry.Title, entry.PlaceID}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var id string
	// log.Printf("singleSave: inserting business name=%q place_id=%q\n", entry.Title, entry.PlaceID)
	err = tx.QueryRowContext(ctx, qb, argsqb...).Scan(&id)
	if err == sql.ErrNoRows {
		// log.Printf("singleSave: business already scraped, skipping entry title=%q place_id=%q\n", entry.Title, entry.PlaceID)
		return err
	} else if err != nil {
		// log.Printf("singleSave: error inserting business for place_id=%q: %v\n", entry.PlaceID, err)
		return err
	}
	// log.Printf("singleSave: business insert returned id=%q for place_id=%q\n", id, entry.PlaceID)

	qcon := `insert into contacts (business_id, phone_number, email, address, website)
	values ($1, $2, $3, $4, $5)`
	argsqcon := []any{id, dirtyMarshal(entry.Phone), dirtyMarshal(entry.Emails), dirtyMarshal(entry.Address), entry.WebSite}

	qcat := `insert into categories (business_id, category, subcategory)
	values ($1, $2, $3)`
	argsqcat := []any{id, entry.Category, dirtyMarshal(entry.Categories)}

	// gmaps.id is the Google Maps place ID, not the business UUID
	qg := `insert into gmaps (id, price_range, popular_times, review_rating, review_count, owner_id, latitude, longitude, link)
	values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	argsqg := []any{entry.PlaceID, entry.PriceRange, dirtyMarshal(entry.PopularTimes), entry.ReviewRating, entry.ReviewCount, entry.Owner.ID, entry.Latitude, entry.Longtitude, entry.Link}

	// log.Printf("singleSave: inserting contacts for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qcon, argsqcon...)
	if err != nil {
		// log.Printf("singleSave: error inserting contacts for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	// log.Printf("singleSave: inserting categories for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qcat, argsqcat...)
	if err != nil {
		// log.Printf("singleSave: error inserting categories for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	// log.Printf("singleSave: inserting gmaps row for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qg, argsqg...)
	if err != nil {
		// log.Printf("singleSave: error inserting gmaps row for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		// log.Printf("singleSave: gmaps row: %v\n", argsqg)
		return err
	}

	err = tx.Commit()
	if err != nil {
		// log.Printf("singleSave: error committing transaction for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	// log.Printf("singleSave: successfully saved entry title=%q place_id=%q business_id=%q\n", entry.Title, entry.PlaceID, id)

	return err
}

func (r *resultWriter) batchSave(ctx context.Context, entries []*gmaps.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		singleSave(ctx, r.db, entry)
	}	

	return nil

}
