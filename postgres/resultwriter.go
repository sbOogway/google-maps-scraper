package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	// "encoding/json"
	"errors"
	// "fmt"

	// "os"
	// "strings"
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

// func buildBulkInsert[T any](baseQuery string, entries []T, fieldsPerEntry int, extractor func(T) []interface{}) (string, []interface{}) {
// 	placeholders := make([]string, 0, len(entries))
// 	args := make([]interface{}, 0, len(entries)*fieldsPerEntry)

// 	for i := 0; i < len(entries); i++ {
// 		// Genera ($1, $2, $3...) per ogni riga
// 		rowPlaceholders := make([]string, fieldsPerEntry)
// 		for j := 0; j < fieldsPerEntry; j++ {
// 			rowPlaceholders[j] = fmt.Sprintf("$%d", i*fieldsPerEntry+j+1)
// 		}
// 		placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ", ")+")")

// 		// Estrae i campi dall'oggetto
// 		args = append(args, extractor(entries[i])...)
// 	}

// 	fullQuery := baseQuery + " VALUES " + strings.Join(placeholders, ", ")
// 	return fullQuery, args
// }

// func BuildBulkInsert[T any](
// 	tableName string,
// 	columns []string,
// 	entries T,
// 	extractor func(T) []interface{},
// ) (string, []interface{}) {

// 	numCols := len(columns)
// 	placeholders := make([]string, 0, len(entries))
// 	args := make([]interface{}, 0, len(entries)*numCols)

// 	for i, entry := range entries {
// 		// Crea i placeholder per ogni riga: ($1, $2, $3...)
// 		rowPlaceholders := make([]string, numCols)
// 		for j := 0; j < numCols; j++ {
// 			rowPlaceholders[j] = fmt.Sprintf("$%d", i*numCols+j+1)
// 		}
// 		placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ", ")+")")

// 		// Estrae i valori dall'oggetto tramite la callback
// 		args = append(args, extractor(entry)...)
// 	}

// 	// Costruisce la query finale: INSERT INTO table (col1, col2) VALUES ($1, $2), ($3, $4)...
// 	query := fmt.Sprintf(
// 		"INSERT INTO %s (%s) VALUES %s",
// 		tableName,
// 		strings.Join(columns, ", "),
// 		strings.Join(placeholders, ", "),
// 	)

// 	return query, args
// }

func dirtyMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func singleSave(ctx context.Context, db *sql.DB, entry *gmaps.Entry) error {
	log.Printf("singleSave: start saving entry title=%q place_id=%q\n", entry.Title, entry.PlaceID)

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
	log.Printf("singleSave: inserting business name=%q place_id=%q\n", entry.Title, entry.PlaceID)
	err = tx.QueryRowContext(ctx, qb, argsqb...).Scan(&id)
	if err == sql.ErrNoRows {
		log.Printf("singleSave: business already scraped, skipping entry title=%q place_id=%q\n", entry.Title, entry.PlaceID)
		return err
	} else if err != nil {
		log.Printf("singleSave: error inserting business for place_id=%q: %v\n", entry.PlaceID, err)
		return err
	}
	log.Printf("singleSave: business insert returned id=%q for place_id=%q\n", id, entry.PlaceID)

	qcon := `insert into contacts (business_id, phone_number, email, address, website)
	values ($1, $2, $3, $4, $5)`
	argsqcon := []any{id, dirtyMarshal(entry.Phone), dirtyMarshal(entry.Emails), dirtyMarshal(entry.Address), entry.WebSite}

	qcat := `insert into categories (business_id, category, subcategory)
	values ($1, $2, $3)`
	argsqcat := []any{id, dirtyMarshal(entry.Category), dirtyMarshal(entry.Categories)}

	// gmaps.id is the Google Maps place ID, not the business UUID
	qg := `insert into gmaps (id, price_range, popular_times, review_rating, review_count, owner_id)
	values ($1, $2, $3, $4, $5, $6)`
	argsqg := []any{entry.PlaceID, entry.PriceRange, dirtyMarshal(entry.PopularTimes), entry.ReviewRating, entry.ReviewCount, entry.Owner.ID}

	log.Printf("singleSave: inserting contacts for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qcon, argsqcon...)
	if err != nil {
		log.Printf("singleSave: error inserting contacts for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	log.Printf("singleSave: inserting categories for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qcat, argsqcat...)
	if err != nil {
		log.Printf("singleSave: error inserting categories for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	log.Printf("singleSave: inserting gmaps row for business_id=%q place_id=%q\n", id, entry.PlaceID)
	_, err = tx.ExecContext(ctx, qg, argsqg...)
	if err != nil {
		log.Printf("singleSave: error inserting gmaps row for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		log.Printf("singleSave: gmaps row: %v\n", argsqg)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("singleSave: error committing transaction for business_id=%q place_id=%q: %v\n", id, entry.PlaceID, err)
		return err
	}

	log.Printf("singleSave: successfully saved entry title=%q place_id=%q business_id=%q\n", entry.Title, entry.PlaceID, id)

	return err
}

func (r *resultWriter) batchSave(ctx context.Context, entries []*gmaps.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		singleSave(ctx, r.db, entry)
		// if err != nil {
		// 	return err
		// }
	}	

	return nil

	// q := `INSERT INTO results
	// 	(place_id, data)
	// 	VALUES
	// 	`
	// elements := make([]string, 0, len(entries))
	// args := make([]interface{}, 0, len(entries))

	// for i, entry := range entries {
	// 	data, err := json.Marshal(entry)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	elements = append(elements, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
	// 	args = append(args, entry.PlaceID, data)
	// }

	// q += strings.Join(elements, ", ")
	// q += " ON CONFLICT DO NOTHING"

	// q2 := `INSERT INTO businesses (name, gmaps_id) VALUES `
	// elements2 := make([]string, 0, len(entries))
	// args2 := make([]interface{}, 0, len(entries))

	// for i, entry := range entries {
	// 	elements2 = append(elements2, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
	// 	args2 = append(args2, entry.Title, entry.PlaceID)
	// }

	// q2 += strings.Join(elements2, ", ")
	// q2 += " ON CONFLICT DO NOTHING"

	// fmt.Println("=========================")
	// fmt.Println("=========================")
	// fmt.Println("=========================")
	// fmt.Println("=========================")
	// fmt.Println("=========================")
	// fmt.Println("=========================")

	// colsBQ := []string{"name", "gmaps_id"}
	// qBQ, argsBQ := BuildBulkInsert("businesses", colsBQ, entries, func(e *gmaps.Entry) []interface{} {
	// 	return []interface{}{e.Title, e.PlaceID}
	// })
	// qBQ += "ON CONFLICT (gmaps_id) DO UPDATE SET name = EXCLUDED.name RETURNING id"

	// tx, err := r.db.BeginTx(ctx, nil)
	// if err != nil {
	// 	return err
	// }

	// defer func() {
	// 	_ = tx.Rollback()
	// }()

	// var rows *sql.Rows
	// rows, err = tx.QueryContext(ctx, qBQ, argsBQ...)
	// if err != nil {
	// 	log.Println("business already scraped skipping")
	// 	fmt.Printf("error bq %s\n", err)
	// 	return err
	// }
	// defer rows.Close()

	// placeToUUID := make(map[string]string)

	// i := 0
	// for rows.Next() {
	// 	var newID string
	// 	if err := rows.Scan(&newID); err != nil {
	// 		return err
	// 	}
	// 	// IMPORTANTE: Poiché l'ordine di RETURNING segue l'ordine di INSERT,
	// 	// associamo l'ID all'entry corretta usando l'indice o il PlaceID
	// 	placeToUUID[entries[i].PlaceID] = newID
	// 	i++
	// }

	// // log.Printf("id returned from db %s\n", id)

	// colsCQ := []string{"business_id", "phone_number", "email", "address", "website"}
	// qCQ, argsCQ := BuildBulkInsert("contacts", colsCQ, entries, func(e *gmaps.Entry) []interface{} {
	// 	businessID := placeToUUID[e.PlaceID]
	// 	return []interface{}{businessID, dirtyMarshal(e.Phone), dirtyMarshal(e.Emails), dirtyMarshal(e.Address), e.WebSite}
	// })

	// colsCatQ := []string{"business_id", "category", "subcategory"}
	// qCatQ, argsCatQ := BuildBulkInsert("categories", colsCatQ, entries, func(e *gmaps.Entry) []interface{} {
	// 	businessID := placeToUUID[e.PlaceID]
	// 	return []interface{}{businessID, dirtyMarshal(e.Category), dirtyMarshal(e.Categories)}
	// })

	// colsGQ := []string{"id", "price_range", "popular_times", "review_rating", "review_count", "owner_id"}
	// qGQ, argsGQ := BuildBulkInsert("gmaps", colsGQ, entries, func(e *gmaps.Entry) []interface{} {
	// 	return []interface{}{e.PlaceID, e.PriceRange, dirtyMarshal(e.PopularTimes), e.ReviewRating, e.ReviewCount, e.Owner.ID}
	// })

	// _, err = tx.ExecContext(ctx, qCQ, argsCQ...)
	// if err != nil {
	// 	fmt.Printf("error cq %s\n", err)
	// 	fmt.Printf("%s %v", qCQ, argsCQ)
	// 	return err
	// }

	// _, err = tx.ExecContext(ctx, qCatQ, argsCatQ...)
	// if err != nil {
	// 	fmt.Printf("error catq %s\n", err)
	// 	fmt.Printf("%s %v", qCatQ, argsCatQ)
	// 	return err
	// }

	// _, err = tx.ExecContext(ctx, qGQ, argsGQ...)
	// if err != nil {
	// 	fmt.Printf("error gq %s\n", err)
	// 	fmt.Printf("%s %v", qGQ, argsGQ)
	// 	return err
	// }

	// err = tx.Commit()
	// fmt.Printf("error tx %s\n", err)

	// return err
}
