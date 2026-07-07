package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() { var _ *pgxpool.Pool; fmt.Println("pgx links") }
