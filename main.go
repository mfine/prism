package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lib/pq"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	limit  = flag.Int("limit", 1000, "Query Limit")
	setup  = flag.Bool("setup", false, "Setup Database")
	insert = flag.Bool("insert", false, "Insert Worker")
	update = flag.Bool("update", false, "Update Worker")
	dbUrl  = mustGetenv("DATABASE_URL")
	db     = dbOpen()
	urlRe  = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
	org    = mustGetenv("GITHUB_ORG")
	auth   = "token " + mustGetenv("GITHUB_OAUTH_TOKEN")
	wg     sync.WaitGroup
)

type handler func(rc io.ReadCloser)

func nextUrl(hdr http.Header) string {
	for _, link := range hdr["Link"] {
		urls := strings.Split(link, ",")
		for _, url := range urls {
			ms := urlRe.FindStringSubmatch(url)
			if len(ms) == 3 && ms[2] == "next" {
				return ms[1]
			}
		}
	}

	return ""
}

func rateLimit(hdr http.Header) (int, int) {
	remaining, err := strconv.Atoi(hdr["X-Ratelimit-Remaining"][0])
	if err != nil {
		log.Fatal(err)
	}

	reset, err := strconv.Atoi(hdr["X-Ratelimit-Reset"][0])
	if err != nil {
		log.Fatal(err)
	}

	return remaining, reset
}

func request(url string, h handler) {
	for url != "" {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", auth)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		if remaining, reset := rateLimit(resp.Header); remaining == 0 {
			log.Printf("reset=%s\n", time.Unix(int64(reset), 0))
			time.Sleep(time.Unix(int64(reset), 0).Sub(time.Now()))
			continue
		}

		h(resp.Body)

		url = nextUrl(resp.Header)
	}
}

func shas(id string) handler {
	return func(rc io.ReadCloser) {
		defer rc.Close()

		var result struct {
			Commit struct {
				Message string
				Author  struct {
					Email string
					Date  string
				}
			}
			Stats struct {
				Additions int
				Deletions int
				Total     int
			}
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Fatal(err)
		}

		dbUpdate(id,
			result.Commit.Author.Email,
			result.Commit.Author.Date,
			result.Commit.Message,
			result.Stats.Additions,
			result.Stats.Deletions,
			result.Stats.Total)
	}
}

func dbQuery() {
	for {
		rows, err := db.Query("SELECT id, repo, sha FROM commits WHERE total IS NULL LIMIT $1", *limit)
		if err != nil {
			log.Fatal(err)
		}

		if !rows.Next() {
			break
		}

		for rows.Next() {
			var id, repo, sha string
			if err := rows.Scan(&id, &repo, &sha); err != nil {
				log.Fatal(err)
			}

			wg.Add(1)
			go func(id, repo, sha string) {
				defer wg.Done()
				log.Printf("repo=%s sha=%s\n", repo, sha)
				url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
				request(url, shas(id))
			}(id, repo, sha)
		}
	}
}

func dbFind(repo, sha string) bool {
	rows, err := db.Query("SELECT id FROM commits WHERE repo = $1 AND sha = $2", repo, sha)
	if err != nil {
		log.Fatal(err)
	}

	return rows.Next()
}

func dbCreate(repo, sha string) {
	if _, err := db.Query("INSERT INTO commits (repo, sha) VALUES ($1, $2)", repo, sha); err != nil {
		log.Fatal(err)
	}
}

func dbUpdate(id, email, date, message string, additions, deletions, total int) {
	if _, err := db.Query("UPDATE commits SET email=$2, date=$3, msg=$4, adds=$5, dels=$6, total=$7 WHERE id=$1", id, email, date, message, additions, deletions, total); err != nil {
		log.Fatal(err)
	}
}

func commits(repo string) handler {
	return func(rc io.ReadCloser) {
		defer rc.Close()

		var result []struct {
			Sha string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Fatal(err)
		}

		for _, c := range result {
			func(repo, sha string) {
				log.Printf("repo=%s sha=%s\n", repo, sha)
				if !dbFind(repo, sha) {
					dbCreate(repo, sha)
				}
			}(repo, c.Sha)
		}
	}
}

func repos() handler {
	return func(rc io.ReadCloser) {
		defer rc.Close()

		var result []struct {
			Name string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Fatal(err)
		}

		for _, r := range result {
			wg.Add(1)
			go func(repo string) {
				defer wg.Done()
				log.Printf("repo=%s\n", repo)
				url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits", org, repo)
				request(url, commits(repo))
			}(r.Name)
		}
	}
}

func dbSetup() {
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=gitz ")

	flag.Parse()

	if *setup {
		dbSetup()
	}

	if *insert {
		wg.Add(1)
		go func() {
			defer wg.Done()
			url := fmt.Sprintf("https://api.github.com/orgs/%s/repos", org)
			request(url, repos())
		}()
	}

	if *update {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dbQuery()
		}()
	}

	wg.Wait()
}

func dbOpen() (db *sql.DB) {
	name, err := pq.ParseURL(dbUrl)
	if err != nil {
		log.Fatal(err)
	}

	db, err = sql.Open("postgres", name)
	if err != nil {
		log.Fatal(err)
	}

	return
}

func mustGetenv(key string) (value string) {
	if value = os.Getenv(key); value == "" {
		log.Fatalf("%s not set", key)
	}

	return
}
