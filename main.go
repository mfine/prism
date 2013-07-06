package main

import (
	"code.google.com/p/go-uuid/uuid"
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
	limit     = flag.Int("limit", 1000, "Query Limit")
	insert  = flag.Bool("insert", false, "Insert Worker")
	update  = flag.Bool("update", false, "Update Worker")
	dbUrl = mustGetenv("DATABASE_URL")
	db    = dbOpen()
	urlRe = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
	org   = mustGetenv("GITHUB_ORG")
	auth  = "token " + mustGetenv("GITHUB_OAUTH_TOKEN")
	wg    sync.WaitGroup
)

type handler func(rc io.ReadCloser) error

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

func rateLimit(hdr http.Header) (remaining, reset int, err error) {
	remaining, err = strconv.Atoi(hdr["X-Ratelimit-Remaining"][0])
	if err != nil {
		return
	}

	reset, err = strconv.Atoi(hdr["X-Ratelimit-Reset"][0])
	if err != nil {
		return
	}

	return
}

func requester(url string, h handler) error {
	for url != "" {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", auth)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		if remaining, reset, err := rateLimit(resp.Header); err != nil {
			log.Fatal(err)
		} else if remaining == 0 {
			log.Printf("sleeping reset=%s\n", time.Unix(int64(reset), 0))
			time.Sleep(time.Unix(int64(reset), 0).Sub(time.Now()))
			continue
		}

		if err = h(resp.Body); err != nil {
			return err
		}

		url = nextUrl(resp.Header)
	}

	return nil
}

func shasHandler(id string) handler {
	return func(rc io.ReadCloser) error {
		defer rc.Close()

		var result struct {
			Commit struct {
				Author struct {
					Email string
					Date string
				}
				Message string
			}
			Stats struct {
				Additions int
				Deletions int
				Total int
			}
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		shasDbUpdate(id,
			result.Commit.Author.Email,
			result.Commit.Author.Date,
			result.Commit.Message,
			result.Stats.Additions,
			result.Stats.Deletions,
			result.Stats.Total)

		return nil
	}
}

func shas(id, repo, sha string) {
	log.Printf("repo=%s sha=%s\n", repo, sha)
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
	if err := requester(url, shasHandler(id)); err != nil {
		log.Fatal(err)
	}
}

func shasDbUpdate(id, email, date, message string, additions, deletions, total int) {
	rows, err := db.Query("UPDATE commits SET email=$2, date=$3, message=$4, additions=$5, deletions=$6, total=$7 WHERE id=$1", id, email, date, message, additions, deletions, total)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func shasDbQuery() bool {
	rows, err := db.Query("SELECT id, repo, sha FROM commits WHERE total IS NULL LIMIT $1", *limit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	next := false
	for rows.Next() {
		next = true
		var id, repo, sha string
		if err := rows.Scan(&id, &repo, &sha); err != nil {
			log.Fatal(err)
		}

		wg.Add(1)
		go func(i, r, s string) {
			defer wg.Done()
			shas(i, r, s)
		}(id, repo, sha)
	}

	return next
}

func shasDbFind(repo, sha string) bool {
	rows, err := db.Query("SELECT id FROM commits WHERE repo = $1 AND sha = $2", repo, sha)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	return rows.Next()
}

func shasDbCreate(repo, sha string) {
	rows, err := db.Query("INSERT INTO commits (id, repo, sha) VALUES ($1, $2, $3)", uuid.New(), repo, sha)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func shasDb(repo, sha string) {
	log.Printf("repo=%s sha=%s\n", repo, sha)
	if !shasDbFind(repo, sha) {
		shasDbCreate(repo, sha)
	}
}

func commitsHandler(repo string) handler {
	return func(rc io.ReadCloser) error {
		defer rc.Close()

		var result []struct {
			Sha string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		for _, c := range result {
			shasDb(repo, c.Sha)
		}

		return nil
	}
}

func commits(repo string) {
	log.Printf("repo=%s\n", repo)
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits", org, repo)
	if err := requester(url, commitsHandler(repo)); err != nil {
		log.Fatal(err)
	}
}

func reposHandler() handler {
	return func(rc io.ReadCloser) error {
		defer rc.Close()

		var result []struct {
			Name string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		for _, r := range result {
			wg.Add(1)
			go func(r string) {
				defer wg.Done()
				commits(r)
			}(r.Name)
		}

		return nil
	}
}

func repos() {
	url := fmt.Sprintf("https://api.github.com/orgs/%s/repos", org)
	if err := requester(url, reposHandler()); err != nil {
		log.Fatal(err)
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=gitz ")

	flag.Parse()

	if *insert {
		repos()
	}

	if *update {
		for shasDbQuery() {
		}
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
