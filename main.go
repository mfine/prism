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
	org   = mustGetenv("GITHUB_ORG")
	auth  = "token " + mustGetenv("GITHUB_OAUTH_TOKEN")
	dbUrl = mustGetenv("DATABASE_URL")
	db    = dbOpen()
	urlRe = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
	wg    sync.WaitGroup
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

func rateLimit(hdr http.Header) bool {
	remaining, err := strconv.Atoi(hdr["X-Ratelimit-Remaining"][0])
	if err != nil {
		log.Fatal(err)
	}

	reset, err := strconv.Atoi(hdr["X-Ratelimit-Reset"][0])
	if err != nil {
		log.Fatal(err)
	}

	if remaining == 0 {
		log.Printf("reset=%s\n", time.Unix(int64(reset), 0))
		time.Sleep(time.Unix(int64(reset), 0).Sub(time.Now()))
		return true
	}

	return false
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

		if rateLimit(resp.Header) {
			continue
		}

		h(resp.Body)

		url = nextUrl(resp.Header)
	}
}

func ignore(repo string) bool {
	rows, err := db.Query("SELECT id FROM ignores WHERE repo = $1", repo)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	return rows.Next()
}

func query(c chan<- func(), limit int) (more bool) {
	rows, err := db.Query("SELECT id, repo, sha FROM commits WHERE email IS NULL LIMIT $1", limit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, repo, sha string
		if err := rows.Scan(&id, &repo, &sha); err != nil {
			log.Fatal(err)
		}

		c <- func() { shasWork(repo, sha, id) }

		more = true
	}

	return
}

func findOrCreate(repo, sha string) {
	rows, err := db.Query("SELECT id FROM commits WHERE repo = $1 AND sha = $2", repo, sha)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		return
	}

	if _, err := db.Exec("INSERT INTO commits (repo, sha) VALUES ($1, $2)", repo, sha); err != nil {
		log.Fatal(err)
	}
}

func update(id, email, date, message string, additions, deletions, total int) {
	if _, err := db.Exec("UPDATE commits SET email=$2, date=$3, msg=$4, adds=$5, dels=$6, total=$7 WHERE id=$1", id, email, date, message, additions, deletions, total); err != nil {
		log.Fatal(err)
	}
}

func shasHandler(id string) handler {
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

		update(id,
			result.Commit.Author.Email,
			result.Commit.Author.Date,
			result.Commit.Message,
			result.Stats.Additions,
			result.Stats.Deletions,
			result.Stats.Total)
	}
}

func shasUrl(repo, sha string) string {
	return fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
}

func shasWork(repo, sha, id string) {
	request(shasUrl(repo, sha), shasHandler(id))
}

func commitsHandler(repo string) handler {
	return func(rc io.ReadCloser) {
		defer rc.Close()

		var result []struct {
			Sha string
		}
		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Fatal(err)
		}

		for _, c := range result {
			log.Printf("repo=%v sha=%v\n", repo, c.Sha)
			findOrCreate(repo, c.Sha)
		}
	}
}

func commitsUrl(repo string) string {
	return fmt.Sprintf("https://api.github.com/repos/%s/%s/commits", org, repo)
}

func commitsWork(repo string) {
	request(commitsUrl(repo), commitsHandler(repo))
}

func reposHandler(c chan<- func()) handler {
	return func(rc io.ReadCloser) {
		defer rc.Close()

		var result []struct {
			Name string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Fatal(err)
		}

		for _, r := range result {
			log.Printf("repo=%v\n", r.Name)
			if !ignore(r.Name) {
				c <- func() { commitsWork(r.Name) }
			}
		}
	}
}

func reposUrl() string {
	return fmt.Sprintf("https://api.github.com/orgs/%s/repos", org)
}

func reposWork(c chan<- func()) {
	request(reposUrl(), reposHandler(c))
}

func worker(c <-chan func()) {
	defer wg.Done()
	for w := range c {
		w()
	}
}

func workers(count int) (c chan func()) {
	c = make(chan func())
	wg.Add(count)
	for i := 0; i < count; i++ {
		go worker(c)
	}

	return
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=prism ")

	scale := flag.Int("scale", 5, "Number of Workers")
	insert := flag.Bool("insert", false, "Insert Worker")
	update := flag.Bool("update", false, "Update Worker")
	limit := flag.Int("limit", 1000, "Query Limit")

	flag.Parse()

	c := workers(*scale)

	if *insert {
		c <- func() {
			for {
				reposWork(c)
			}
		}
	}

	if *update {
		c <- func() {
			for {
				for query(c, *limit) { }
			}
		}
	}

	wg.Wait()
}

func dbOpen() (db *sql.DB) {
	name, err := pq.ParseURL(dbUrl)
	if err != nil {
		log.Fatal(err)
	}

	db, err = sql.Open("postgres", name+" sslmode=disable")
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
