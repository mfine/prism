package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lib/pq"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 2012-08-22T04:40:05Z
var (
	inserter = flag.Bool("inserter", false, "Insert Worker")
	updater  = flag.Bool("updater", false, "Update Worker")
	loop     = flag.Bool("loop", false, "Loop Worker")
	limit    = flag.Int("limit", 1000, "Query Limit")
	scale    = flag.Int("scale", 5, "Number of Workers")
	delay    = flag.Int("delay", 60, "Delay")
	org      = flag.String("org", "heroku", "Organization")
	ignore   = flag.String("ignore", "", "Ignore Repos")
	since    = flag.String("since", "", "Since Timestamp")
	until    = flag.String("until", "", "Until Timestamp")
	auth     = "token " + mustGetenv("GITHUB_OAUTH_TOKEN")
	db       = dbOpen(mustGetenv("DATABASE_URL"))
	urlRe    = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
)

type handler func(io.Reader)

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
		log.Printf("fn=rateLimit reset=%v\n", time.Unix(int64(reset), 0))
		time.Sleep(time.Unix(int64(reset), 0).Sub(time.Now()))
		return true
	}

	return false
}

func rateCheck() bool {
	req, err := http.NewRequest("GET", "https://api.github.com/rate_limit", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", auth)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	return rateLimit(resp.Header)
}

func request(url string, h handler) string {
	if rateCheck() {
		return url
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", auth)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if rateLimit(resp.Header) {
		return url
	}

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("StatusCode=%v Body=%q", resp.StatusCode, body)
	}

	h(resp.Body)

	return nextUrl(resp.Header)
}

func requests(url string, h handler) {
	for url != "" {
		url = request(url, h)
	}
}

func query(c chan<- func()) {
	rows, err := db.Query("SELECT id, repo, sha FROM commits WHERE org=$1 AND email IS NULL LIMIT $2", *org, *limit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	more := false
	for rows.Next() {
		var id, repo, sha string
		if err := rows.Scan(&id, &repo, &sha); err != nil {
			log.Fatal(err)
		}

		c <- func() { shas(repo, sha, id) }

		more = true
	}

	if more {
		c <- func() { query(c) }
	} else {
		time.Sleep(time.Duration(*delay) * time.Second)

		if *loop {
			c <- func() { query(c) }
		} else {
			close(c)
		}
	}
}

func findOrCreate(repo, sha string) {
	rows, err := db.Query("SELECT id FROM commits WHERE org=$1 AND repo=$2 AND sha=$3", *org, repo, sha)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		return
	}

	if _, err := db.Exec("INSERT INTO commits (org, repo, sha) VALUES ($1, $2, $3)", *org, repo, sha); err != nil {
		log.Fatal(err)
	}
}

func update(id, email, date, message string, additions, deletions, total int) {
	if _, err := db.Exec("UPDATE commits SET email=$2, date=$3, msg=$4, adds=$5, dels=$6, total=$7 WHERE id=$1", id, email, date, message, additions, deletions, total); err != nil {
		log.Fatal(err)
	}
}

func shasHandler(repo, sha, id string) handler {
	return func(rc io.Reader) {
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
			log.Printf("fn=shasHandler err=%v org=%v repo=%v sha=%v id=%v", err, *org, repo, sha, id)
			return
		}

		log.Printf("fn=shasHandler org=%v repo=%v sha=%v id=%v\n", *org, repo, sha, id)

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
	return fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", *org, repo, sha)
}

func shas(repo, sha, id string) {
	requests(shasUrl(repo, sha), shasHandler(repo, sha, id))
}

// commits request processing
func commitsHandler(repo string) handler {
	return func(rc io.Reader) {
		var result []struct {
			Sha string
		}
		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Printf("fn=commitsHandler err=%v org=%v repo=%v\n", err, *org, repo)
			return
		}

		// walk through shas, adding them to db if not present
		for _, c := range result {
			log.Printf("fn=commitsHandler org=%v repo=%v sha=%v\n", *org, repo, c.Sha)
			findOrCreate(repo, c.Sha)
		}
	}
}

func commitsUrlFormat() (url string) {
	url = "https://api.github.com/repos/%s/%s/commits?"
	if *since != "" {
		url += fmt.Sprintf("since=%s&", *since)
	}
	if *until != "" {
		url += fmt.Sprintf("until=%s", *until)
	}

	return
}

func commitsUrl(repo string) string {
	return fmt.Sprintf(commitsUrlFormat(), *org, repo)
}

// list commits
func commits(repo string) {
	requests(commitsUrl(repo), commitsHandler(repo))
}

// repos request processing
func reposHandler(c chan<- func(), ignored map[string]bool) handler {
	return func(rc io.Reader) {
		var result []struct {
			Name string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Printf("fn=reposHandler err=%v org=%v\n", err, *org)
			return
		}

		// walk through repos, if not ignored add to worker
		for _, r := range result {
			log.Printf("fn=reposHandler org=%v repo=%v\n", *org, r.Name)
			if !ignored[r.Name] {
				c <- func() { commits(r.Name) }
			}
		}
	}
}

func reposUrl() string {
	return fmt.Sprintf("https://api.github.com/orgs/%s/repos", *org)
}

// list repos
func repos(c chan<- func(), ignored map[string]bool) {
	requests(reposUrl(), reposHandler(c, ignored))

	time.Sleep(time.Duration(*delay) * time.Second)

	if *loop {
		c <- func() { repos(c, ignored) }
	} else {
		close(c)
	}
}

// worker loops on func's to call
func worker(wg *sync.WaitGroup, c <-chan func()) {
	defer wg.Done()
	for w := range c {
		w()
	}
}

// setup channel and workers
func workers(wg *sync.WaitGroup) (c chan func()) {
	c = make(chan func())
	wg.Add(*scale)
	for i := 0; i < *scale; i++ {
		go worker(wg, c)
	}

	return
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=prism ")

	flag.Parse()

	var wg sync.WaitGroup
	if *inserter {
		// setup worker pool and walk repos
		c := workers(&wg)
		c <- func() { repos(c, makeIgnored(*ignore)) }
	}
	if *updater {
		// setup worker pool and walk db
		c := workers(&wg)
		c <- func() { query(c) }
	}
	wg.Wait()
}

func dbOpen(url string) (db *sql.DB) {
	name, err := pq.ParseURL(url)
	if err != nil {
		log.Fatal(err)
	}

	db, err = sql.Open("postgres", name+" sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	return
}

func makeIgnored(ignore string) map[string]bool {
	m := make(map[string]bool)
	for _, i := range strings.Split(ignore, ",") {
		m[i] = true
	}

	return m
}

func mustGetenv(key string) (value string) {
	if value = os.Getenv(key); value == "" {
		log.Fatalf("%v not set", key)
	}

	return
}
