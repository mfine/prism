package main

import (
	"bytes"
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

var (
	inserter = flag.Bool("inserter", false, "Insert Worker")
	updater  = flag.Bool("updater", false, "Update Worker")
	loop     = flag.Bool("loop", false, "Loop Worker")
	limit    = flag.Int("limit", 1000, "Query Limit")
	scale    = flag.Int("scale", 5, "Number of Workers")
	delay    = flag.Int("delay", 15, "Delay")
	since    = flag.String("since", "", "Since Timestamp")
	until    = flag.String("until", "", "Until Timestamp")
	org      = mustGetenv("ORG")
	ignores  = makeIgnored(mustGetenv("IGNORE_REPOS"))
	auth     = "token " + mustGetenv("OAUTH_TOKEN")
	db       = dbOpen(mustGetenv("DATABASE_URL"))
	urlRe    = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
	iso8601  = "2006-01-02T15:04:05Z"
	next     = time.Now().Format(iso8601)
	etags    map[string]string
	now      string
	wg       sync.WaitGroup
)

type handler func(io.Reader)

// get the next url from the link headers
// http://developer.github.com/v3/#pagination
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

// check rate limiting headers
// http://developer.github.com/v3/#rate-limiting
func rateLimit(hdr http.Header) bool {
	remaining, err := strconv.Atoi(hdr["X-Ratelimit-Remaining"][0])
	if err != nil {
		log.Fatal(err)
	}

	reset, err := strconv.Atoi(hdr["X-Ratelimit-Reset"][0])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("fn=rateLimit remaining=%v\n", remaining)
	if remaining == 0 {
		resetAt := time.Unix(int64(reset), 0)
		log.Printf("fn=rateLimit reset=%v wait=%v\n", resetAt.Format(iso8601), resetAt.Sub(time.Now()))
		// use delay... don't sleep for wait, as remaining can stay 0 during reset update :(
		time.Sleep(time.Duration(*delay) * time.Second)
		return true
	}

	return false
}

// check rate limit
func rateLimitCheck() bool {
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

// requests for repos, commits, and shas; returned url controls iteration
func request(url string, h handler) string {
	if rateLimitCheck() {
		return url
	}

	log.Printf("fn=request url=%q\n", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", auth)

	// NOT THREAD SAFE
	if etag := etags[url]; etag != "" {
		req.Header.Set("If-None-Match", fmt.Sprintf("\"%s\"", etag))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// yes, check rate limit headers again
	if rateLimit(resp.Header) {
		return url
	}

	// 409 - empty repository
	if resp.StatusCode != 200 {
		if resp.StatusCode != 304 {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Printf("url=%v StatusCode=%v Body=%q\n", url, resp.StatusCode, body)
		}
		return nextUrl(resp.Header)
	}

	h(resp.Body)

	// NOT THREAD SAFE
	etags[url] = resp.Header["ETag"][0]

	return nextUrl(resp.Header)
}

// loop requests based on returned url
func requests(url string, h handler) {
	for url != "" {
		url = request(url, h)
	}
}

// find shas the need metadata
func query(c chan<- func()) {
	rows, err := db.Query("SELECT id, repo, sha FROM commits WHERE org=$1 AND email IS NULL LIMIT $2", org, *limit)
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

		// closure to lookup sha
		c <- func(repo, sha, id string) func() { return func() { shas(repo, sha, id) } }(repo, sha, id)
		more = true
	}

	if more {
		// found something... look for more
		c <- func() { query(c) }
	} else {
		log.Println("fn=query at=done")

		// delay before looping, or close worker channel
		if *loop {
			time.Sleep(time.Duration(*delay) * time.Second)
			c <- func() { query(c) }
		} else {
			close(c)
		}
	}
}

// check if sha already there, or insert it
func findOrCreate(repo, sha string) {
	rows, err := db.Query("SELECT id FROM commits WHERE org=$1 AND repo=$2 AND sha=$3", org, repo, sha)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		return
	}

	if _, err := db.Exec("INSERT INTO commits (org, repo, sha) VALUES ($1, $2, $3)", org, repo, sha); err != nil {
		log.Fatal(err)
	}
}

// add metadata to sha
func update(id, email, date, message string, additions, deletions, total int) {
	if _, err := db.Exec("UPDATE commits SET email=$2, date=$3, msg=$4, adds=$5, dels=$6, total=$7 WHERE id=$1", id, email, date, message, additions, deletions, total); err != nil {
		log.Fatal(err)
	}
}

// shas request processing
func shasHandler(repo, sha, id string) handler {
	return func(rc io.Reader) {
		// http://developer.github.com/v3/repos/commits/#get-a-single-commit
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
			log.Printf("fn=shasHandler err=%v org=%v repo=%v sha=%v id=%v\n", err, org, repo, sha, id)
			return
		}

		log.Printf("fn=shasHandler org=%v repo=%v sha=%v id=%v\n", org, repo, sha, id)
		update(id,
			result.Commit.Author.Email,
			result.Commit.Author.Date,
			result.Commit.Message,
			result.Stats.Additions,
			result.Stats.Deletions,
			result.Stats.Total)
	}
}

// http://developer.github.com/v3/repos/commits/#get-a-single-commit
func shasUrl(repo, sha string) string {
	return fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
}

// list sha
func shas(repo, sha, id string) {
	requests(shasUrl(repo, sha), shasHandler(repo, sha, id))
}

// commits request processing
func commitsHandler(repo string) handler {
	return func(rc io.Reader) {
		// http://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
		var result []struct {
			Sha string
		}
		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Printf("fn=commitsHandler err=%v org=%v repo=%v\n", err, org, repo)
			return
		}

		// walk through shas, adding them to db if not present
		for _, c := range result {
			log.Printf("fn=commitsHandler org=%v repo=%v sha=%v\n", org, repo, c.Sha)
			findOrCreate(repo, c.Sha)
		}
	}
}

// bake in since and until values
// http://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
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
	return fmt.Sprintf(commitsUrlFormat(), org, repo)
}

// list commits
func commits(repo string) {
	requests(commitsUrl(repo), commitsHandler(repo))
}

// use repo pushed_at to filter
func pushedOk(pushed string) bool {
	pushedBytes := bytes.NewBufferString(pushed).Bytes()
	if now != "" {
		// repo hasn't changed since last loop
		nowBytes := bytes.NewBufferString(now).Bytes()
		if bytes.Compare(nowBytes, pushedBytes) == 1 {
			return false
		}
	}
	if *since != "" {
		// repo hasn't changed since since
		sinceBytes := bytes.NewBufferString(*since).Bytes()
		if bytes.Compare(sinceBytes, pushedBytes) == 1 {
			return false
		}
	}

	return true
}

// repos request processing
func reposHandler(c chan<- func()) handler {
	return func(rc io.Reader) {
		// http://developer.github.com/v3/repos/#list-organization-repositories
		var result []struct {
			Name      string
			Pushed_at string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			log.Printf("fn=reposHandler err=%v org=%v\n", err, org)
			return
		}

		// walk through repos, if not ignored add to worker
		for _, r := range result {
			log.Printf("fn=reposHandler org=%v repo=%v pushed=%q\n", org, r.Name, r.Pushed_at)
			if !ignores[r.Name] && pushedOk(r.Pushed_at) {
				c <- func(repo string) func() { return func() { commits(repo) } }(r.Name)
			}
		}
	}
}

// http://developer.github.com/v3/repos/#list-organization-repositories
func reposUrl() string {
	return fmt.Sprintf("https://api.github.com/orgs/%s/repos", org)
}

// list repos
func repos(c chan<- func()) {
	log.Printf("fn=repos now=%v next=%v\n", now, next)
	requests(reposUrl(), reposHandler(c))

	log.Println("fn=repos at=done")

	// delay before looping, or close worker channel
	// and update now, next times for filtering repos
	if *loop {
		time.Sleep(time.Duration(*delay) * time.Second)
		now, next = next, time.Now().Format(iso8601)
		c <- func() { repos(c) }
	} else {
		close(c)
	}
}

// worker loops on func's to call
func worker(c <-chan func()) {
	defer wg.Done()
	for f := range c {
		f()
	}
}

// setup channel and workers
func workers(c <-chan func()) {
	wg.Add(*scale)
	for i := 0; i < *scale; i++ {
		go worker(c)
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=prism ")

	flag.Parse()

	if *inserter {
		// setup worker pool and walk repos
		c := make(chan func())
		workers(c)
		c <- func() { repos(c) }
	}
	if *updater {
		// setup worker pool and walk db
		c := make(chan func())
		workers(c)
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
