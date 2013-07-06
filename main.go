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
	"time"
)

var (
	dbUrl = mustGetenv("DATABASE_URL")
	db    = dbOpen()
	urlRe = regexp.MustCompile("<(.*)>; rel=\"(.*)\"")
	org   = mustGetenv("GITHUB_ORG")
	auth  = "token " + mustGetenv("GITHUB_OAUTH_TOKEN")
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

		if err = h(resp.Body); err != nil {
			return err
		}

		remaining, reset, err := rateLimit(resp.Header)
		if remaining == 0 {
			time.Sleep(time.Unix(int64(reset), 0).Sub(time.Now()))
		}

		url = nextUrl(resp.Header)
	}

	return nil
}

func shasHandler(repo, sha string) handler {
	return func(rc io.ReadCloser) error {
		var result struct {
			Sha string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		log.Println(result.Sha)

		return nil
	}
}

func shas(repo, sha string) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
	err := requester(url, shasHandler(repo, sha))
	return err
}

func commitsHandler(repo string) handler {
	return func(rc io.ReadCloser) error {
		var result []struct {
			Sha string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		for _, c := range result {
			shas(repo, c.Sha)
		}

		return nil
	}
}

func commits(repo string) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits", org, repo)
	err := requester(url, commitsHandler(repo))
	return err
}

func reposHandler() handler {
	return func(rc io.ReadCloser) error {
		var result []struct {
			Name string
		}

		if err := json.NewDecoder(rc).Decode(&result); err != nil {
			return err
		}

		for _, r := range result {
			commits(r.Name)
		}

		return nil
	}
}

func repos() error {
	url := fmt.Sprintf("https://api.github.com/orgs/%s/repos", org)
	err := requester(url, reposHandler())
	return err
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=gitz ")

	flag.Parse()

	repos()
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
