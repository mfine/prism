### Setup on Heroku

```
heroku create
heroku config:add \
  BUILDPACK_URL="https://github.com/kr/heroku-buildpack-go.git" \
  ORG=${ORG} \
  OAUTH_TOKEN=${OAUTH_TOKEN}
git push heroku master
heroku addons:add heroku-postgresql:crane
heroku pg:wait
heroku pg:promote COLOR
heroku pg:psql
# load db.sql
heroku drains:add syslog://forward.log.herokai.com:9999
heroku ps:scale main=1
```

