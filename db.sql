CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE commits (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org text NOT NULL,
    repo text NOT NULL,
    sha text NOT NULL,
    msg text,
    email text,
    date timestamp with time zone,
    adds integer,
    dels integer,
    total integer
);

CREATE UNIQUE INDEX commits_on_org_repo_sha ON commits USING btree(org, repo, sha);
CREATE INDEX commits_on_email ON commits USING btree(email);

CREATE TABLE ignores (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org text NOT NULL,
    repo text NOT NULL
);

CREATE UNIQUE INDEX ignores_on_org_repo ON ignores USING btree(org, repo);
INSERT INTO ignores (org, repo) VALUES ('heroku', 'redistogo');
INSERT INTO ignores (org, repo) VALUES ('heroku', 'otp');
