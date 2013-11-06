CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

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
CREATE INDEX commits_on_date ON commits USING btree(date);
CREATE INDEX commits_on_repo ON commits USING btree(repo);
CREATE INDEX commits_on_msg ON commits USING gist(msg gist_trgm_ops);

CREATE TABLE pulls (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org text NOT NULL,
    repo text NOT NULL,
    number integer NOT NULL
);

CREATE UNIQUE INDEX pulls_on_org_repo_number ON pulls USING btree(org, repo, number);
