CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE commits (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    repo text NOT NULL,
    sha text NOT NULL,
    msg text,
    email text,
    date timestamp with time zone,
    adds integer,
    dels integer,
    total integer
);

CREATE UNIQUE INDEX commits_on_repo_sha ON commits USING btree(repo, sha);
CREATE UNIQUE INDEX commits_on_email ON commits USING btree(email);
