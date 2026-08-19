-- Frozen pre-Alembic Reader schema fixture.
--
-- This file is intentionally independent from the Alembic baseline and
-- schema_contract.py. It represents the schema produced by the historical
-- runtime create_all + ensure_columns + PostgreSQL index preparation path at
-- the pre-Issue #3 fixed point f3a04d8.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE app_settings (
  key VARCHAR(80) NOT NULL PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE clusters (
  id SERIAL PRIMARY KEY,
  cluster_key VARCHAR(80) NOT NULL,
  title TEXT NOT NULL,
  generated_title TEXT NOT NULL,
  generated_summary TEXT NOT NULL,
  generated_content TEXT,
  citations TEXT NOT NULL,
  model_version VARCHAR(120) NOT NULL,
  prompt_version VARCHAR(120) NOT NULL,
  first_seen_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT clusters_cluster_key_key UNIQUE (cluster_key)
);

CREATE TABLE folders (
  id SERIAL PRIMARY KEY,
  name VARCHAR(240) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT folders_name_key UNIQUE (name)
);

CREATE TABLE llm_tasks (
  id SERIAL PRIMARY KEY,
  task_type VARCHAR(80) NOT NULL,
  provider VARCHAR(80) NOT NULL,
  object_type VARCHAR(40) NOT NULL,
  object_id INTEGER NOT NULL,
  status VARCHAR(40) NOT NULL,
  prompt_version VARCHAR(120) NOT NULL,
  model_version VARCHAR(120) NOT NULL,
  result_json TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE topic_groups (
  id SERIAL PRIMARY KEY,
  name VARCHAR(240) NOT NULL,
  query TEXT NOT NULL,
  description TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT topic_groups_name_key UNIQUE (name)
);

CREATE TABLE user_states (
  id SERIAL PRIMARY KEY,
  object_type VARCHAR(40) NOT NULL,
  object_id INTEGER NOT NULL,
  read_status VARCHAR(40) NOT NULL,
  read_later BOOLEAN NOT NULL,
  starred BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_user_state_object UNIQUE (object_type, object_id)
);

CREATE TABLE sources (
  id SERIAL PRIMARY KEY,
  folder_id INTEGER REFERENCES folders(id),
  name VARCHAR(320) NOT NULL,
  url TEXT NOT NULL,
  site_url TEXT NOT NULL,
  media_type VARCHAR(32) NOT NULL,
  status VARCHAR(20) NOT NULL,
  enabled BOOLEAN NOT NULL,
  fetch_full_content BOOLEAN NOT NULL,
  feed_trust_score DOUBLE PRECISION NOT NULL,
  last_fetched_at TIMESTAMPTZ,
  last_error TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  status_changed_at TIMESTAMPTZ,
  CONSTRAINT sources_url_key UNIQUE (url)
);

CREATE TABLE feed_metrics (
  id SERIAL PRIMARY KEY,
  source_id INTEGER NOT NULL REFERENCES sources(id),
  fetched_count INTEGER NOT NULL,
  read_count INTEGER NOT NULL,
  opened_count INTEGER NOT NULL,
  starred_count INTEGER NOT NULL,
  read_later_count INTEGER NOT NULL,
  cluster_count INTEGER NOT NULL,
  duplicate_count INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE raw_entries (
  id SERIAL PRIMARY KEY,
  source_id INTEGER NOT NULL REFERENCES sources(id),
  external_id TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT NOT NULL,
  author TEXT NOT NULL,
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL,
  raw_summary TEXT NOT NULL,
  raw_content TEXT NOT NULL,
  content_hash VARCHAR(64) NOT NULL,
  CONSTRAINT uq_raw_source_external UNIQUE (source_id, external_id)
);

CREATE TABLE documents (
  id SERIAL PRIMARY KEY,
  raw_entry_id INTEGER NOT NULL REFERENCES raw_entries(id),
  document_type VARCHAR(32) NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  content_text TEXT NOT NULL,
  digest_score DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT documents_raw_entry_id_key UNIQUE (raw_entry_id)
);

CREATE TABLE content_items (
  id SERIAL PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id),
  source_id INTEGER NOT NULL REFERENCES sources(id),
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  content_text TEXT NOT NULL,
  url TEXT NOT NULL,
  published_at TIMESTAMPTZ,
  content_hash VARCHAR(64) NOT NULL,
  canonical_url TEXT NOT NULL,
  normalized_title TEXT NOT NULL,
  lsh_signature TEXT NOT NULL,
  media_url TEXT,
  media_kind VARCHAR(32),
  media_duration INTEGER NOT NULL,
  embedding_vector halfvec(2560),
  embedding_model VARCHAR(120),
  cluster_score DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE cluster_items (
  id SERIAL PRIMARY KEY,
  cluster_id INTEGER NOT NULL REFERENCES clusters(id),
  content_item_id INTEGER NOT NULL REFERENCES content_items(id),
  duplicate_score DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_cluster_item UNIQUE (cluster_id, content_item_id)
);

CREATE TABLE content_embeddings (
  id SERIAL PRIMARY KEY,
  content_item_id INTEGER NOT NULL REFERENCES content_items(id),
  representation VARCHAR(40) NOT NULL,
  model VARCHAR(120) NOT NULL,
  vector halfvec(2560),
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_content_embedding_representation
    UNIQUE (content_item_id, representation, model)
);

CREATE INDEX ix_content_items_fts
ON content_items USING GIN (
  to_tsvector(
    'simple',
    coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content_text, '')
  )
);

CREATE INDEX ix_sources_name_fts
ON sources USING GIN (to_tsvector('simple', coalesce(name, '')));

CREATE INDEX ix_clusters_fts
ON clusters USING GIN (
  to_tsvector(
    'simple',
    coalesce(title, '') || ' ' || coalesce(generated_title, '') || ' '
      || coalesce(generated_summary, '') || ' ' || coalesce(generated_content, '')
  )
);

CREATE INDEX ix_content_items_embedding_hnsw
ON content_items USING hnsw (embedding_vector halfvec_cosine_ops)
WHERE embedding_vector IS NOT NULL;

CREATE INDEX ix_content_embeddings_zh_hnsw
ON content_embeddings USING hnsw (vector halfvec_cosine_ops)
WHERE vector IS NOT NULL AND representation = 'zh_canonical';
