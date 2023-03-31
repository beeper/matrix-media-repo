CREATE TABLE IF NOT EXISTS media_references (
	media_id TEXT NOT NULL,
	origin TEXT NOT NULL,
	room_id TEXT NOT NULL,
	FOREIGN KEY (media_id, origin) REFERENCES media (media_id, origin)
);
CREATE UNIQUE INDEX IF NOT EXISTS media_references_index ON media_references (media_id, origin, room_id);
