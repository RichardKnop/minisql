package e2etests

import (
	"regexp"
)

var uuidRegexp = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func (s *TestSuite) TestGenRandomUUID() {
	_, err := s.db.Exec(`create table accounts (
		id      uuid    not null default gen_random_uuid(),
		name    text    not null,
		ref_id  uuid
	);`)
	s.Require().NoError(err)

	s.Run("default_generates_uuid_on_omit", func() {
		_, err := s.db.Exec(`insert into accounts (name) values (?)`, "Alice")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id, name from accounts where name = ?`, "Alice")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID, gotName string
		s.Require().NoError(rows.Scan(&gotID, &gotName))
		s.Equal("Alice", gotName)
		s.Regexp(uuidRegexp, gotID, "generated UUID must be valid v4")
	})

	s.Run("explicit_gen_random_uuid_in_values", func() {
		_, err := s.db.Exec(`insert into accounts (id, name) values (gen_random_uuid(), ?)`, "Bob")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id from accounts where name = ?`, "Bob")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID string
		s.Require().NoError(rows.Scan(&gotID))
		s.Regexp(uuidRegexp, gotID, "explicit GEN_RANDOM_UUID() must produce valid v4 UUID")
	})

	s.Run("update_set_gen_random_uuid", func() {
		_, err := s.db.Exec(`insert into accounts (name) values (?)`, "Carol")
		s.Require().NoError(err)

		_, err = s.db.Exec(`update accounts set ref_id = gen_random_uuid() where name = ?`, "Carol")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select ref_id from accounts where name = ?`, "Carol")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotRefID string
		s.Require().NoError(rows.Scan(&gotRefID))
		s.Regexp(uuidRegexp, gotRefID, "UPDATE SET gen_random_uuid() must produce valid v4 UUID")
	})

	s.Run("select_gen_random_uuid_scalar", func() {
		// SELECT gen_random_uuid() as an expression alongside a real column.
		rows, err := s.db.Query(`select gen_random_uuid() from accounts where name = ?`, "Alice")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID string
		s.Require().NoError(rows.Scan(&gotID))
		s.Regexp(uuidRegexp, gotID, "SELECT gen_random_uuid() must produce valid v4 UUID")
	})

	s.Run("two_inserts_get_distinct_uuids", func() {
		_, err := s.db.Exec(`insert into accounts (name) values (?)`, "Dave")
		s.Require().NoError(err)
		_, err = s.db.Exec(`insert into accounts (name) values (?)`, "Eve")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id from accounts where name = ? or name = ? order by name`, "Dave", "Eve")
		s.Require().NoError(err)
		defer rows.Close()

		var ids []string
		for rows.Next() {
			var id string
			s.Require().NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		s.Require().Len(ids, 2)
		s.NotEqual(ids[0], ids[1], "each row must get a unique UUID")
	})

	s.Run("ddl_round_trip_preserves_default", func() {
		_, err := s.db.Exec(`create table sessions (
			token   uuid    not null default gen_random_uuid(),
			user_id int8    not null
		);`)
		s.Require().NoError(err)

		_, err = s.db.Exec(`insert into sessions (user_id) values (?)`, 42)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select token from sessions where user_id = ?`, 42)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var tok string
		s.Require().NoError(rows.Scan(&tok))
		s.Regexp(uuidRegexp, tok, "UUID generated after DDL round-trip must be valid v4")
	})

	s.Run("gen_random_uuid_on_non_uuid_column_rejected", func() {
		_, err := s.db.Exec(`create table bad_default (
			id   int8    not null,
			name text    default gen_random_uuid() not null
		);`)
		s.Require().Error(err, "GEN_RANDOM_UUID() default must be rejected on non-UUID column")
	})
}

func (s *TestSuite) TestUUID() {
	_, err := s.db.Exec(`create table widgets (
		id    uuid primary key,
		name  varchar(100) not null,
		owner uuid
	);`)
	s.Require().NoError(err)

	const uuid1 = "550e8400-e29b-41d4-a716-446655440000"
	const uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	const uuid3 = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"

	s.Run("INSERT_and_SELECT", func() {
		_, err := s.db.Exec(
			`insert into widgets (id, name) values (?, ?)`,
			uuid1, "Widget Alpha",
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id, name from widgets where id = ?`, uuid1)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID, gotName string
		s.Require().NoError(rows.Scan(&gotID, &gotName))
		s.Equal(uuid1, gotID)
		s.Equal("Widget Alpha", gotName)
	})

	s.Run("INSERT_uppercase_normalised", func() {
		_, err := s.db.Exec(
			`insert into widgets (id, name) values (?, ?)`,
			"6BA7B810-9DAD-11D1-80B4-00C04FD430C8", "Widget Beta",
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select id from widgets where name = ?`, "Widget Beta")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var gotID string
		s.Require().NoError(rows.Scan(&gotID))
		// Must come back in lowercase canonical form
		s.Equal(uuid2, gotID)
	})

	s.Run("INSERT_invalid_uuid_rejected", func() {
		_, err := s.db.Exec(
			`insert into widgets (id, name) values (?, ?)`,
			"not-a-uuid", "bad",
		)
		s.Require().Error(err)
	})

	s.Run("WHERE_equality", func() {
		rows, err := s.db.Query(`select name from widgets where id = ?`, uuid1)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var name string
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Widget Alpha", name)
	})

	s.Run("WHERE_not_equal", func() {
		rows, err := s.db.Query(`select name from widgets where id != ?`, uuid1)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var name string
		s.Require().NoError(rows.Scan(&name))
		s.Equal("Widget Beta", name)
	})

	s.Run("nullable_UUID_column", func() {
		_, err := s.db.Exec(
			`insert into widgets (id, name, owner) values (?, ?, ?)`,
			uuid3, "Widget Gamma", uuid1,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select owner from widgets where id = ?`, uuid3)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var owner string
		s.Require().NoError(rows.Scan(&owner))
		s.Equal(uuid1, owner)
	})

	s.Run("NULL_UUID_column", func() {
		rows, err := s.db.Query(`select owner from widgets where id = ?`, uuid1)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var owner *string
		s.Require().NoError(rows.Scan(&owner))
		s.Nil(owner)
	})

	s.Run("UPDATE_UUID_column", func() {
		_, err := s.db.Exec(
			`update widgets set owner = ? where id = ?`,
			uuid2, uuid1,
		)
		s.Require().NoError(err)

		rows, err := s.db.Query(`select owner from widgets where id = ?`, uuid1)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var owner string
		s.Require().NoError(rows.Scan(&owner))
		s.Equal(uuid2, owner)
	})

	s.Run("CAST_UUID_to_text_in_SELECT", func() {
		// CAST a UUID column value to TEXT in SELECT.
		rows, err := s.db.Query(
			`select cast(id as text) from widgets where name = ?`,
			"Widget Alpha",
		)
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(uuid1, got)
	})

	s.Run("DDL_round_trip", func() {
		_, err := s.db.Exec(`create table things (
			item_id uuid primary key,
			label   varchar(50) not null
		);`)
		s.Require().NoError(err)

		const itemUUID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
		_, err = s.db.Exec(`insert into things (item_id, label) values (?, ?)`,
			itemUUID, "test")
		s.Require().NoError(err)

		rows, err := s.db.Query(`select item_id from things where label = ?`, "test")
		s.Require().NoError(err)
		defer rows.Close()

		s.Require().True(rows.Next())
		var got string
		s.Require().NoError(rows.Scan(&got))
		s.Equal(itemUUID, got)
	})
}
