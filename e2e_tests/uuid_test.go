package e2etests

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
