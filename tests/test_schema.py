from etl_components.schema import Column, Schema, Table


def test_print_schema() -> None:
    """Test if schema_to_string formats the schema correctly."""
    schema = Schema(
        [
            Table("fiets", [Column("ketting", "BOOL"), Column("zadel", "STR")]),
            Table(
                "trein",
                [Column("pantograaf", "BOOL"), Column("locomotief", "STR")],
            ),
        ]
    )

    schema_string = "TABLE fiets {\n  ketting: BOOL\n  zadel: STR\n}\n\nTABLE trein {\n  pantograaf: BOOL\n  locomotief: STR\n} "
    assert str(schema) == schema_string.strip()
