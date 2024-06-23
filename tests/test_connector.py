from etl_components.connector import schema_to_string


def test_print_schema() -> None:
    """Test if schema_to_string formats the schema correctly."""
    schema = {
        "fiets": ["ketting", "zadel"],
        "trein": ["pantograaf", "locomotief"],
    }
    schema_string = "TABLE fiets {\n  ketting\n  zadel\n}\n\nTABLE trein {\n  pantograaf\n  locomotief\n} "
    assert schema_to_string(schema) == schema_string.strip()
