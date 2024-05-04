from preprocess import parse_directory_to_txt


def test_parse_directory_method():
    res = parse_directory_to_txt("sample_data")
    assert type(res[res.keys()[0]], dict), "expected nested dictionary as output"
