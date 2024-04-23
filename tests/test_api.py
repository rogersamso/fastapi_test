from pathlib import Path
import json
import pytest
from fastapi.testclient import TestClient
import pandas as pd
from datetime import datetime
from deepdiff import DeepDiff

from main import app, Item, DATE_FORMAT

main_dir = Path(__file__).resolve().parent.parent

# could be a fixture as well
test_df = pd.read_parquet(main_dir / "data/11.parquet")


def mocked_get_data():
    "mock of get_data so that we can replace the dataframe with any other"
    return test_df


@pytest.fixture
def client(mocker):
    mocker.patch("main.get_data", return_value=mocked_get_data())
    return TestClient(app)


def test_read_items_no_params(client):

    df = mocked_get_data()
    expected_cols = df.columns.to_list()

    params = {}

    # Make the GET request with the query parameters
    response = client.get("/items", params=params)

    # Check if the request was successful (status code 200)
    assert response.status_code == 200

    # Assuming your response contains JSON data, you can access it like this:
    data = response.json()
    assert data["col_names"] == expected_cols  # Check if data is not empty


def test_read_items_optional_varname(client):

    params = {"search": "vel"}
    # Make the GET request with the query parameters
    response = client.get("/items", params=params)

    # Check if the request was successful (status code 200)
    assert response.status_code == 200

    # Assuming your response contains JSON data, you can access it like this:
    data = response.json()
    assert all(
        map(
            lambda x: x in ["vel58.3", "vel47.5", "vel32"],
            data[params["search"]].keys(),
        )
    )


def test_read_items_optional_skip_limit(client):

    params = {"skip": 2, "limit": 10, "search": "vel58"}

    # Make the GET request with the query parameters
    response = client.get("/items", params=params)

    # Check if the request was successful (status code 200)
    assert response.status_code == 200

    # Assuming your response contains JSON data, you can access it like this:
    data = response.json()
    assert data.get("skip") == 2
    assert data.get("limit") == 10

    assert len(data["vel58"]["vel58.3"]["values"]) == 8


def test_item_model_serialization():
    item = Item(
        names=["alpha", "beta", "gamma"],
        start_date="2020-06-27 00:00:00",
        end_date="2021-01-10 20:50:00",
    )
    result = item.model_dump_json()
    assert (
        result
        == '{"names":["alpha","beta","gamma"],"start_date":"2020-06-27 00:00:00","end_date":"2021-01-10 20:50:00"}'
    )


def test_item_model_serialization_wrong_date_fmt():
    wrong_date_fmt = "2020/06/27 00:00:00"

    with pytest.raises(ValueError) as e:
        Item(
            names=["alpha", "beta", "gamma"],
            start_date=wrong_date_fmt,
            end_date="2021-01-10 20:50:00",
        )

    assert "Value error, Incorrect date format" in str(e.value)


def test_item_model_serialization_date_bounds_error():

    initial_outside_bounds = "2019-03-03 10:00:45"
    final_outside_bounds = "2022-03-03 10:05:45"

    data = {
        "names": ["vel58.3"],
        "start_date": initial_outside_bounds,
        "end_date": final_outside_bounds,
    }
    with pytest.raises(ValueError) as e:
        Item(**data)

    assert "Value error, Date 2019-03-03 10:00:45 outside of data bounds" in str(
        e.value
    )


def test_item_model_serialization_empty_arguments():

    data = {}

    item = Item(**data)
    result = item.model_dump_json()
    assert result == '{"names":null,"start_date":null,"end_date":null}'


# all the following tests could be parametrized
def test_get_stats_unavailable_var_names(client):
    start_date = "2019-06-27 00:00:00"
    end_date = "2021-01-10 23:50:00"

    data = {
        "names": ["missing", "var", "names"],
        "start_date": start_date,
        "end_date": end_date,
    }

    response = client.post("/items/", json=data)
    assert response.status_code == 400

    text = json.loads(response.text)
    assert text == "None of the variables are present in the dataset"


def test_get_stats_wrong_dates(client):
    initial_date = "2020-03-03 10:00:45"
    final_date = "2019-07-03 10:05:45"

    data = {
        "names": ["vel58.3"],
        "start_date": initial_date,
        "end_date": final_date,
    }

    response = client.post("/items/", json=data)
    assert response.status_code == 400

    text = json.loads(response.text)

    assert text["error"]["message"] == "Initial date must be before final date"


def test_get_stats_wrong_date_format(client):

    data = {
        "names": ["date format error"],
        "start_date": "2019-03-03",
        "end_date": "2022-03-03 10:05:45",
    }

    response = client.post("/items/", json=data)

    assert response.status_code == 422
    assert response.reason_phrase == "Unprocessable Entity"
    text = json.loads(response.text)
    assert (
        text["detail"][0]["msg"]
        == "Value error, Incorrect date format. Date should be in the format YYYY-MM-DD HH:MM:SS"
    )
    assert text["detail"][1]["msg"].startswith(
        "Value error, Date 2022-03-03 10:05:45 outside of data bounds"
    )


def test_get_stats_with_empty_arguments(client):

    data = {}

    response = client.post("/items/", json=data)
    assert response.status_code == 200

    text = json.loads(response.text)
    df = mocked_get_data()

    # stats available for all df columns
    assert len(text["stats"].keys()) == len(df.columns)
    assert text["start_date"] == "2019-06-27T00:00:00"
    assert text["end_date"] == "2021-01-10T23:50:00"


def test_get_stats_several_vars(client):
    # Data for the POST request

    start_date = "2020-12-27 00:00:00"
    end_date = "2021-01-10 23:50:00"

    start_dt = datetime.strptime(start_date, DATE_FORMAT)
    end_dt = datetime.strptime(end_date, DATE_FORMAT)

    df = mocked_get_data()
    cols = df.columns[3:6].to_list()

    expected_stats = df.loc[start_dt:end_dt, cols].describe().to_dict()

    data = {
        "names": list(cols),
        "start_date": start_date,
        "end_date": end_date,
    }

    # Make the POST request
    response = client.post("/items/", json=data)

    # Check the response status code
    assert response.status_code == 200

    assert response.headers["Content-Type"] == "application/json"

    # Check the response content
    text = json.loads(response.text)
    diff = DeepDiff(text["stats"], expected_stats)

    assert not diff
