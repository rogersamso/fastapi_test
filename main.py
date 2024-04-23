from pathlib import Path
from typing import Optional, List
from datetime import datetime

from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator, field_serializer
import uvicorn
import pandas as pd

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

app = FastAPI()


parquet_file_path = Path("data/11.parquet")

# read parquet with pandas
df = pd.read_parquet(parquet_file_path)


def get_data():
    return df


def get_date_bounds():
    return df.index[0], df.index[-1]


class Item(BaseModel):
    names: Optional[List[str]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @field_serializer("start_date", "end_date")
    def serialize_dates(self, value: datetime):
        return value.strftime(DATE_FORMAT) if value is not None else None

    @field_validator("start_date", "end_date", mode="before")
    @staticmethod
    def check_date_format(value):
        if value:
            try:
                datetime.strptime(value, DATE_FORMAT)
            except ValueError:
                raise ValueError(
                    "Incorrect date format. Date should be in the format YYYY-MM-DD HH:MM:SS"
                )

        return value

    @field_validator("start_date", "end_date")
    @classmethod
    def check_date_within_range(cls, value):

        df_start_date, df_end_date = get_date_bounds()

        if value and not (df_start_date <= value <= df_end_date):
            raise ValueError(
                f"Date {value} outside of data bounds ({df_start_date}: {df_end_date})"
            )

        return value


@app.get("/")
def root():
    return {"message": "get and post endpoints on /items/"}


@app.get("/items/")
def read_items(
    df: pd.DataFrame = Depends(get_data),
    skip: Optional[int] = 0,
    limit: Optional[int] = None,
    search: Optional[str] = None,
):

    col_names = df.columns.to_list()

    if not search:
        return {"col_names": col_names}

    return_items = {}

    return_cols = list(filter(lambda s: s.startswith(search), col_names))

    if not list(return_cols):
        return {"message": f"No columns found with the search criteria: {search}"}

    for column in return_cols:

        return_items[column] = {}

        # json encoders cannot handle np.nans
        series = df[column].fillna('missing')

        idx_list, values_list = series.index.to_list(), series.values

        values = values_list[skip:limit] if limit else values_list[skip:]
        dates = idx_list[skip:limit] if limit else idx_list[skip:]

        return_items[column].update({"values": values.tolist()})
        return_items[column].update({"dates": dates})

    return {search: return_items, "skip": skip, "limit": limit}


@app.post("/items/")
def get_stats(item: Item, df: pd.DataFrame = Depends(get_data)):

    user_columns = item.names
    start_date = df.index[0] if not item.start_date else item.start_date
    end_date = df.index[-1] if not item.end_date else item.end_date

    df_cols = df.columns

    if start_date > end_date:
        error = {
            "error": {
                "message": "Initial date must be before final date",
                "status_code": 400,
            }
        }
        return JSONResponse(content=error, status_code=400)

    # define response dict
    response = {"start_date": start_date, "end_date": end_date}

    missing_cols = []
    if not user_columns:
        df_slice = df.loc[start_date:end_date, :]
    else:
        missing_cols = list(filter(lambda x: x not in df_cols, user_columns))
        return_items = list(set(user_columns) ^ set(missing_cols))

        if not return_items:
            return JSONResponse(
                content="None of the variables are present in the dataset",
                status_code=400,
            )

        df_slice = df.loc[start_date:end_date, return_items] if return_items else None

    # update response with actual data
    response.update(
        {"stats": df_slice.describe().to_dict(), "wrong_var_names": missing_cols}
    )

    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
