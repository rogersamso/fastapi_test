# fastAPI for nnergix

## Endpoint 1: GET /items/

- **Description:** Retrieves a list of items or the actual values of specific datasets.
- **Parameters:**
  - `limit` (optional): Limits the number of rows returned per request. Default is 10.
  - `skip` (optional): Skips the specified number of rows from the beginning of the result set.
  - `search` (optional): Searches for items based on the provided query string.
- **Additional Functionality:**
  - If no arguments are passed, it returns all column names in the data file.
- **Example Request:**
  - `GET /items/?limit=10&skip=0&search=var`
  - `GET /items/` (to retrieve all column names)


  ## Endpoint 2: POST /items/

- **Description:** Returns statistics for the whole dataset, or for specific data series.
- **Parameters:**
  - `names`: The name of the item.
  - `start_date`: The start date of the item in the format `yyyy-mm-dd hh:mm:ss`.
  - `end_date`: The end date of the item in the format `yyyy-mm-dd hh:mm:ss`.
- **Example Request:**
  ```json
  POST /items/
  {
      "name": ["vel58.3"],
      "start_date": "2024-04-23 08:00:00",
      "end_date": "2024-04-30 18:00:00"
  }
  ```
