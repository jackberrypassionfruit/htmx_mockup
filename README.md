
# Django + HTMX + Alpine.js DEMO

## Using Cores Hub - Serialize page

### Featuring
- DB queries & trnasactions
- UI Reactivity
- Pop up Modals
- More to come...

## How to Install

### *If you do not have [uv](https://docs.astral.sh/uv/) installed:*

For Unix:

`curl -LsSf https://astral.sh/uv/install.sh | sh`

For Windows:

`powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`

## Syncing the project
*Navigate into project folder*

`uv sync`

## Starting the Django Development Server

*Using Unix:*

```
. ./.venv/bin/activate

django manage.py runserver
```

## Restoring the mock DB (after "moving" all jobs)

- Delete the db.sqlite3 file
- Runs these commands:

```
django manage.py migrate

uv run collect.py serialize/test_files/serialize_now.csv
```