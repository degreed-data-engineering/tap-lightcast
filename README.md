# tap-lightcast
This tap lightcast was created by Degreed to be used for extracting data via Meltano into defined targets.

# Configuration required:

```python
    config_jsonschema = th.PropertiesList(
        th.Property("client_id", th.StringType, required=True, description="Client ID"),
        th.Property(
            "client_secret", th.StringType, required=True, description="Client Secret"
        ),
        th.Property(
            "limit",
            th.IntegerType,
            required=True,
            description="Used for debugging purposes. It limits the number of IDs to select.",
        ),
    ).to_dict()
```
## Testing locally

To test locally, pipx poetry
```bash
pipx install poetry
```

Install poetry for the package
```bash
poetry install
```

To confirm everything is setup properly, run the following: 
```bash
poetry run tap-lightcast --help
```

To run the tap locally outside of Meltano and view the response in a text file, run the following: 
```bash
poetry run tap-lightcast > output.txt 
```

A full list of supported settings and capabilities is available by running: `tap-lightcast --about`