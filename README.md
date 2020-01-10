Package
-------

![badge](https://github.com/MartinHowarth/python-template-repository/workflows/Test/badge.svg)

Instructions for using this template:

1. Rename the package in the following locations:
  - This `README.md`
  - The `package` directory
  - In the environment variable `LINT_DIRS` at the top of `.github/workflows/test_and_lint.yml`
  - The `name` field in `pyproject.toml`
2. Update with your other information:
  - Year and your name in `LICENSE`
  - Various fields in `pyproject.toml` such as authors and repository
3. Create a github secret called `SEMANTIC_RELEASE_GITHUB_TOKEN` containing a github access token with push permissions
4. Create a github secret called `PYPI_TOKEN` containing your pypi token for uploading python packages.


Development installation
------------------------
The following command should be used to install the dependencies:

    poetry install


Testing
-------
The following command should be used to run the tests:

    poetry run pytest tests
