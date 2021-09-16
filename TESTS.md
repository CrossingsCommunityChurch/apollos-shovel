# Running Tests

In order to run tests, you will need to install the requirements from requirements.txt

```shell
pip3 install -r requirements.txt
```

Then you'll be able to run the tests with

```shell
pytest

```

# Adding tests

Drop tests in the `./dags/tests` folder. Your tests will need to be a function with a name beginning with `def test_....`. You have pyVCR at your disposal for capturing network requests (likely from rock). There are utilities for setting up a database that you can connect to in order to check on the result of your shoveling. 