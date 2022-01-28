# Preprocessing



## Installation


We use conda as an environment manager and poetry as dependency manager.

1. Generate a conda env 
First, create and activate a basic conda env from the [env_prep.yml](./env/env_prep.yml) file. 

Run 
```
    conda env create -f ./env/env_prep.yml
```

then 

```
    conda activate env_prep
```

NB: it can be good to change the conda name env into [env_basic_conda.yml](./env/env_basic_conda.yml) file.


2. Install poetry and package dependencies

To install package dependencies with poetry, 

```
    poetry install
```

To update package dependencies, 
```
    poetry update
```


## Testing 

For running all the tests:

```
    poetry run pytest 
```

For running a specific test: 
```
    poetry run pytest path/my_test
```


See test coverage : [TO COMPLETE]


## Preprocessing pipeline 

- [How does the preprocessing pipeline works?](docs/pipeline_preprocessing/pipeline.md)


## Good pratices 

### PEP8

Use the pep8 norm to format all the code. Specific pep8 parameters are given into the [pyproject.toml](pyproject.toml) file.

```
autopep8 --in-place --aggressive --aggressive ./
```


### FLAKE8

[TO COMPLETE]
