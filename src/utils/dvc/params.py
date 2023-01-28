"""
Functions needed to load parameters from params.yaml tracked with DVC
"""
import yaml
import sys
import os


def get_params():
    """
    Reads parameters for a given DVC stage from params.yaml.

    The stage name is inferred from the name of the python file that calls this
    function.

    :returns params: dict with parameters for the stage
    :raises KeyError: if the stage name is not found in params.yaml
    """

    stage_fn = os.path.basename(sys.argv[0]).replace(".py", "")

    try:
        params = yaml.safe_load(open("params.yaml"))[stage_fn]
    except KeyError:
        print(f'ERROR: Key "{stage_fn}" not in parameters.yaml.')
        print(f"""Is the stage file name ({sys.argv[0]}) the
         same as the stage name in params.yaml?""")
        sys.exit(1)

    return params
