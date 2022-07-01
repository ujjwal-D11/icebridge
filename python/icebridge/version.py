import os


def get_version() -> str:
    if  os.environ.get('GITHUB_REF_TYPE') != 'tag':
        return '0.0.1.dev'

    version = os.environ.get('GITHUB_REF_NAME')
    assert version is not None
    version = version.lstrip('v.')
    return version