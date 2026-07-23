"""Import smoke tests.

Every subpackage's ``api`` module must import cleanly (heavy third-party deps
are lazy-imported inside methods, so importing the module itself must not
require the optional extras) and expose its documented client class(es).
Catches syntax errors, bad top-level imports, and accidental renames.
"""

import importlib

import pytest

MODULES = [
    ("cit_pydata.util.api", []),
    ("cit_pydata.sql.api", ["SQLClient"]),
    ("cit_pydata.aws.api", ["S3Client", "SSMClient"]),
    ("cit_pydata.salesforce.api", ["SalesforceClient", "SalesforceSOAPClient"]),
    ("cit_pydata.marketo.api", ["MarketoClient"]),
    ("cit_pydata.box.api", ["BoxClient"]),
    ("cit_pydata.joblog.api", ["JobLogClient"]),
    ("cit_pydata.sftp.api", ["SFTPClient"]),
    ("cit_pydata.sfsync.api", ["SFSyncClient"]),
]


@pytest.mark.parametrize("module_name,attrs", MODULES)
def test_module_imports(module_name, attrs):
    module = importlib.import_module(module_name)
    for attr in attrs:
        assert hasattr(module, attr), f"{module_name} is missing {attr}"
