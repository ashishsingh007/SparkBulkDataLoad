import pytest

from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


def test_blank_test(spark):
    print(spark.version)

    print("Pytest framework is awesome")
    assert spark.version == "3.2.1"


