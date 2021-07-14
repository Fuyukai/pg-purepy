import pytest

from tests.hilevel import open_pool

pytestmark = pytest.mark.anyio


async def test_get_type_oid():
    """
    Tests getting a type OID.
    """
    async with open_pool(conn_count=1) as pool:
        oid = await pool.find_oid_for_type("int4")
        assert oid == 23
