from uopclient.connect import direct
from uopmeta.schemas.predefined import pkm_schema
from sqluop import adaptor
from uopclient.uop_connect import register_adaptor

def test_connect():
    register_adaptor(adaptor.AlchemyDatabase, 'sqlite')
    connect = direct.DirectConnection.connect(db_type='sqlite', db_name='pkm_app',
                                              schemas=[pkm_schema])
    assert connect
