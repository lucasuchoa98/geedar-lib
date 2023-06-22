from geedar_lib.geedar import (
    getCollection, getSpectralBands)
 
def test_retornar_colecao_de_imagens():
    productID = 101

    result = getCollection(productID)

    assert result

def test_retornar_regioes_espectrais():
    productID = 101

    result = getSpectralBands(productID)

    assert result
