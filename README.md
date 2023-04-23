# Google Earth Engine Data Retriever (GEEDaR)

[![Documentation Status](https://readthedocs.org/projects/geedar-lib/badge/?version=latest)](https://geedar-lib.readthedocs.io/en/latest/?badge=latest) ![CI](https://github.com/lucasuchoa98/geedar-lib/actions/workflows/pipeline.yaml/badge.svg)
Este script destina-se a recuperar dados do Google Earth Engine.
Com base na lista fornecida de sites e datas e nos algoritmos de processamento escolhidos,
ele recupera os dados de satélite correspondentes.
Os produtos suportados incluem MODIS, VIIRS, Landsat, Sentinel-2 e Sentinel-3.

## Instalação
Para instalação do projeto recomendamos o uso do `pipx`

```bash
pipx install geedar-lib
```

Embora isso seja somente uma recomendação! Você também pode instalar o projeto com o gerenciador de sua preferência. Como o pip:
```bash
pip install geedar-lib
```