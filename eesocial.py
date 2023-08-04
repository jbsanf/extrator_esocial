import os
import re
import zipfile
from xml.etree import ElementTree as ET
from pathlib import WindowsPath
from typing import List

from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient, UpdateOne
load_dotenv()

client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
bd = client.eesocial
bd.eventos.create_index('id', unique=True)
bd.eventos.create_index('retorno.recibo.nrRecibo', unique=True)
cll_arquivos = bd.arquivos
cll_eventos = bd.eventos

compile_tag = re.compile(r"{.+}(?P<tag>\w.+)")

def relacionar_exclusoes() -> None:
    logger.info("Relacionamento de eventos S-3000 iniciada.")
    query = {
        "tabela": "evtExclusao",
        "_proc": {"$exists": False},
    }
    eventos_exclusao = cll_eventos.find(query)
    total = cll_eventos.count_documents(query)
    atualizar = []
    for evt in eventos_exclusao:
        rec_exc = evt['envio']['infoExclusao']['nrRecEvt']
        if cll_eventos.find_one({"retorno.recibo.nrRecibo": rec_exc}):
            rec_evt = evt['retorno']['recibo']['nrRecibo']
            atualizar += [
                UpdateOne(
                    {"retorno.recibo.nrRecibo": rec_exc},
                    {"$set": {'exclusao': rec_evt}}
                ),
                UpdateOne(
                    {"retorno.recibo.nrRecibo": rec_evt},
                    {"$set": {'_proc': 1}}
                )
            ]
    if atualizar:
        cll_eventos.bulk_write(atualizar)
    logger.info(f"{len(atualizar)/2} de {total} exclusão(ões) relacionada(s).")

def relacionar_retificacoes() -> None:
    logger.info("Relacionamento de eventos retificadores iniciada.")
    query = {
        "envio.ideEvento.nrRecibo": {"$exists": True},
        "_proc": {"$exists": False},
    }
    eventos_retificacao = cll_eventos.find(query)
    total = cll_eventos.count_documents(query)
    atualizar = []
    for evt in eventos_retificacao:
        rec_ret = evt['envio']['ideEvento']['nrRecibo']
        if cll_eventos.find_one({"retorno.recibo.nrRecibo": rec_ret}):
            rec_evt = evt['retorno']['recibo']['nrRecibo']
            atualizar += [
                UpdateOne(
                    {"retorno.recibo.nrRecibo": rec_ret},
                    {"$set": {'retificado': rec_evt}}
                ),
                UpdateOne(
                    {"retorno.recibo.nrRecibo": rec_evt},
                    {"$set": {'_proc': 1}}
                )
            ]
    if atualizar:
        cll_eventos.bulk_write(atualizar)
    logger.info(f"{len(atualizar)/2} de {total} retificação(ões) relacionada(s).")

def xml_para_json(el: ET.Element) -> dict:
    if len(el):
        rtrn = {}
        for item in el:
            tag = compile_tag.match(item.tag).group('tag')
            if tag != 'Signature':
                rtrn.update({tag: xml_para_json(item)})
        return rtrn
    return el.text


class Arquivo(object):
    __arq_test = re.compile(r"(?P<id>ID1\d{33})\.S-(?P<evento>\d{4})\.xml")

    def __init__(self, loc: WindowsPath) -> None:
        self._loc: WindowsPath = loc.absolute()
        self._zipfile = zipfile.ZipFile(self._loc)
    
    def __repr__(self) -> str:
        return f"Arquivo(loc='{self._loc}')"

    def processado(self) -> bool:
        stat = self._loc.stat()
        return cll_arquivos.find_one({
            'loc': self._loc.stem,
            'st_size': stat.st_size
        })

    def processar(self) -> None:
        if self.processado():
            return
        logger.info(f"Processando arquivo {self._loc}.")
        novo_eventos = []
        for arq in self._zipfile.filelist:
            match = self.__arq_test.match(arq.filename)
            if match:
                id_evento = match.group('id')
                if cll_eventos.find_one({'id': id_evento}):
                    continue
                with self._zipfile.open(arq) as xml_arq:
                    root = ET.fromstring(xml_arq.read().decode())
                    dds = xml_para_json(el=root[0][0][0][0])
                    rcb = xml_para_json(el=root[0][1][0][0])
                    reg = {
                        'envio': dds,
                        'retorno': rcb,
                        'id': id_evento,
                        'tabela': compile_tag.match(
                            root[0][0][0][0].tag
                        ).group('tag'),
                    }
                    novo_eventos.append(reg)
        cll_eventos.insert_many(novo_eventos)
        stat = self._loc.stat()
        cll_arquivos.insert_one({
            'loc': self._loc.stem,
            'st_size': stat.st_size,
            'st_mtime': stat.st_mtime,
        })


class Diretorio(object):
    def __init__(self, loc=WindowsPath) -> None:
        self._loc: WindowsPath = loc
    
    def __listar(self, dir: WindowsPath) -> List[WindowsPath]:
        lista = []
        for item in dir.iterdir():
            if item.is_dir():
                lista += self.__listar(dir=item)
            elif item.suffix == '.zip':
                lista.append(Arquivo(loc=item))
        return lista
    
    def lista(self) -> List[WindowsPath]:
        return self.__listar(dir=self._loc)


if __name__ == '__main__':
    dir_trabalho = Diretorio(loc=WindowsPath(os.getenv("LOC_DIR", '.')))
    for arq_zip in dir_trabalho.lista():
        arq_zip.processar()
    
    relacionar_exclusoes()
    relacionar_retificacoes()