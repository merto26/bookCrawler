from testAnfrage import testAnfrageIndex, testAnfragePartition, testAnfrageDataModell
from testUpdate import testUpdateIndex, testUpdatePartition, testUpdateDataModell

from testVerschiedeneDatenmengen import testUpdatePartition1_5, testAnfragePartition1_5, testUpdateIndex1_5, testAnfrageIndex1_5, testUpdateDatamodell1_5, testAnfragenDatamodell1_5
if __name__ == '__main__':

    testAnfrageIndex.testeAnfrageIndex(20)
    testAnfragePartition.testeAnfragePartition(20)
    testAnfrageDataModell.testeAnfrageDatamodell(20)
    testUpdateIndex.testeUpdateIndex(20)
    testUpdatePartition.testeUpdatePartition(20)
    testUpdateDataModell.testeUpdateDatamodell(20)
    testAnfragePartition1_5.testeAnfragePartition(20)
    testUpdatePartition1_5.testeUpdatePartition(20)
    testAnfrageIndex1_5.testeAnfrageIndex(20)
    testUpdateIndex1_5.testeUpdateIndex(20)
    testAnfragenDatamodell1_5.testeAnfrageDatamodell(20)
    testUpdateDatamodell1_5.testeUpdateDatamodell(20)