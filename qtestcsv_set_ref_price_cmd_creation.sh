echo "Transaction: secBoardSetRefPrice"
echo "user,onbehalfof,valuesList[1].secBoardId.securityIdType,valuesList[1].secBoardId.secCode,valuesList[1].secBoardId.boardId,valuesList[1].secBoardId.speedIndex,valuesList[1].refPrice,valuesList[1].time.date,valuesList[1].time.time,valuesList[1].messageText"

for i in `cat $1`; do echo "su3,,,`echo $i|cut -f1 -d':'`,SAEQ,,`echo $i|cut -f2 -d':'`,,,"; done

