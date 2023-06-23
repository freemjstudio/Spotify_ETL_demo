import json
from datetime import date
from datetime import timedelta

today = date.today()
yesterday = today - timedelta(days = 1)
RULE_ID = ""

settings = {
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": f"{RULE_ID}",
      "rule-name": f"{RULE_ID}",
      "object-locator": {
        "schema-name": "%",
        "table-name": "%"
      },
      "rule-action": "include",
      "filters": [
        {
          "filter-type": "source",
          "column-name": "timestamp",
          "filter-conditions": [
            {
              "filter-operator": "between",
              "start-value": f"{yesterday}",
              "end-value": f"{today}"
            },
            {
              "filter-operator": "noteq",
              "value": f"{today}"
            }
          ]
        }
      ]
    }
  ]
}

jsonString = json.dumps(settings)



jsonFile = open("dms.json","w")
jsonFile.write(jsonString)
jsonFile.close()

# json file 생성해서 -> 이 내용 기반으로 DMS 작업 재실행
# 작업 실행 trigger / scheduling 은 step function 으로