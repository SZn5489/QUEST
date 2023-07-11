import sys

filePath = sys.argv[1]

adv_file = open(filePath + "/advertiser.tbl","r", encoding='utf8')
cam_file = open(filePath + "/campaign.tbl","r", encoding='utf8')
wordset_file = open(filePath + "/wordset.tbl","r", encoding='utf8')
word_file = open(filePath + "/word.tbl","r", encoding='utf8')
click_file = open(filePath + "/clicks.tbl","r", encoding='utf8')
pc_file = open(filePath + "/person_click.tbl","r", encoding='utf8')


json_file = open(filePath + "/all.json", "w", encoding="utf8")

adv_line = adv_file.readline()
adv_line = adv_line[:-1]
cam_line = cam_file.readline()
cam_line = cam_line[:-1]
wordset_line = wordset_file.readline()
wordset_line = wordset_line[:-1]
word_line = word_file.readline()
word_line = word_line[:-1]
click_line = click_file.readline()
click_line = click_line[:-1]
pc_line = pc_file.readline()
pc_line = pc_line[:-1]


def campaignJson(advId):
    json = "["
    global cam_line
    global cam_file
    while cam_line:
        camList = cam_line.split("|")
        if camList[1] == advId:
            json = json + "{\"c_id\":" + camList[0] + ", \"c_aid\":" + camList[1] + ", \"c_budget\":" + camList[2] + ", \"WordSetList\": "
            json = json + wordsetJson(camList[0])
            json = json + ", \"ClickList\": "
            json = json + clickJson(camList[0])
            json = json + "},"
            cam_line = cam_file.readline()
            cam_line = cam_line[:-1]
        else:
            json = json[:-1]
            if len(json) == 0:
                json = "["
            break
    json = json + "]"
    return json

def wordsetJson(campaignId):
    global wordset_file
    global wordset_line
    json = "["
    while wordset_line:
        wordsetList = wordset_line.split("|")
        if wordsetList[1] == campaignId:
            json = json + "{\"w_id\":" + wordsetList[0] + ", \"w_cid\":" + wordsetList[1] + ", \"WordList\":" 
            json = json + wordJson(wordsetId=wordsetList[0])
            json = json + "},"
            wordset_line = wordset_file.readline()
            wordset_line = wordset_line[:-1]
        else:
            json = json[:-1]
            if len(json) == 0:
                json = "["
            break
    json = json + "]"
    return json          

def clickJson(camId):
    global click_file
    global click_line
    json = "["
    while click_line:
        
        clickList = click_line.split("|")
        if clickList[1] == camId:
            json = json + "{\"cl_id\": " + clickList[0] + ", \"cl_cid\": " + clickList[1] + ", \"cl_fee\": " + clickList[2] + ", \"cl_date\": " + "\""+ clickList[3] + "\", \"PersonClickList\": "
            json = json + personClickJson(clickList[0])
            json = json + "},"
            click_line = click_file.readline()
            click_line = click_line[:-1]
        else:
            json = json[:-1]
            if len(json) == 0:
                json = "["
            break
    json = json + "]"
    return json

def personClickJson(clickId):
    global pc_file
    global pc_line
    json = "["
    while pc_line:
        
        pcList = pc_line.split("|")
        if pcList[1] == clickId:
            json = json + "{\"p_id\": " + pcList[0] + ", \"p_clid\": " + pcList[1] + ", \"p_clickDate\": \"" + pcList[2] + "\"},"
            pc_line = pc_file.readline()
            pc_line = pc_line[:-1]
        else:
            json= json[:-1]
            if len(json) == 0:
                json = "["
            break
    json = json + "]"
    return json

def wordJson(wordsetId):
    global word_file
    global word_line
    json = "["
    while word_line:
        
        wordList = word_line.split("|")
        if wordList[0] == wordsetId:
            json = json + "{\"wo_wid\":" + wordList[0] + ", \"wo_word\":" + "\"" + wordList[1] + "\"},"
            word_line = word_file.readline()
            word_line = word_line[:-1]
        else:
            json = json[:-1]
            if len(json) == 0:
                json = "["
            break
    json = json + "]"    
    return json


while adv_line: 
    print(adv_line)   
    advList = adv_line.split("|")
    json = "{\"a_id\": " + advList[0] + ", \"a_email\":" + "\""+advList[1]+"\"" + ", \"a_name\":" + "\"" + advList[2] + "\"" + ", \"CampaignList\":"
    json = json + campaignJson(advList[0])
    json = json + "}"
    json_file.write(json)
    json_file.write("\n")
    adv_line = adv_file.readline()
    adv_line = adv_line[:-1]

adv_file.close()
cam_file.close()
word_file.close()
wordset_file.close()
click_file.close()
pc_file.close()
json_file.close()
        

