import logging
import sys

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as mqttsuber
import socket
import numpy as np
import Queue
import time
from datetime import datetime, timedelta
import pip

awakeTime = 5  # 60*5

usrName = "students"
passWord = "33106331"

clientID = "3310-u6325688"
client = mqtt.Client(client_id=clientID, transport="tcp")

brokerPort = 1883
brokerHost = "comp3310.ddns.net"
brokerIP = ""

slowPub = "counter/slow/q"  # q0,q1,q2
fastPub = "counter/fast/q"  # q0,q1,q2
# MQTT_TOPIC_SLOW = [("counter/slow/q0",0),("counter/slow/q1",1),("counter/slow/q2",2)] #default
MQTT_TOPIC_FAST = [("counter/fast/q0", 0), ("counter/fast/q1", 1), ("counter/fast/q2", 2)]
# MQTT_TOPIC_FAST = [("counter/fast/q1",1)]

msgQ0 = Queue.Queue(maxsize=0)
msgQ1 = Queue.Queue(maxsize=0)
msgQ2 = Queue.Queue(maxsize=0)

recvTSQ0 = Queue.Queue(maxsize=0)
recvTSQ1 = Queue.Queue(maxsize=0)
recvTSQ2 = Queue.Queue(maxsize=0)

subData = {
    0: {
        "ct": 0,
        "pl": msgQ0,
        "recvTS": recvTSQ0,
        "dup": 0,
        "prvMsg": -1,
        "ooo": 0,
        "minpl": sys.maxsize,
        "maxpl": -1
    },
    1: {
        "ct": 0,
        "pl": msgQ1,
        "recvTS": recvTSQ1,
        "dup": 0,
        "prvMsg": -1,
        "ooo": 0,
        "minpl": sys.maxsize,
        "maxpl": -1
    },
    2: {
        "ct": 0,
        "pl": msgQ2,
        "recvTS": recvTSQ2,
        "prvMsg": -1,
        "dup": 0,
        "ooo": 0,
        "minpl": sys.maxsize,
        "maxpl": -1
    }
}

SUB_RESULT = []

MQTT_CODES = {
    0: "Connection successful",
    1: "Connection refused - incorrect protocol version",
    2: "Connection refused - invalid client identifier",
    3: "Connection refused - server unavailable",
    4: "Connection refused - bad username or password",
    5: "Connection refused - not authorised"
}


def analyseSubData():
    for qos in range(0, 3):
        isize = subData[qos]["ct"]
        # PrevData
        prevpl = ""
        prevTS = -1

        # (i)RECV
        startTS = -1
        endTS = -1

        # (ii)LOSS
        expectedSet = set(range(subData[qos]["minpl"], subData[qos]["maxpl"] + 1))
        recvSet = set()

        # (iv)Out-Of-Order
        oooCounter = 0

        # (v)GAP
        gapList = np.zeros(isize)

        if qos == 1:
            print("Qos")

        for i in range(0, isize):
            currpl = subData[qos]["pl"].get()
            currTS = subData[qos]["recvTS"].get()

            if i == 0:
                startTS = currTS
            if i == isize - 1:
                endTS = currTS

            # ---------LOSS---------
            recvSet.add(currpl)

            if prevpl != "" and prevTS != -1:
                # ---------OOO---------
                if prevpl > currpl:
                    oooCounter += 1
                # ---------GAP---------
                if prevTS > -1:
                    gapList[i] = (currTS - prevTS) * 1000

            prevpl = currpl
            prevTS = currTS

        # Prepare Report answers:
        r_recvRate = round(isize / (endTS - startTS), 4)
        r_lossRate = round(len(expectedSet - recvSet) / len(expectedSet) * 100, 4)
        r_dupRate = round(subData[qos]["dup"] / isize * 100, 4)
        r_oooRate = round(oooCounter / isize * 100, 4)
        r_meanGap = round(gapList.mean(), 4)
        r_stdGap = round(gapList.std(), 4)
        # print("QoS_"+qos+"\trecvRate="+r_recvRate+"\tlossRate="+r_lossRate+"\tdupRate="
        #     +r_dupRate+"\toooRate="+r_oooRate+"\tmeanGap="+r_meanGap+"\tstdGap="+r_stdGap+"\n")
        print("total msg received:" + str(isize) + "\n\n")
        # SUB_RESULT
        SUB_RESULT.append({"qos": str(qos),
                           "recv": str(r_recvRate) + "(messages/second)",
                           "loss": str(r_lossRate) + "%", "dupe": str(r_dupRate) + "%", "ooo": str(r_oooRate) + "%",
                           "gap": str(r_meanGap) + "(milliseconds)", "gvar": str(r_stdGap) + "(milliseconds)"})
        print(SUB_RESULT[qos])


def publishResult():
    for i in range(0, len(SUB_RESULT)):
        p_topic = "studentreport/u6325688"
        p_qos = 2
        p_ctQoS = SUB_RESULT[i]["qos"]
        p_retain = True
        p_counter = ""
        if i < 3:
            p_counter = "slow"
        else:
            p_counter = "fast"

        client.publish(p_topic + '/language', payload="Python3.7\tMQTT Library: paho.mqtt.client", qos=p_qos,
                       retain=p_retain)
        client.publish(p_topic + '/network', payload="TCP", qos=p_qos, retain=p_retain)
        for topic in ["recv", "loss", "dupe", "ooo", "gap", "gvar"]:
            client.publish(p_topic + '/' + p_counter + '/' + p_ctQoS + '/' + topic, payload=SUB_RESULT[i][topic],
                           qos=p_qos, retain=p_retain)


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        logging.warning()  # ToDo: code ==> meaning
        return
    # ToDo: Test unreliable connection
    # For 2(b)
    # client.subscribe("$SYS/#")

    # For 2()


def on_disconnect(client, userdata, rc):
    print("Disconnected: " + str(rc))
    msgQ0 = Queue.Queue(maxsize=0)
    msgQ1 = Queue.Queue(maxsize=0)
    msgQ2 = Queue.Queue(maxsize=0)

    recvTSQ0 = Queue.Queue(maxsize=0)
    recvTSQ1 = Queue.Queue(maxsize=0)
    recvTSQ2 = Queue.Queue(maxsize=0)

    subData = {
        0: {"ct": 0, "pl": msgQ0, "recvTS": recvTSQ0, "dup": 0, "prvMsg": -1, "ooo": 0, "minpl": sys.maxsize,
            "maxpl": -1},
        1: {"ct": 0, "pl": msgQ1, "recvTS": recvTSQ1, "dup": 0, "prvMsg": -1, "ooo": 0, "minpl": sys.maxsize,
            "maxpl": -1},
        2: {"ct": 0, "pl": msgQ2, "recvTS": recvTSQ2, "prvMsg": -1, "dup": 0, "ooo": 0, "minpl": sys.maxsize,
            "maxpl": -1}
    }


def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    qos = int(msg.qos)
    ts = msg.timestamp
    dp = msg.dup
    logging.warning('topic:%s\tpl:%s\tqs%d\tts:%f', msg._topic.decode('utf-8'), payload, qos, ts)
    try:
        payload = int(payload)
        subData[qos]["ct"] += 1
        subData[qos]["pl"].put(payload)
        subData[qos]["recvTS"].put(ts)
        if dp != 0:
            subData[qos]["dup"] += 1
            # logging.warning('***!!!!***%d\tct:%d\tdup:%d', qos, subData[qos]["ct"], dp)
        if subData[qos]["prvMsg"] > payload:
            subData[qos]["ooo"] += 1
        if payload < subData[qos]["minpl"]:
            subData[qos]["minpl"] = payload
        if payload > subData[qos]["maxpl"]:
            subData[qos]["maxpl"] = payload
        subData[qos]["prevMsg"] = payload

    except ValueError:
        return False


def on_subscribe(client, userdata, mid, granted_qos):
    print('ON_SUBSCRIBE %d %s', mid, str(granted_qos))


def prepare(topic):
    MQTT_TOPIC = []
    if topic == 's':
        MQTT_TOPIC = MQTT_TOPIC_SLOW
    else:
        MQTT_TOPIC = MQTT_TOPIC_FAST

    brokerIP = socket.gethostbyname(brokerHost)  # translate hostname to IP
    client.username_pw_set(username=usrName, password=passWord)
    # Listeners

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    # client.loop_start()
    client.connect(host=brokerIP, port=brokerPort, keepalive=awakeTime)
    print("Connected to Broker: comp3310.ddns.net")
    # For 2(a)
    startTime = time.time()
    client.subscribe(MQTT_TOPIC)
    waitTime = awakeTime
    while True:
        client.loop()
        elapsedTime = time.time() - startTime
        if elapsedTime > waitTime:
            for t in MQTT_TOPIC:
                client.unsubscribe(t[0])
            client.disconnect()
            break


def subTopic(topic):
    client.subscribe(topic, 0)


def pubTopic(where, msg):
    client.publish(where, msg)


if __name__ == "__main__":
    # --------------------------------------
    #  RUN  3 SLOW COUNTER -- >5 mins
    # --------------------------------------
    prepare("f")
    analyseSubData()
    print("________________________________")  # ToDo: Prepare .....Comments

    # prepare("f")

    # pip install paho_mqtt

    # --------------------------------------
    #   RUN  3 FAST COUNTER -- >5 mins
    # --------------------------------------

    # --------------------------------------
    #   PREPARE REPORT
    # --------------------------------------

