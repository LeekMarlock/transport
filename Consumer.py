from confluent_kafka import Consumer
import time
import json
import copy
import requests
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'group.instance.id':'chat_cons',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'latest'}

consumer = Consumer(conf)
local_ip = '127.0.0.1'
consumer.subscribe(['segments_test2'])
q = dict()
while 1:
    while 1:
        msg = consumer.poll(0)
        print(msg, msg is None)
        if msg == None:
            print('1')
            print(0, end = ' ')
            copy_q = copy.deepcopy(q)
            for i in q:
                result = {'sender_name':q[i][1]['sender_name'],'send_time':q[i][1]['send_time'],'message':''}
                
                if q[i][1]['quantity_of_segments'] == len(q[i])-1:  # not q[i][0] and
                    mess = [0]*q[i][1]['quantity_of_segments']
                    for u in q[i][1:]:
                        mess[int(u['number_of_this_segment'])] = u['segment']
                    if 0 in mess:
                        print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                        resp = requests.post(f'http://{local_ip}:8024/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}", "sender_name":"{q[i][1]["sender_name"]}"}}'))
                    else:
                        result['message']=''.join(mess)
                        print(result)
                        resp = requests.post(f'http://{local_ip}:8024/receive',json=result)
                    del copy_q[i]
                    continue
                else:
                    copy_q[i][0] += 1

                if copy_q[i][0] == 2:
                    print('ERROR ERROR ERROR ERROR ERROR ERROR ')
                    resp = requests.post(f'http://{local_ip}:8024/receive',json=json.loads(f'{{"error":true, "send_time":"{q[i][1]["send_time"]}", "sender_name":"{q[i][1]["sender_name"]}"}}'))

                    del copy_q[i]
            q = copy.deepcopy(copy_q)
            break
                    
                            
        else:
            print('2')
            print(msg.value().decode('utf-8'))
            segment = json.loads(msg.value()) #.decode('utf-8')
            if segment['send_time'] not in q:
                q[segment['send_time']] = [0]
            else:
                q[segment['send_time']][0] = 0
            q[segment['send_time']].append(segment)
            print(1, end = ' ')
    time.sleep(2)

        
                

        
