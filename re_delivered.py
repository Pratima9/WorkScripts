import requests
import csv
import json
from time import time



class mydict(dict):
    def __str__(self):
        return json.dumps(self)


def get_token():
    url = "https://api.ekartlogistics.com/admin/token"
    headers = {}
    headers['content-type'] = 'application/json'
    headers['EKART_SECRET_CODE'] = 'G[8428B5MI$UqF@iX/4d0>kI.9d3>T'
    headers['Authorization'] = 'Basic ZWthcnQ6bT16anE0QVdGaDwzS3tBOA=='
    headers['EKART_USER_CODE'] = 'EKT'
    headers = mydict(headers)

    # print headers
    response = requests.get(url, headers=headers)
    print response.status_code
    # token = response.content
    with open('/home/pratima.u/token.txt', 'w') as tx_file:
        tx_file.write(response.content)
    with open('/home/pratima.u/token.txt', 'r') as file_read:
        data = json.load(file_read)
        return data['Authorization'].encode("utf-8")


# d = get_token()
# print d

def mark_rts():
    count = 0
    l = [0,2000, 5000,10000,15000 ,20000,25000,30000,35000,40000,45000,50000]

    with open('/home/pratima.u/fm.csv', 'r') as rts:
        for id in csv.reader(rts, delimiter=','):
            print "Tracking_id:", id[0]
            url = 'http://10.24.1.120:80/shipments/{content}/update_status'.format(content = id[0])
            print url

            headers = {}
            headers['content-type'] = 'application/json'
            headers['EKART_SECRET_CODE'] = 'G[8428B5MI$UqF@iX/4d0>kI.9d3>T'

            if count in l:
                print "count",count
                headers['Authorization'] = get_token()
                print "headers['Authorization']",headers['Authorization']
            else:
                with open('/home/pratima.u/token.txt',
                          'r') as file_read:
                    d = json.load(file_read)
                    headers['Authorization'] = d['Authorization'].encode("utf-8")



            count += 1

            headers['EKART_USER_CODE'] = 'EKT'
            headers = mydict(headers)
            print "headers",headers

            data ={}
            #data['delivery_entity'] = 'Seller'
            data['delivery_entity'] = 'Customer'
            data['responsible'] = 'EKL'
            add = '000'
            data['update_time'] = int(str(int(time())) + add)
            data['status'] = 'delivered'
            data['open_box_delivery'] = False
            data = mydict(data)
            print "data",data
            r = requests.post(url, data=json.dumps(data), headers=headers)
            print r.status_code




mark_rts()
