from time import sleep
import requests


text = 'technical problems'
time = 0
while True:
    r = requests.get('http://dbiweb.sfgov.org/dbipts/default.aspx?page=Permit&PermitNumber=9409803')
    if text in r.text:
        time+=3600
        sleep(3600)
    else:
        print('Time elapsed before working again ' + str(time))
        break
